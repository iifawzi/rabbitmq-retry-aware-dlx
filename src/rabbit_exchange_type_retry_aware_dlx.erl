-module(rabbit_exchange_type_retry_aware_dlx).
-author("iifawzie@gmail.com").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/mc.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2, route/3]).
-export([validate/1, validate_binding/2,
  create/2, delete/2, policy_changed/2, add_binding/3,
  remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
  [{description, "exchange type ra-dlx: registry"},
    {mfa, {rabbit_registry, register,
      [exchange, <<"radlx">>, ?MODULE]}},
    {requires, database}]}).

%%----------------------------------------------------------------------------
-define(MaxPerCycleArgument, <<"radlx-max-per-cycle">>).
-define(QueueToTrackArgument, <<"radlx-track-queue">>).
-define(ReasonToTrackArgument, <<"radlx-track-reason">>).
-define(DeathArgumentKey, <<"radlx.dead.from">>).

%%----------------------------------------------------------------------------
info(_X) -> [].
info(_X, _) -> [].

description() ->
  [{description, <<"RabbitMQ exchange that enables atomic, per-message death decisions with retries">>}].

serialise_events() -> false.

route(#exchange{name = Name}, Msg) ->
  route(#exchange{name = Name}, Msg, #{}).

route(#exchange{name = Name}, Msg, _Opts) ->
  Headers = mc:routing_headers(Msg, [x_headers]),
  MaxPerRound = maps:get(?MaxPerCycleArgument, Headers, undefined),
  QueueToTrack = maps:get(?QueueToTrackArgument, Headers, undefined),
  ReasonToTrack =
    case maps:get(?ReasonToTrackArgument, Headers, <<"rejected">>) of
      <<"expired">> -> expired;
      <<"rejected">> -> rejected;
      <<"maxlen">> -> maxlen;
      <<"delivery_limit">> -> delivery_limit;
      _ -> rejected
    end,

  Deaths = mc:get_annotation(deaths, Msg),
  DeathHeader = getDeadLetterHeader(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths),
  getMatchings(DeathHeader, Name, Msg, Headers).


getMatchings(DeathHeader, Name, Msg, Headers) when map_size(DeathHeader) =/= 0 ->
  MergedHeaders = maps:merge(Headers, DeathHeader),
  HeaderMatches = rabbit_router:match_bindings(
    Name, fun(#binding{args = Args, key = RoutingKey}) ->
      case RoutingKey of
        <<"">> ->  %% Empty routing key means it's a header-only binding
          XMatchValue = rabbit_misc:table_lookup(Args, <<"x-match">>),
          Result = case XMatchValue of
                     {longstr, <<"any">>} ->
                       match_any(Args, MergedHeaders, fun match/2);
                     {longstr, <<"any-with-x">>} ->
                       match_any(Args, MergedHeaders, fun match_x/2);
                     {longstr, <<"all-with-x">>} ->
                       match_all(Args, MergedHeaders, fun match_x/2);
                     _ ->
                       match_all(Args, MergedHeaders, fun match/2)
                   end,
          Result;
        _ ->
          false
      end
          end),
  case HeaderMatches of
    [] -> getMatchings(#{}, Name, Msg, []);
    _ -> HeaderMatches
  end;
getMatchings(_, Name, Msg, _) ->
  MsgRoutes = mc:routing_keys(Msg),
  rabbit_db_binding:match_routing_key(Name, MsgRoutes, false).

match_x({<<"x-match">>, _, _}, _M) ->
  skip;
match_x({K, void, _}, M) ->
  maps:is_key(K, M);
match_x({K, _, V}, M) ->
  maps:get(K, M, undefined) =:= V.

match({<<"x-", _/binary>>, _, _}, _M) ->
  skip;
match({K, void, _}, M) ->
  maps:is_key(K, M);
match({K, _, V}, M) ->
  maps:get(K, M, undefined) =:= V.


match_all([], _, _MatchFun) ->
  true;
match_all([Arg | Rem], M, Fun) ->
  case Fun(Arg, M) of
    false ->
      false;
    _ ->
      match_all(Rem, M, Fun)
  end.

match_any([], _, _Fun) ->
  false;
match_any([Arg | Rem], M, Fun) ->
  case Fun(Arg, M) of
    true ->
      true;
    _ ->
      match_any(Rem, M, Fun)
  end.

validate_binding(_X, #binding{args = Args}) ->
  case rabbit_misc:table_lookup(Args, <<"x-match">>) of
    {longstr, <<"all">>} -> ok;
    {longstr, <<"any">>} -> ok;
    {longstr, <<"all-with-x">>} -> ok;
    {longstr, <<"any-with-x">>} -> ok;
    {longstr, Other} ->
      {error, {binding_invalid,
        "Invalid x-match field value ~tp; "
        "expected all, any, all-with-x, or any-with-x", [Other]}};
    {Type, Other} ->
      {error, {binding_invalid,
        "Invalid x-match field type ~tp (value ~tp); "
        "expected longstr", [Type, Other]}};
    undefined -> ok %% [0]
  end.

validate(_X) -> ok.
create(_Serial, _X) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).


getDeadLetterHeader(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths) when is_integer(MaxPerRound),
  MaxPerRound =/= 0,
  QueueToTrack =/= undefined ->
  case has_death_record(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths) of
    true ->
      #{?DeathArgumentKey => QueueToTrack};
    false ->
      #{}
  end;
getDeadLetterHeader(_, _, _, _) ->
  #{}.

has_death_record(MaxPerRound, RejectQueueName, ReasonToTrack, Deaths) when is_list(Deaths) ->
  Key = {RejectQueueName, ReasonToTrack},
  case lists:keyfind(Key, 1, Deaths) of
    {_, {death, _, _, Count, _}} when Count rem MaxPerRound =:= 0 ->
      true;
    _ ->
      false
  end;
has_death_record(_, _, _, _) ->
  false.