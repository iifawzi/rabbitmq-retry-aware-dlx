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
-define(DeathArgumentKey, <<"radlx.dead.source">>).
%%----------------------------------------------------------------------------
info(_X) -> [].
info(_X, _) -> [].

description() ->
  [{description, <<"RabbitMQ exchange that enables atomic, per-message death decisions with retries">>}].

serialise_events() -> false.

route(#exchange{name = Name}, Msg) ->
  route(#exchange{name = Name}, Msg, #{}).

route(#exchange{name = Name}, Msg, Opts) ->
  Headers = mc:routing_headers(Msg, [x_headers]),
  {MaxPerRound, QueueToTrack, ReasonToTrack} = extract_radlx_headers(Headers),
  Deaths = mc:get_annotation(deaths, Msg),
  RadlxDeathDecision = build_death_decision_header(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths),
  route_with_death_header(RadlxDeathDecision, Name, Msg, Headers, Opts).


extract_radlx_headers(Headers) ->
  MaxPerRound = maps:get(?MaxPerCycleArgument, Headers, undefined),
  QueueToTrack = maps:get(?QueueToTrackArgument, Headers, undefined),
  ReasonToTrack = parse_reason_to_track(Headers),
  {MaxPerRound, QueueToTrack, ReasonToTrack}.

parse_reason_to_track(Headers) ->
  case maps:get(?ReasonToTrackArgument, Headers, <<"rejected">>) of
    <<"expired">> -> expired;
    <<"rejected">> -> rejected;
    <<"maxlen">> -> maxlen;
    <<"delivery_limit">> -> delivery_limit;
    _ -> rejected
  end.

route_with_death_header(DeathDecision, Name, Msg, Headers, Opts) when map_size(DeathDecision) =/= 0 ->
  MergedHeaders = maps:merge(Headers, DeathDecision),
  HeaderMatches = match_header_bindings(Name, MergedHeaders),
  case HeaderMatches of
    [] -> topic_semantics(Name, Msg, Opts);
    _ -> HeaderMatches
  end;
route_with_death_header(_, Name, Msg, _, Opts) ->
  topic_semantics(Name, Msg, Opts).

match_header_bindings(Name, MergedHeaders) ->
  rabbit_router:match_bindings(
    Name, fun(#binding{args = Args, key = RoutingKey}) ->
      is_header_binding_match(RoutingKey, Args, MergedHeaders)
    end).

is_header_binding_match(<<>>, Args, MergedHeaders) ->
  XMatchValue = rabbit_misc:table_lookup(Args, <<"x-match">>),
  apply_match_strategy(XMatchValue, Args, MergedHeaders);
is_header_binding_match(_, _, _) ->
  false.

apply_match_strategy({longstr, <<"any">>}, Args, Headers) ->
  match_any(Args, Headers, fun match/2);
apply_match_strategy({longstr, <<"any-with-x">>}, Args, Headers) ->
  match_any(Args, Headers, fun match_x/2);
apply_match_strategy({longstr, <<"all-with-x">>}, Args, Headers) ->
  match_all(Args, Headers, fun match_x/2);
apply_match_strategy(_, Args, Headers) ->
  match_all(Args, Headers, fun match/2).

topic_semantics(Name, Msg, Opts) ->
  RKeys = mc:routing_keys(Msg),
  lists:append([rabbit_db_topic_exchange:match(Name, RKey, Opts) || RKey <- RKeys]).

build_death_decision_header(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths) ->
  case should_add_death_header(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths) of
    true -> #{?DeathArgumentKey => QueueToTrack};
    false -> #{}
  end.

should_add_death_header(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths) 
  when is_integer(MaxPerRound), MaxPerRound > 0, QueueToTrack =/= undefined ->
  check_death_count_reached_threshold(MaxPerRound, QueueToTrack, ReasonToTrack, Deaths);
should_add_death_header(_, _, _, _) ->
  false.

check_death_count_reached_threshold(MaxPerRound, QueueName, Reason, Deaths) when is_list(Deaths) ->
  DeathKey = {QueueName, Reason},
  case find_death_record(DeathKey, Deaths) of
    {death, _, _, Count, _} -> is_threshold_reached(Count, MaxPerRound);
    not_found -> false
  end;
check_death_count_reached_threshold(_, _, _, _) ->
  false.

find_death_record(Key, Deaths) ->
  case lists:keyfind(Key, 1, Deaths) of
    {_, DeathRecord} -> DeathRecord;
    false -> not_found
  end.

is_threshold_reached(Count, MaxPerRound) ->
  Count rem MaxPerRound =:= 0.

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
    undefined -> ok
  end.

delete(_Serial, #exchange{name = X}) ->
  rabbit_db_topic_exchange:delete_all_for_exchange(X).

add_binding(_Serial, _Exchange, Binding = #binding{key = RoutingKey}) ->
  case RoutingKey of
    <<>> -> ok;
    _    -> rabbit_db_topic_exchange:set(Binding)
  end.

remove_bindings(_Serial, _X, Binding = #binding{key = RoutingKey}) ->
  case RoutingKey of
    <<>> -> ok;
    _    -> rabbit_db_topic_exchange:delete(Binding)
  end.
assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).

validate(_X) -> ok.
create(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.