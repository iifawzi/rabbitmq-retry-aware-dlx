-module(rabbit_exchange_type_retry_aware_dlx_SUITE).
-author("iifawzie@gmail.com").

-compile(export_all).
-import(queue_utils, [wait_for_messages/2]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").


all() ->
  [
    {group, non_parallel_tests}
  ].

groups() ->
  [
    {non_parallel_tests, [], [
      direct_routing_semantics_when_messages_missing_radlx_arguments
    ]}
  ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
  rabbit_ct_helpers:log_environment(),
  Config1 = rabbit_ct_helpers:set_config(Config, [
    {rmq_nodename_suffix, ?MODULE}
  ]),
  rabbit_ct_helpers:run_setup_steps(Config1,
    rabbit_ct_broker_helpers:setup_steps() ++
    rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
  rabbit_ct_helpers:run_teardown_steps(Config,
    rabbit_ct_client_helpers:teardown_steps() ++
    rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
  Config.

end_per_group(_, Config) ->
  Config.

init_per_testcase(Testcase, Config) ->
  rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
  rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

direct_routing_semantics_when_messages_missing_radlx_arguments(Config) ->
%%  Testing direct routing when radlx headers are not defined in the message
%%  This is not the intended use of this exchange, but I want to ensure
%%  that it behaves like a regular direct exchange in that case.
  ok = routing_test0(Config, t1()),
  ok = routing_test0(Config, t2()),
  ok = routing_test0(Config, t3()),
  ok = routing_test0(Config, t4()),

  passed.

t1() ->
  {[<<"a0.b0.c0.d0">>, <<"a1.b1.c1.d1">>],
    [<<"a0.b0.c0.d0">>],
    1}.

t2() ->
  {[<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>, <<"a0.b0.c0.d2">>],
    [<<"a0.b0.c0.d3">>],
    0}.

t3() ->
  {[<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>],
    [<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>],
    2}.

t4() ->
  {[<<"a0.b0.c0.d0">>],
    [<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>],
    1}.

routing_test0(Config, {Queues, Publishes, Count}) ->
  Msg = #amqp_msg{props = #'P_basic'{}, payload = <<>>},
  Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
  #'exchange.declare_ok'{} =
    amqp_channel:call(Chan,
      #'exchange.declare'{
        type = <<"radlx">>,
        exchange = <<"testing">>,
        auto_delete = true
      }),
  [#'queue.declare_ok'{} =
    amqp_channel:call(Chan, #'queue.declare'{
      queue = Q, exclusive = true}) || Q <- Queues],
  [#'queue.bind_ok'{} =
    amqp_channel:call(Chan, #'queue.bind'{queue = Q,
      exchange = <<"testing">>,
      routing_key = Q})
    || Q <- Queues],

  #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
  [amqp_channel:call(Chan, #'basic.publish'{
    exchange = <<"testing">>, routing_key = RK},
    Msg) || RK <- Publishes],
  amqp_channel:call(Chan, #'tx.commit'{}),

  Counts =
    [begin
       #'queue.declare_ok'{message_count = M} =
         amqp_channel:call(Chan, #'queue.declare'{queue = Q,
           exclusive = true}),
       M
     end || Q <- Queues],
  Count = lists:sum(Counts),
  amqp_channel:call(Chan, #'exchange.delete'{exchange = <<"testing">>}),
  [amqp_channel:call(Chan, #'queue.delete'{queue = Q}) || Q <- Queues],

  rabbit_ct_client_helpers:close_channel(Chan),

  ok.