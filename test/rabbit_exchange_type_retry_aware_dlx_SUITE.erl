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
  ok = test_direct_routing_semantics(Config, single_queue_single_publish()),
  ok = test_direct_routing_semantics(Config, multiple_queues_no_match()),
  ok = test_direct_routing_semantics(Config, multiple_queues_all_match()),
  ok = test_direct_routing_semantics(Config, single_queue_multiple_publishes()),

  passed.

single_queue_single_publish() ->
  {[<<"a0.b0.c0.d0">>, <<"a1.b1.c1.d1">>],
    [<<"a0.b0.c0.d0">>],
    1}.

multiple_queues_no_match() ->
  {[<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>, <<"a0.b0.c0.d2">>],
    [<<"a0.b0.c0.d3">>],
    0}.

multiple_queues_all_match() ->
  {[<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>],
    [<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>],
    2}.

single_queue_multiple_publishes() ->
  {[<<"a0.b0.c0.d0">>],
    [<<"a0.b0.c0.d0">>, <<"a0.b0.c0.d1">>],
    1}.

test_direct_routing_semantics(Config, {Queues, PublishKeys, ExpectedCount}) ->
  Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
  ExchangeName = <<"testing">>,
  
  setup_exchange(Chan, ExchangeName),
  setup_queues_and_bindings(Chan, ExchangeName, Queues),
  publish_messages(Chan, ExchangeName, PublishKeys),
  
  ActualCount = count_total_messages(Chan, Queues),
  ExpectedCount = ActualCount,
  
  cleanup_resources(Chan, ExchangeName, Queues),
  rabbit_ct_client_helpers:close_channel(Chan),
  ok.

setup_exchange(Chan, ExchangeName) ->
  #'exchange.declare_ok'{} =
    amqp_channel:call(Chan,
      #'exchange.declare'{
        type = <<"radlx">>,
        exchange = ExchangeName,
        auto_delete = true
      }).

setup_queues_and_bindings(Chan, ExchangeName, Queues) ->
  [declare_and_bind_queue(Chan, ExchangeName, Q) || Q <- Queues].

declare_and_bind_queue(Chan, ExchangeName, QueueName) ->
  #'queue.declare_ok'{} =
    amqp_channel:call(Chan, #'queue.declare'{
      queue = QueueName, exclusive = true}),
  #'queue.bind_ok'{} =
    amqp_channel:call(Chan, #'queue.bind'{queue = QueueName,
      exchange = ExchangeName,
      routing_key = QueueName}).

publish_messages(Chan, ExchangeName, PublishKeys) ->
  Msg = #amqp_msg{props = #'P_basic'{}, payload = <<>>},
  #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
  [publish_single_message(Chan, ExchangeName, RK, Msg) || RK <- PublishKeys],
  amqp_channel:call(Chan, #'tx.commit'{}).

publish_single_message(Chan, ExchangeName, RoutingKey, Msg) ->
  amqp_channel:call(Chan, #'basic.publish'{
    exchange = ExchangeName, routing_key = RoutingKey},
    Msg).

count_total_messages(Chan, Queues) ->
  Counts = [get_queue_message_count(Chan, Q) || Q <- Queues],
  lists:sum(Counts).

get_queue_message_count(Chan, QueueName) ->
  #'queue.declare_ok'{message_count = Count} =
    amqp_channel:call(Chan, #'queue.declare'{queue = QueueName,
      exclusive = true}),
  Count.

cleanup_resources(Chan, ExchangeName, Queues) ->
  amqp_channel:call(Chan, #'exchange.delete'{exchange = ExchangeName}),
  [amqp_channel:call(Chan, #'queue.delete'{queue = Q}) || Q <- Queues].