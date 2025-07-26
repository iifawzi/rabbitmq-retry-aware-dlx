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
      direct_routing_semantics_when_messages_missing_radlx_arguments,
      direct_routing_semantics_when_messages_with_radlx_arguments_but_should_not_die_yet,
      testing_topology_two_retries_for_one_cycle
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

direct_routing_semantics_when_messages_with_radlx_arguments_but_should_not_die_yet(Config) ->
%%    Testing direct routing when radlx headers defined in the message
%%    but the message should not die yet, as the death condition is not met.
%%    so the expectations here are the same of the previous test.
  ok = test_direct_routing_semantics_given_radlx(Config, single_queue_single_publish()),
  ok = test_direct_routing_semantics_given_radlx(Config, multiple_queues_no_match()),
  ok = test_direct_routing_semantics_given_radlx(Config, multiple_queues_all_match()),
  ok = test_direct_routing_semantics_given_radlx(Config, single_queue_multiple_publishes()),
  passed.

testing_topology_two_retries_for_one_cycle(Config) ->
  %% Testing a topology with a Radlx exchange, a queue with a dead letter exchange,
  %% The message should be processed by the queue two times, and then moves to the final dead letter queue.
  {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
  DLXExchange = <<"dlx">>,
  QueueName = <<"queue">>,
  FinalDeadLetterQueue = <<"failed_queue">>,

  declare_radlx_exchange(Ch, DLXExchange),
  declare_queue_with_dlx(Ch, QueueName, DLXExchange),
  declare_queue(Ch, FinalDeadLetterQueue),
  bind_queue_with_routing_key(Ch, QueueName, DLXExchange, QueueName),
  bind_queue_with_arguments(Ch, FinalDeadLetterQueue, DLXExchange, [
    {<<"x-match">>, longstr, <<"all">>},
    {<<"radlx.dead.source">>, longstr, QueueName}
  ]),

  Payload = <<"p">>,
  publish_message_with_headers(Ch, QueueName, Payload, [
    {<<"radlx-max-per-cycle">>, long, 2},
    {<<"radlx-track-queue">>, longstr, QueueName}
  ]),

  %%  First cycle: first reject, should go to Radlx exchange, and it decided should not die yet.
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag] = consume(Ch, QueueName, [Payload]),
  amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = DTag, requeue = false}),

  %%  First cycle: second reject, should go to Radlx exchange, and given it died MaxPerCycle times,
  %%  it should be send to the final dead letter queue.
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag2] = consume(Ch, QueueName, [Payload]),
  amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = DTag2, requeue = false}),

  %%  Final dead letter queue should have the message now.
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"1">>, <<"1">>, <<"0">>]]),
  passed.


%% -------------------------------------------------------------------
%% Specs.
%% -------------------------------------------------------------------
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

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

test_direct_routing_semantics(Config, TestSpec) ->
  test_routing_with_headers(Config, TestSpec, []).

test_direct_routing_semantics_given_radlx(Config, TestSpec) ->
  test_routing_with_headers(Config, TestSpec, [
    {<<"radlx-max-per-cycle">>, long, 1000},
    {<<"radlx-track-queue">>, longstr, <<"queue">>}
  ]).

test_routing_with_headers(Config, {Queues, PublishKeys, ExpectedCount}, Headers) ->
  Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
  ExchangeName = <<"testing">>,

  declare_exchange(Chan, ExchangeName),
  declare_queues_and_bindings(Chan, ExchangeName, Queues),
  publish_messages(Chan, ExchangeName, PublishKeys, Headers),
  verify_message_count(Chan, Queues, ExpectedCount),
  cleanup_resources(Chan, ExchangeName, Queues),
  ok.

declare_exchange(Chan, ExchangeName) ->
  #'exchange.declare_ok'{} =
    amqp_channel:call(Chan,
      #'exchange.declare'{
        type = <<"radlx">>,
        exchange = ExchangeName,
        auto_delete = true
      }).

declare_queues_and_bindings(Chan, ExchangeName, Queues) ->
  [declare_and_bind_queue(Chan, ExchangeName, Q) || Q <- Queues].

declare_and_bind_queue(Chan, ExchangeName, QueueName) ->
  #'queue.declare_ok'{} =
    amqp_channel:call(Chan, #'queue.declare'{
      queue = QueueName, exclusive = true}),
  #'queue.bind_ok'{} =
    amqp_channel:call(Chan, #'queue.bind'{queue = QueueName,
      exchange = ExchangeName,
      routing_key = QueueName}).

publish_messages(Chan, ExchangeName, PublishKeys, Headers) ->
  Msg = #amqp_msg{props = #'P_basic'{headers = Headers}, payload = <<>>},
  #'tx.select_ok'{} = amqp_channel:call(Chan, #'tx.select'{}),
  [publish_single_message(Chan, ExchangeName, RK, Msg) || RK <- PublishKeys],
  amqp_channel:call(Chan, #'tx.commit'{}).

publish_single_message(Chan, ExchangeName, RoutingKey, Msg) ->
  amqp_channel:call(Chan, #'basic.publish'{
    exchange = ExchangeName, routing_key = RoutingKey},
    Msg).

verify_message_count(Chan, Queues, ExpectedCount) ->
  ActualCount = count_total_messages(Chan, Queues),
  ExpectedCount = ActualCount.

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
  [amqp_channel:call(Chan, #'queue.delete'{queue = Q}) || Q <- Queues],
  rabbit_ct_client_helpers:close_channel(Chan).


declare_radlx_exchange(Ch, ExchangeName) ->
  #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{
    exchange = ExchangeName,
    type = <<"radlx">>
  }).

declare_queue_with_dlx(Ch, QueueName, DLXExchange) ->
  #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{
    queue = QueueName,
    arguments = [{<<"x-dead-letter-exchange">>, longstr, DLXExchange}],
    durable = true
  }).

declare_queue(Ch, QueueName) ->
  #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{
    queue = QueueName
  }).

bind_queue_with_routing_key(Ch, QueueName, ExchangeName, RoutingKey) ->
  #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{
    queue = QueueName,
    exchange = ExchangeName,
    routing_key = RoutingKey
  }).

bind_queue_with_arguments(Ch, QueueName, ExchangeName, Arguments) ->
  #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{
    queue = QueueName,
    exchange = ExchangeName,
    arguments = Arguments
  }).

publish_message_with_headers(Ch, RoutingKey, Payload, Headers) ->
  amqp_channel:call(Ch, #'basic.publish'{routing_key = RoutingKey},
    #amqp_msg{props = #'P_basic'{headers = Headers}, payload = Payload}).


consume(Ch, QName, Payloads) ->
  [begin
     {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = Payload}} =
       amqp_channel:call(Ch, #'basic.get'{queue = QName}),
     DTag
   end || Payload <- Payloads].