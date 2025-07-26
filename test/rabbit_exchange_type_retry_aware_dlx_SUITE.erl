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
      topic_routing_semantics_when_messages_missing_radlx_arguments,
      topic_routing_semantics_when_messages_with_radlx_arguments_but_should_not_die_yet,
      testing_topology_two_retries_for_one_cycle_reject_reason,
      testing_topology_two_retries_for_two_cycles_reject_reason,
      testing_topology_expired_reason
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

topic_routing_semantics_when_messages_missing_radlx_arguments(Config) ->
%%  Testing topic routing when radlx headers are not defined in the message
%%  This is not the intended use of this exchange, but I want to ensure
%%  that it behaves like a regular topic exchange in that case.
  ok = test_topic_routing_semantics(Config, single_queue_single_publish()),
  ok = test_topic_routing_semantics(Config, multiple_queues_no_match()),
  ok = test_topic_routing_semantics(Config, multiple_queues_all_match()),
  ok = test_topic_routing_semantics(Config, single_queue_multiple_publishes()),

  passed.

topic_routing_semantics_when_messages_with_radlx_arguments_but_should_not_die_yet(Config) ->
%%    Testing topic routing when radlx headers defined in the message
%%    but the message should not die yet, as the death condition is not met.
%%    so the expectations here are the same of the previous test.
  ok = test_topic_routing_semantics_given_radlx(Config, single_queue_single_publish()),
  ok = test_topic_routing_semantics_given_radlx(Config, multiple_queues_no_match()),
  ok = test_topic_routing_semantics_given_radlx(Config, multiple_queues_all_match()),
  ok = test_topic_routing_semantics_given_radlx(Config, single_queue_multiple_publishes()),
  passed.

testing_topology_two_retries_for_one_cycle_reject_reason(Config) ->
  %% Testing a topology where the message should be processed by the queue two times,
  %% and then moves to the final dead letter queue.
  {_, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
  DLXExchange = <<"dlx">>,
  QueueName = <<"queue">>,
  FinalDeadLetterQueue = <<"failed_queue">>,

  declare_exchange(Chan, DLXExchange, <<"radlx">>),
  declare_queue_with_dlx(Chan, QueueName, DLXExchange),
  declare_queue(Chan, FinalDeadLetterQueue),
  bind_queue_with_routing_key(Chan, QueueName, DLXExchange, QueueName),
  bind_queue_with_arguments(Chan, FinalDeadLetterQueue, DLXExchange, [
    {<<"x-match">>, longstr, <<"all">>},
    {<<"radlx.dead.source">>, longstr, QueueName}
  ]),

  Payload = <<"p">>,
  publish_message_with_headers(Chan, QueueName, Payload, [
    {<<"radlx-max-per-cycle">>, long, 2},
    {<<"radlx-track-queue">>, longstr, QueueName}
  ]),

  %%  First cycle: first reject, should go to Radlx exchange, that should decide not to die yet.
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag, requeue = false}),
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"0">>, <<"0">>, <<"0">>]]),

  %%  First cycle: second reject, should go to Radlx exchange, and given it died MaxPerCycle times,
  %%  it should be send to the final dead letter queue.
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag2] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag2, requeue = false}),

  %%  Final dead letter queue should have the message now.
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"1">>, <<"1">>, <<"0">>]]),
  %% This's important to ensure the message hasn't been routed to the original queue again
  %% this's the only difference against normal headers exchange smeantics
  %% where the message will be routed to all queue those have no args at all.
  %% pattern matching in is_header_binding_match handles that, so we only do routing
  %% to queues those have no routing key. (to avoid routing to the original queues)
  wait_for_messages(Config, [[QueueName, <<"0">>, <<"0">>, <<"0">>]]),

  cleanup_resources(Chan, [DLXExchange], [QueueName, FinalDeadLetterQueue]),
  passed.


testing_topology_two_retries_for_two_cycles_reject_reason(Config) ->
  %% Testing a topology where the message should be processed by the queue two times, and rejected twice,
  %% and then moves to the final dead letter queue that has TTL so messages will be routed back to the router exchange
  %% which will route it to the original queue and should be processed again for the same two times again and rejected,
  %% before moving again to the final dead letter queue.
  %% This test i'm validating the rem technique used to ensure same number of retries across cycles.
  {_, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
  RouterExchange = <<"router">>,
  DLXExchange = <<"dlx">>,
  QueueName = <<"queue">>,
  FinalDeadLetterQueue = <<"failed_queue">>,

  declare_exchange(Chan, RouterExchange, <<"direct">>),
  declare_exchange(Chan, DLXExchange, <<"radlx">>),
  declare_queue_with_dlx(Chan, QueueName, DLXExchange),
  declare_queue_with_dlx_and_ttl(Chan, FinalDeadLetterQueue, RouterExchange, 5000),
  bind_queue_with_routing_key(Chan, QueueName, RouterExchange, QueueName),
  bind_queue_with_routing_key(Chan, QueueName, DLXExchange, QueueName),
  bind_queue_with_arguments(Chan, FinalDeadLetterQueue, DLXExchange, [
    {<<"x-match">>, longstr, <<"all">>},
    {<<"radlx.dead.source">>, longstr, QueueName}
  ]),

  Payload = <<"p">>,
  publish_message_with_headers(Chan, QueueName, Payload, [
    {<<"radlx-max-per-cycle">>, long, 2},
    {<<"radlx-track-queue">>, longstr, QueueName},
    {<<"radlx-track-reason">>, longstr, <<"rejected">>}
  ]),

  %%  First cycle: first reject, should go to Radlx exchange, that should decide not to die yet.
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag, requeue = false}),
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"0">>, <<"0">>, <<"0">>]]),

  %%  First cycle: second reject, should go to Radlx exchange, and given it died MaxPerCycle times,
  %%  it should be send to the final dead letter queue.
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag2] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag2, requeue = false}),

  %%  Final dead letter queue should have the message now, which will expire after 5 seconds
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"1">>, <<"1">>, <<"0">>]]),
  %% After 5 seconds, the message should be routed back to the original queue
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  %% Let's consume again in the second cycle, and reject twice to ensure it end up in the final dead letter queue again.
  [DTag3] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag3, requeue = false}),
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"0">>, <<"0">>, <<"0">>]]),
  %%  Second cycle: second reject, should go to Radlx exchange, and given it died MaxPerCycle times,
  %%  it should be send to the final dead letter queue again.
  [DTag4] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag4, requeue = false}),
  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"1">>, <<"1">>, <<"0">>]]),

  cleanup_resources(Chan, [DLXExchange], [QueueName, FinalDeadLetterQueue]),
  passed.

testing_topology_expired_reason(Config) ->
  %% Testing a topology where the message should be processed by the queue two times, once rejected
  %% should go to delayed queue that will have a TTL of 5 seconds, then when expires go to the router exchange
  %% that will route it to the original queue, once rejected again it should go to the final dead letter queue
  {_, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
  RouterExchange = <<"router">>,
  DLXExchange = <<"dlx">>,
  QueueName = <<"queue">>,
  DelayedQueueName = <<"delayed_queue">>,
  FinalDeadLetterQueue = <<"failed_queue">>,

  declare_exchange(Chan, RouterExchange, <<"direct">>),
  declare_exchange(Chan, DLXExchange, <<"radlx">>),

  declare_queue_with_dlx(Chan, QueueName, DLXExchange),
  declare_queue(Chan, FinalDeadLetterQueue),
  declare_queue_with_dlx_and_ttl(Chan, DelayedQueueName, RouterExchange, 5000),

  bind_queue_with_routing_key(Chan, QueueName, RouterExchange, QueueName),
  bind_queue_with_routing_key(Chan, DelayedQueueName, DLXExchange, QueueName),
  bind_queue_with_arguments(Chan, FinalDeadLetterQueue, DLXExchange, [
    {<<"x-match">>, longstr, <<"all">>},
    {<<"radlx.dead.source">>, longstr, DelayedQueueName}
  ]),

  Payload = <<"p">>,
  publish_message_with_headers(Chan, QueueName, Payload, [
    {<<"radlx-max-per-cycle">>, long, 2},
    {<<"radlx-track-queue">>, longstr, DelayedQueueName},
    {<<"radlx-track-reason">>, longstr, <<"expired">>}
  ]),


  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),

  [DTag1] = consume(Chan, QueueName, [Payload]),
  %% after this rejection, message will go to radlx which will decide not to die given didn't expire twice yet, will be routed to the delayed queue
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag1, requeue = false}),

  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"0">>, <<"0">>, <<"0">>]]),
  wait_for_messages(Config, [[DelayedQueueName, <<"1">>, <<"1">>, <<"0">>]]),
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag2] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag2, requeue = false}),

  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"0">>, <<"0">>, <<"0">>]]),
  wait_for_messages(Config, [[DelayedQueueName, <<"1">>, <<"1">>, <<"0">>]]),
  wait_for_messages(Config, [[QueueName, <<"1">>, <<"1">>, <<"0">>]]),
  [DTag3] = consume(Chan, QueueName, [Payload]),
  amqp_channel:cast(Chan, #'basic.reject'{delivery_tag = DTag3, requeue = false}),

  wait_for_messages(Config, [[FinalDeadLetterQueue, <<"1">>, <<"1">>, <<"0">>]]),
  wait_for_messages(Config, [[QueueName, <<"0">>, <<"0">>, <<"0">>]]),
  wait_for_messages(Config, [[DelayedQueueName, <<"0">>, <<"0">>, <<"0">>]]),

  cleanup_resources(Chan, [DLXExchange], [QueueName, FinalDeadLetterQueue]),
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

test_topic_routing_semantics(Config, TestSpec) ->
  test_routing_with_headers(Config, TestSpec, []).

test_topic_routing_semantics_given_radlx(Config, TestSpec) ->
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
  cleanup_resources(Chan, [ExchangeName], Queues),
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

cleanup_resources(Chan, ExchangeNames, Queues) ->
  [amqp_channel:call(Chan, #'exchange.delete'{exchange = ExchangeName}) || ExchangeName <- ExchangeNames],
  [amqp_channel:call(Chan, #'queue.delete'{queue = Q}) || Q <- Queues],
  rabbit_ct_client_helpers:close_channel(Chan).


declare_exchange(Chan, ExchangeName, ExchangeType) ->
  #'exchange.declare_ok'{} = amqp_channel:call(Chan, #'exchange.declare'{
    exchange = ExchangeName,
    type = ExchangeType
  }).

declare_queue(Chan, QueueName) ->
  #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{
    queue = QueueName
  }).

declare_queue_with_dlx(Chan, QueueName, DLXExchange) ->
  #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{
    queue = QueueName,
    arguments = [{<<"x-dead-letter-exchange">>, longstr, DLXExchange}]
  }).

declare_queue_with_dlx_and_ttl(Chan, QueueName, DLXExchange, TTL) ->
  #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{
    queue = QueueName,
    arguments = [
      {<<"x-message-ttl">>, long, TTL},
      {<<"x-dead-letter-exchange">>, longstr, DLXExchange}
    ]
  }).

bind_queue_with_routing_key(Chan, QueueName, ExchangeName, RoutingKey) ->
  #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{
    queue = QueueName,
    exchange = ExchangeName,
    routing_key = RoutingKey
  }).

bind_queue_with_arguments(Chan, QueueName, ExchangeName, Arguments) ->
  #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{
    queue = QueueName,
    exchange = ExchangeName,
    arguments = Arguments
  }).

publish_message_with_headers(Chan, RoutingKey, Payload, Headers) ->
  amqp_channel:call(Chan, #'basic.publish'{routing_key = RoutingKey},
    #amqp_msg{props = #'P_basic'{headers = Headers}, payload = Payload}).


consume(Chan, QName, Payloads) ->
  [begin
     {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = Payload}} =
       amqp_channel:call(Chan, #'basic.get'{queue = QName}),
     DTag
   end || Payload <- Payloads].