# RabbitMQ Retry-Aware DLX Plugin

A RabbitMQ exchange plugin that provides atomic, per-message dead-lettering with configurable retry limits.

## TL;DR

The `radlx` exchange type combines topic and headers exchange behaviors:
- **When death count = limit**: Adds `radlx.dead.source` header and routes using headers exchange semantics
- **When death count < limit**: Routes using topic exchange semantics (can go to any queue based on routing keys)
- **Important**: You MUST create bindings with `radlx.dead.source` header to catch dead messages

## Overview

The `rabbitmq_retry_aware_dlx` plugin introduces a new exchange type (`radlx`) that intelligently manages message retries before dead-lettering. It tracks death counts per queue/reason combination and makes atomic decisions about when to finally dead-letter a message, even across multiple retry cycles.

## Key Features

- **Per-message retry configuration** - Set retry limits on individual messages via headers
- **Cycle-aware retries** - Maintains consistent retry counts even when messages cycle back through TTL or shovel
- **Reason tracking** - Track different death reasons (rejected, expired, maxlen, delivery_limit)
- **Advanced routing** - Uses headers exchange for dead messages, enabling sophisticated DLQ topologies
- **Default to topic routing** - Messages those shouldn't be dead lettered yet, are routed using standard topic exchange semantics

## Installation

1. Download the compatible version from the [releases](releases/) directory
2. Place the `.ez` file in your RabbitMQ plugins directory
3. Enable the plugin:
```bash
rabbitmq-plugins enable rabbitmq_retry_aware_dlx
```

### Version Compatibility

| RabbitMQ Version | Plugin Version |
|------------------|----------------|
| 4.0.4+          | v4.1.2          |
| 4.0.0 - 4.0.3   | v4.0.3          |
| 3.13.3 - 3.13.7 | v3.13.7         |
| 3.13.0 - 3.13.2 | v3.13.2         |

For other versions, please [create an issue](https://github.com/your-repo/issues).

## Usage

### Basic Setup

1. Declare an exchange with type `radlx`:
```bash
rabbitmqadmin declare exchange name=my-dlx type=radlx
```

2. Configure message headers:
- `radlx-max-per-cycle` (integer) - Maximum retries per cycle
- `radlx-track-queue` (string) - Queue name to track deaths for
- `radlx-track-reason` (string) - Death reason to track: "rejected", "expired", "maxlen", or "delivery_limit"

3. Bind queues:
- Regular bindings use topic exchange semantics
- Header bindings with `radlx.dead.source` match dead-lettered messages

### Message Flow

```
┌──────────────┐    reject/expire    ┌────────────────┐
│ Source Queue ├────────────────────►│ RADLX Exchange │
└──────────────┘                     └───────┬────────┘
                                             │
                                             ▼
                                   ┌─────────────────────┐
                                   │ Death count = limit?│
                                   └──────────┬──────────┘
                                              │
                    ┌─────────────────────────┴─────────────────────────┐
                    │                                                   │
                    ▼ Yes                                               ▼ No
        ┌────────────────────┐                              ┌───────────────────┐
        │  Headers routing   │                              │  Topic routing    │
        │+ radlx.dead.source │                              │ (any queue by     │
        └─────────┬──────────┘                              │  routing key)     │
                  │                                         └───────────────────┘
                  ▼
          ┌──────────────┐
          │ Dead Letter  │
          │   Queue(s)   │
          └──────────────┘
```

## Examples

### 1. Simple Retry with DLQ

**Setup**: Messages get 2 retries before going to DLQ. When rejected, messages go to the RADLX exchange which routes back to the same queue until the retry limit is reached.

```bash
# Create RADLX exchange
rabbitmqadmin declare exchange name=dlx type=radlx

# Create source queue with dead letter exchange
rabbitmqadmin declare queue name=orders arguments='{"x-dead-letter-exchange":"dlx"}'

# Create final dead letter queue
rabbitmqadmin declare queue name=orders.failed

# Bind source queue to RADLX for retry routing (topic semantics)
rabbitmqadmin declare binding source=dlx destination=orders routing_key=orders

# Bind DLQ to RADLX with header matching for dead messages
rabbitmqadmin declare binding source=dlx destination=orders.failed \
  arguments='{"x-match":"all","radlx.dead.source":"orders"}'

```

**Flow Diagram**:
```
First Rejection (death count = 1):
┌────────┐  reject   ┌────────┐  topic routing  ┌────────┐
│ Orders ├──────────►│ RADLX  ├────────────────►│ Orders │
└────────┘           └────────┘  (count < 2)    └────────┘

Second Rejection (death count = 2):
┌────────┐  reject   ┌────────┐  headers routing  ┌──────────────┐
│ Orders ├──────────►│ RADLX  ├──────────────────►│Orders.Failed │
└────────┘           └────────┘  (count = 2)      └──────────────┘
                              + radlx.dead.source
```

### 2. Retry with Delay Queue (TTL)

**Setup**: Messages retry with a 5-second delay between attempts. Rejected messages go to a delay queue with TTL, then back to the router exchange.

```bash
# Create router exchange (standard direct type)
rabbitmqadmin declare exchange name=router type=direct

# Create RADLX exchange
rabbitmqadmin declare exchange name=dlx type=radlx

# Create source queue with RADLX as dead letter exchange
rabbitmqadmin declare queue name=orders arguments='{"x-dead-letter-exchange":"dlx"}'

# Create delay queue with TTL and router as DLX
rabbitmqadmin declare queue name=orders.delay \
  arguments='{"x-message-ttl":5000,"x-dead-letter-exchange":"router"}'

# Create final DLQ
rabbitmqadmin declare queue name=orders.failed

# Router sends messages back to orders queue
rabbitmqadmin declare binding source=router destination=orders routing_key=orders

# RADLX uses wildcard to send non-dead messages to delay queue
rabbitmqadmin declare binding source=dlx destination=orders.delay routing_key="#"

# RADLX sends dead messages to failed queue
rabbitmqadmin declare binding source=dlx destination=orders.failed \
  arguments='{"x-match":"all","radlx.dead.source":"orders"}'

```

**Flow Diagram**:
```
First Cycle:
┌────────┐ reject  ┌────────┐ topic (#)  ┌──────────────┐ TTL expires ┌────────┐
│ Orders ├────────►│ RADLX  ├───────────►│Orders.Delay  ├────────────►│ Router │
└────────┘         └────────┘            └──────────────┘   (5 sec)   └───┬────┘
    ▲                                                                     │
    └─────────────────────────────────────────────────────────────────────┘

Second Cycle (after 2 rejections):
┌────────┐ reject  ┌────────┐ headers     ┌──────────────┐
│ Orders ├────────►│ RADLX  ├────────────►│Orders.Failed │
└────────┘         └────────┘             └──────────────┘
                   + radlx.dead.source
```

### 3. Multiple Cycles with TTL DLQ

**Setup**: DLQ itself has TTL, creating continuous retry cycles. Messages get 2 retries per cycle.

```bash
# Create exchanges
rabbitmqadmin declare exchange name=router type=direct
rabbitmqadmin declare exchange name=dlx type=radlx

# Create queues
rabbitmqadmin declare queue name=orders arguments='{"x-dead-letter-exchange":"dlx"}'
rabbitmqadmin declare queue name=orders.failed \
  arguments='{"x-message-ttl":5000,"x-dead-letter-exchange":"router"}'

# Bindings
rabbitmqadmin declare binding source=router destination=orders routing_key=orders
rabbitmqadmin declare binding source=dlx destination=orders routing_key=orders
rabbitmqadmin declare binding source=dlx destination=orders.failed \
  arguments='{"x-match":"all","radlx.dead.source":"orders"}'

```

**Flow Diagram**:
```
Cycle 1:
┌────────┐→reject(1)→┌────────┐→topic→┌────────┐→reject(2)→┌────────┐→headers→┌──────────────┐
│ Orders │           │ RADLX  │       │ Orders │           │ RADLX  │         │Orders.Failed │
└────────┘           └────────┘       └────────┘           └────────┘         └──────┬───────┘
                                                                                     │TTL
Cycle 2:                                                                             ▼
┌────────┐←────────────────────────────────────────────────────────────────────┌────────┐
│ Orders │→reject(3)→┌────────┐→topic→┌────────┐→reject(4)→┌────────┐→headers-→│ Router │
└────────┘           │ RADLX  │       │ Orders │           │ RADLX  │          └────────┘
                     └────────┘       └────────┘           └───┬────┘
                                                               │
                                                               ▼
                                                          ┌──────────────┐
                                                          │Orders.Failed │ (cycle repeats)
                                                          └──────────────┘
```


### 5. Priority-Based DLQ Routing

**Setup**: High-priority messages go to urgent DLQ, all messages go to logger, low-priority to standard DLQ.

```bash
# Create RADLX exchange
rabbitmqadmin declare exchange name=dlx type=radlx

# Create queues
rabbitmqadmin declare queue name=orders arguments='{"x-dead-letter-exchange":"dlx"}'
rabbitmqadmin declare queue name=dlq.urgent
rabbitmqadmin declare queue name=dlq.logger
rabbitmqadmin declare queue name=dlq.standard

# Retry binding
rabbitmqadmin declare binding source=dlx destination=orders routing_key=orders

# High priority to urgent queue (all match)
rabbitmqadmin declare binding source=dlx destination=dlq.urgent \
  arguments='{"x-match":"all","radlx.dead.source":"orders","priority":"high"}'

# Logger catches all dead messages (any match)
rabbitmqadmin declare binding source=dlx destination=dlq.logger \
  arguments='{"x-match":"any","radlx.dead.source":"orders"}'

# Standard priority messages
rabbitmqadmin declare binding source=dlx destination=dlq.standard \
  arguments='{"x-match":"all","radlx.dead.source":"orders","priority":"standard"}'

```

**Flow Diagram**:
```
High Priority Message (death count = 1):
┌────────┐ reject  ┌────────┐ headers matching  ┌─────────────┐
│ Orders ├────────►│ RADLX  ├──────────────────►│ DLQ.Urgent  │ (priority=high)
└────────┘         └───┬────┘                   └─────────────┘
                       │
                       ├────────────────────────►┌─────────────┐
                       │                         │ DLQ.Logger  │ (any match)
                       │                         └─────────────┘
                       │
Standard Priority:     │
                       └────────────────────────►┌───────────────┐
                                                 │ DLQ.Standard  │ (priority=standard)
                                                 └───────────────┘
```

## How It's Different From Standard Exchanges

With standard RabbitMQ exchanges, implementing retry logic with dead-lettering requires:

1. **Application-level death count checking** - Your code must examine the death history
2. **Manual routing decisions** - Explicitly publish to DLQ and acknowledge from source queue
3. **Non-atomic operations** - Risk of duplicates or message loss between publish and ack
4. **Complex error handling** - Must handle failures between DLQ publish and source ack

The RADLX exchange solves the **dual-write problem** by making death decisions atomic within the exchange itself. No application code needed, no risk of duplicates.

## Behind the scenes: How it works?

1. **Death Tracking**: The plugin examines the message's death history to count occurrences for the specified queue/reason combination

2. **Threshold Check**: Uses modulo arithmetic (`count % max-per-cycle == 0`) to ensure consistent retry counts across cycles

3. **Routing Decision**:
   - Below threshold: Routes using topic semantics (depending on routing keys)
   - At threshold: Adds `radlx.dead.source` header and routes using headers exchange semantics (depending on messages headers plus the added `radlx.dead.source`)

4. **Headers Matching**: Supports all standard headers exchange match modes:
   - `all` - All headers must match
   - `any` - At least one header must match
   - `all-with-x` - All headers including x- prefixed
   - `any-with-x` - Any header including x- prefixed


## Contributing

Issues and pull requests are welcome. For support of additional RabbitMQ versions, please create an issue.

## License

This project is licensed under the Mozilla Public License 2.0 - see the [LICENSE](LICENSE) file for details.
