# @galaxy-stack/orbit-microservices-rmq

[![npm version](https://img.shields.io/npm/v/@galaxy-stack/orbit-microservices-rmq.svg)](https://www.npmjs.com/package/@galaxy-stack/orbit-microservices-rmq)
[![docs](https://img.shields.io/badge/docs-galaxy--orbit--framework.vercel.app-blue)](https://galaxy-orbit-framework.vercel.app)

Part of the [Orbit framework](https://github.com/galaxy-orbit/orbit) — a NestJS-style backend framework for [Bun](https://bun.sh).

## Installation

```bash
bun add @galaxy-stack/orbit-microservices-rmq
```

# @galaxy-stack/orbit-microservices-rmq

> **Status**: ✅ Full Implementation - Complete AMQP 0-9-1 protocol với Bun native TCP

## Mô tả

RabbitMQ transport implementation cho Orbit microservices với full AMQP protocol support.

## Tính năng

- Complete AMQP 0-9-1 protocol encoder/decoder
- Connection handshake với PLAIN authentication
- Channel management và heartbeats
- Queue declaration với durable/exclusive/autoDelete options
- Message publishing với persistent delivery mode
- Consumer với ACK/NACK acknowledgments
- RPC pattern với exclusive reply queues
- Per-channel delivery tracking cho parallel consumption
- Automatic reconnection với consumer re-subscription
- Graceful shutdown

## Cài đặt

```bash
bun add @galaxy-stack/orbit-microservices-rmq
```

## Sử dụng

### RabbitMQ Server

```typescript
import { BunFactory } from '@galaxy-stack/orbit-core';
import '@galaxy-stack/orbit-microservices-rmq';

const app = await BunFactory.createMicroservice(AppModule, {
  transport: 'RMQ',
  options: {
    urls: ['amqp://localhost:5672'],
    queue: 'my_queue',
    queueOptions: {
      durable: true,
    },
    prefetchCount: 10,
  },
});

await app.listen();
// [RmqServer] Connected to RabbitMQ
// [RmqServer] Listening on amqp://localhost:5672
```

### RabbitMQ Client

```typescript
import { MicroservicesModule } from '@galaxy-stack/orbit-microservices';
import '@galaxy-stack/orbit-microservices-rmq';

@Module({
  imports: [
    MicroservicesModule.register({
      name: 'RMQ_SERVICE',
      transport: 'RMQ',
      options: {
        urls: ['amqp://localhost:5672'],
        queue: 'my_queue',
      },
    }),
  ],
})
class ClientModule {}
```

### Message Handlers

```typescript
import { Controller, MessagePattern, EventPattern } from '@galaxy-stack/orbit-microservices';

@Controller()
class OrderController {
  @MessagePattern('orders.process')
  async processOrder(data: { orderId: number; items: any[] }) {
    // Process order...
    return { success: true, orderId: data.orderId };
  }

  @EventPattern('orders.completed')
  handleOrderCompleted(data: { orderId: number }) {
    console.log('Order completed:', data.orderId);
  }
}
```

### Client Usage

```typescript
@Injectable()
class OrderService {
  constructor(
    @Inject('RMQ_SERVICE') private client: ClientProxy
  ) {}

  async processOrder(orderId: number, items: any[]) {
    return this.client.send('orders.process', { orderId, items });
  }

  notifyCompleted(orderId: number): void {
    this.client.emit('orders.completed', { orderId });
  }
}
```

## Options

### RmqServerOptions

```typescript
interface RmqServerOptions {
  urls?: string[];             // AMQP URLs
  host?: string;               // Default: 'localhost'
  port?: number;               // Default: 5672
  username?: string;           // Default: 'guest'
  password?: string;           // Default: 'guest'
  vhost?: string;              // Default: '/'
  queue?: string;              // Queue name
  queueOptions?: {
    durable?: boolean;         // Survive broker restart
    exclusive?: boolean;       // Exclusive to connection
    autoDelete?: boolean;      // Delete when not in use
  };
  prefetchCount?: number;      // Consumer prefetch (default: 0)
  noAck?: boolean;             // Auto-acknowledge (default: false)
  persistent?: boolean;        // Persistent messages (default: true)
  heartbeat?: number;          // Heartbeat interval in seconds
  reconnect?: boolean;         // Enable reconnection (default: true)
  reconnectAttempts?: number;  // Max reconnect attempts (default: 10)
  reconnectDelay?: number;     // Delay between attempts (default: 5000ms)
}
```

### RmqClientOptions

```typescript
interface RmqClientOptions extends RmqServerOptions {
  requestTimeout?: number;     // Default: 30000ms
  replyQueue?: string;         // Custom reply queue name
}
```

## AMQP Protocol

Full implementation of AMQP 0-9-1 protocol:

### Frame Types

| Type | Value | Description |
|------|-------|-------------|
| Method | 1 | Protocol methods |
| Header | 2 | Content header |
| Body | 3 | Content body |
| Heartbeat | 8 | Connection keepalive |

### Supported Methods

**Connection Class:**
- Connection.Start/Start-Ok
- Connection.Tune/Tune-Ok
- Connection.Open/Open-Ok
- Connection.Close/Close-Ok

**Channel Class:**
- Channel.Open/Open-Ok
- Channel.Close/Close-Ok

**Queue Class:**
- Queue.Declare/Declare-Ok
- Queue.Delete/Delete-Ok

**Basic Class:**
- Basic.Qos/Qos-Ok
- Basic.Consume/Consume-Ok
- Basic.Cancel/Cancel-Ok
- Basic.Publish
- Basic.Deliver
- Basic.Ack
- Basic.Nack

### Frame Format

```
┌───────────┬─────────────┬──────────────┬────────────────┬──────────┐
│ Type (1B) │ Channel (2B)│ Size (4B)    │ Payload (N)    │ End (1B) │
└───────────┴─────────────┴──────────────┴────────────────┴──────────┘
```

## Queue Naming

| Type | Pattern |
|------|---------|
| Request | `orbit.rpc.{pattern}` |
| Reply | `amq.rabbitmq.reply-to` or exclusive queue |
| Event | `orbit.event.{pattern}` |

## RmqConnection

Low-level AMQP connection với full protocol support:

```typescript
import { RmqConnection } from '@galaxy-stack/orbit-microservices-rmq';

const conn = new RmqConnection({
  host: 'localhost',
  port: 5672,
  username: 'guest',
  password: 'guest',
  vhost: '/',
  heartbeat: 60,
});

conn.onError((error) => {
  console.error('Connection error:', error);
});

conn.onReconnect(() => {
  console.log('Reconnected to RabbitMQ');
});

await conn.connect();

// Channel operations
const channel = await conn.createChannel();
await conn.declareQueue(channel, 'my-queue', { durable: true });
await conn.setQos(channel, 10);

// Consume messages
await conn.consume(channel, 'my-queue', (msg) => {
  console.log('Received:', msg.content.toString());
  conn.ack(channel, msg.deliveryTag);
});

// Publish messages
await conn.publish(channel, '', 'my-queue', Buffer.from('Hello!'), {
  persistent: true,
});

await conn.close();
```

## Message Properties

```typescript
interface BasicProperties {
  contentType?: string;        // MIME type
  contentEncoding?: string;    // Encoding
  headers?: Record<string, any>;
  deliveryMode?: 1 | 2;        // 1=transient, 2=persistent
  priority?: number;           // 0-9
  correlationId?: string;      // RPC correlation
  replyTo?: string;            // Reply queue
  expiration?: string;         // TTL in ms
  messageId?: string;          // Message ID
  timestamp?: number;          // Unix timestamp
  type?: string;               // Message type
  userId?: string;             // User ID
  appId?: string;              // App ID
}
```

## Acknowledgments

```typescript
// Manual acknowledgment
@MessagePattern('task')
async handleTask(data: any, context: RmqContext) {
  try {
    await processTask(data);
    context.getChannelRef().ack(context.getMessage());
  } catch (error) {
    context.getChannelRef().nack(context.getMessage(), false, true);
  }
}
```

## Error Handling

```typescript
try {
  const result = await client.send('orders.validate', { orderId: 123 });
} catch (error) {
  console.error(error.message); // 'Order not found'
}
```

## Reconnection

Automatic reconnection với consumer re-subscription:

- Retry attempts: configurable (default: 10)
- Retry delay: configurable (default: 5000ms)
- Automatic queue re-declaration
- Automatic consumer re-subscription after reconnect

## Parallel Channel Support

Per-channel delivery tracking ensures correct message routing when multiple channels consume simultaneously:

```typescript
// Multiple consumers on different channels work correctly
const channel1 = await conn.createChannel();
const channel2 = await conn.createChannel();

await conn.consume(channel1, 'queue1', handler1);
await conn.consume(channel2, 'queue2', handler2);
// Interleaved frames are correctly routed to each consumer
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
├─────────────────────────────────────────────────────────┤
│      RmqServer           │         RmqClient           │
├─────────────────────────────────────────────────────────┤
│              RmqConnection (Channel Management)         │
├─────────────────────────────────────────────────────────┤
│        AmqpEncoder / AmqpDecoder / AmqpFramer          │
├─────────────────────────────────────────────────────────┤
│                   Bun.connect()                         │
└─────────────────────────────────────────────────────────┘
```
