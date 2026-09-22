import { ClientProxy, type ReadPacket, type WritePacket } from '@galaxy-stack/orbit-microservices';
import { RmqConnection, type RmqConnectionOptions } from './rmq-connection';
import type { BasicProperties, DeliveredMessage } from './amqp-protocol';

export interface RmqClientOptions extends Partial<RmqConnectionOptions> {
  queue?: string;
  queueOptions?: {
    durable?: boolean;
    exclusive?: boolean;
    autoDelete?: boolean;
    args?: Record<string, any>;
  };
  persistent?: boolean;
  timeout?: number;
  serializer?: {
    serialize: (value: any) => string;
    deserialize: (value: string) => any;
  };
}

export class RmqClient extends ClientProxy {
  private readonly options: RmqClientOptions;
  private connection: RmqConnection | null = null;
  private channel = 1;
  private replyQueue = '';
  private replyConsumerTag = '';
  private rmqPendingRequests: Map<string, { resolve: Function; reject: Function; timer: ReturnType<typeof setTimeout> }> = new Map();
  private serializer: { serialize: (v: any) => string; deserialize: (v: string) => any };

  constructor(options: RmqClientOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 5672,
      username: options.username || 'guest',
      password: options.password || 'guest',
      vhost: options.vhost || '/',
      queue: options.queue || 'orbit_queue',
      queueOptions: options.queueOptions || { durable: true },
      persistent: options.persistent ?? true,
      timeout: options.timeout || 30000,
      ...options,
    };
    
    this.serializer = options.serializer || {
      serialize: JSON.stringify,
      deserialize: JSON.parse,
    };
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;

    this.connection = new RmqConnection({
      host: this.options.host,
      port: this.options.port,
      username: this.options.username,
      password: this.options.password,
      vhost: this.options.vhost,
      heartbeat: this.options.heartbeat,
      reconnect: true,
      reconnectAttempts: 10,
      reconnectDelay: 1000,
    });

    this.connection.onError((err) => {
      console.error('[RmqClient] Connection error:', err.message);
    });

    this.connection.onReconnect(async () => {
      console.log('[RmqClient] Reconnected, setting up reply queue...');
      await this.setupReplyQueue();
    });

    await this.connection.connect();
    await this.connection.openChannel(this.channel);
    await this.setupReplyQueue();
    
    this.isConnected = true;
    console.log(`[RmqClient] Connected to RabbitMQ`);
  }

  private async setupReplyQueue(): Promise<void> {
    const result = await this.connection!.declareQueue(this.channel, '', {
      exclusive: true,
      autoDelete: true,
    });
    
    this.replyQueue = result.queue;
    
    this.replyConsumerTag = await this.connection!.consume(
      this.channel,
      this.replyQueue,
      (msg) => this.handleReply(msg),
      { noAck: true }
    );
  }

  private handleReply(msg: DeliveredMessage): void {
    const correlationId = msg.properties.correlationId;
    if (!correlationId) return;
    
    const pending = this.rmqPendingRequests.get(correlationId);
    if (!pending) return;
    
    clearTimeout(pending.timer);
    this.rmqPendingRequests.delete(correlationId);
    
    try {
      const content = msg.content.toString('utf8');
      const reply = this.serializer.deserialize(content);
      
      if (reply.error) {
        pending.reject(new Error(reply.error.message || 'Unknown error'));
      } else {
        pending.resolve(reply.response);
      }
    } catch (error) {
      pending.reject(error);
    }
  }

  protected publish(packet: ReadPacket, callback: (packet: WritePacket) => void): () => void {
    // RmqClient overrides send() with a correlation-ID based flow,
    // so the generic publish hook is intentionally a no-op.
    void packet;
    void callback;
    return () => {};
  }

  protected async dispatchEvent(packet: ReadPacket): Promise<void> {
    await this.connect();
    
    const queue = this.getEventQueueName(packet.pattern);
    
    await this.connection!.declareQueue(this.channel, queue, {
      durable: this.options.queueOptions?.durable ?? true,
    });
    
    const content = Buffer.from(this.serializer.serialize({
      pattern: packet.pattern,
      data: packet.data,
    }));
    
    const properties: BasicProperties = {
      contentType: 'application/json',
      deliveryMode: this.options.persistent ? 2 : 1,
    };
    
    this.connection!.publish(this.channel, '', queue, content, properties);
  }

  send<TResult = any>(pattern: any, data: any): Promise<TResult> {
    return new Promise(async (resolve, reject) => {
      try {
        await this.connect();

        const correlationId = this.generateId();
        const queue = this.getQueueName(pattern);
        
        await this.connection!.declareQueue(this.channel, queue, {
          durable: this.options.queueOptions?.durable ?? true,
        });

        const timer = setTimeout(() => {
          if (this.rmqPendingRequests.has(correlationId)) {
            this.rmqPendingRequests.delete(correlationId);
            reject(new Error('Request timeout'));
          }
        }, this.options.timeout || 30000);

        this.rmqPendingRequests.set(correlationId, { resolve, reject, timer });

        const content = Buffer.from(this.serializer.serialize({
          pattern,
          data,
          id: correlationId,
        }));
        
        const properties: BasicProperties = {
          contentType: 'application/json',
          correlationId,
          replyTo: this.replyQueue,
          deliveryMode: this.options.persistent ? 2 : 1,
        };
        
        this.connection!.publish(this.channel, '', queue, content, properties);
      } catch (error) {
        reject(error);
      }
    });
  }

  private getQueueName(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' ? JSON.stringify(pattern) : pattern;
    return `orbit.rpc.${patternStr.replace(/\//g, '.')}`;
  }

  private getEventQueueName(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' ? JSON.stringify(pattern) : pattern;
    return `orbit.event.${patternStr.replace(/\//g, '.')}`;
  }

  async close(): Promise<void> {
    this.isConnected = false;
    
    for (const [id, pending] of this.rmqPendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Client closed'));
    }
    this.rmqPendingRequests.clear();

    if (this.connection) {
      if (this.replyConsumerTag) {
        try {
          await this.connection.cancel(this.channel, this.replyConsumerTag);
        } catch {}
      }
      
      try {
        await this.connection.closeChannel(this.channel);
      } catch {}
      
      await this.connection.close();
      this.connection = null;
    }
    
    console.log('[RmqClient] Closed');
  }

  get connected(): boolean {
    return this.isConnected;
  }
}
