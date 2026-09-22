import { Server, type TransportOptions, type OutgoingMessage } from '@galaxy-stack/orbit-microservices';
import { RmqConnection, type RmqConnectionOptions } from './rmq-connection';
import type { DeliveredMessage, BasicProperties } from './amqp-protocol';

export interface RmqServerOptions extends TransportOptions, Partial<RmqConnectionOptions> {
  queue?: string;
  queueOptions?: {
    durable?: boolean;
    exclusive?: boolean;
    autoDelete?: boolean;
    args?: Record<string, any>;
  };
  prefetchCount?: number;
  noAck?: boolean;
  persistent?: boolean;
  requeue?: boolean;
  serializer?: {
    serialize: (value: any) => string;
    deserialize: (value: string) => any;
  };
}

export class RmqServer extends Server {
  private readonly options: RmqServerOptions;
  private connection: RmqConnection | null = null;
  private channel = 1;
  private isListening = false;
  private consumerTags: Map<string, string> = new Map();
  private serializer: { serialize: (v: any) => string; deserialize: (v: string) => any };

  constructor(options: RmqServerOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 5672,
      username: options.username || 'guest',
      password: options.password || 'guest',
      vhost: options.vhost || '/',
      queue: options.queue || 'orbit_queue',
      queueOptions: options.queueOptions || { durable: true },
      prefetchCount: options.prefetchCount || 10,
      noAck: options.noAck || false,
      persistent: options.persistent ?? true,
      requeue: options.requeue ?? true,
      ...options,
    };
    
    this.serializer = options.serializer || {
      serialize: JSON.stringify,
      deserialize: JSON.parse,
    };
  }

  async listen(callback?: () => void): Promise<void> {
    try {
      await this.connect();
      this.isListening = true;
      await this.bindHandlers();
      console.log(`[RmqServer] Listening on amqp://${this.options.host}:${this.options.port}${this.options.vhost}`);
      callback?.();
    } catch (error) {
      console.error('[RmqServer] Failed to start:', error);
      throw error;
    }
  }

  private async connect(): Promise<void> {
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
      console.error('[RmqServer] Connection error:', err.message);
    });

    this.connection.onReconnect(async () => {
      console.log('[RmqServer] Reconnected, rebinding handlers...');
      await this.rebindHandlers();
    });

    await this.connection.connect();
    await this.connection.openChannel(this.channel);
    await this.connection.qos(this.channel, this.options.prefetchCount || 10);
    
    console.log(`[RmqServer] Connected to RabbitMQ`);
  }

  protected async bindHandlers(): Promise<void> {
    const handlers = this.getHandlers();
    
    for (const [pattern] of handlers) {
      const queue = this.getQueueName(pattern);
      
      await this.connection!.declareQueue(this.channel, queue, {
        durable: this.options.queueOptions?.durable ?? true,
        exclusive: this.options.queueOptions?.exclusive ?? false,
        autoDelete: this.options.queueOptions?.autoDelete ?? false,
        args: this.options.queueOptions?.args,
      });
      
      const consumerTag = await this.connection!.consume(
        this.channel,
        queue,
        (msg) => this.handleMessage(pattern, msg),
        { noAck: this.options.noAck }
      );
      
      this.consumerTags.set(queue, consumerTag);
      console.log(`[RmqServer] Consuming from queue: ${queue}`);
    }
  }

  private async rebindHandlers(): Promise<void> {
    this.consumerTags.clear();
    await this.connection!.openChannel(this.channel);
    await this.connection!.qos(this.channel, this.options.prefetchCount || 10);
    await this.bindHandlers();
  }

  protected async handleMessage(pattern: string | object, msg: DeliveredMessage): Promise<void> {
    try {
      const content = msg.content.toString('utf8');
      const { data, id } = this.serializer.deserialize(content);
      
      const isRpc = !!msg.properties.replyTo && !!msg.properties.correlationId;
      
      if (isRpc) {
        const startTime = performance.now();
        
        const respond = async (response: OutgoingMessage) => {
          const duration = Math.round(performance.now() - startTime);
          
          const replyContent = Buffer.from(this.serializer.serialize({
            response: response.response,
            error: response.err ? { message: response.err } : undefined,
            id: msg.properties.correlationId,
            duration,
          }));
          
          const replyProps: BasicProperties = {
            correlationId: msg.properties.correlationId,
            contentType: 'application/json',
          };
          
          this.connection!.publish(
            this.channel,
            '',
            msg.properties.replyTo!,
            replyContent,
            replyProps
          );
          
          if (!this.options.noAck) {
            this.connection!.ack(this.channel, msg.deliveryTag);
          }
        };
        
        await super.handleMessage(pattern, data, respond);
      } else {
        await super.handleMessage(pattern, data);
        
        if (!this.options.noAck) {
          this.connection!.ack(this.channel, msg.deliveryTag);
        }
      }
    } catch (error) {
      console.error('[RmqServer] Error processing message:', error);
      
      if (!this.options.noAck) {
        this.connection!.nack(this.channel, msg.deliveryTag, false, this.options.requeue ?? true);
      }
    }
  }

  private getQueueName(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' ? JSON.stringify(pattern) : pattern;
    return `orbit.rpc.${patternStr.replace(/\//g, '.')}`;
  }

  async close(): Promise<void> {
    this.isListening = false;
    
    if (this.connection) {
      for (const [queue, tag] of this.consumerTags) {
        try {
          await this.connection.cancel(this.channel, tag);
        } catch {}
      }
      this.consumerTags.clear();
      
      try {
        await this.connection.closeChannel(this.channel);
      } catch {}
      
      await this.connection.close();
      this.connection = null;
    }
    
    console.log('[RmqServer] Closed');
  }

  get listening(): boolean {
    return this.isListening;
  }

  get subscribedQueues(): string[] {
    return Array.from(this.consumerTags.keys());
  }
}
