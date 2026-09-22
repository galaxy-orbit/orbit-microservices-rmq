import {
  AMQP_PROTOCOL_HEADER,
  FRAME_METHOD,
  FRAME_HEADER,
  FRAME_BODY,
  FRAME_HEARTBEAT,
  CLASS_CONNECTION,
  CLASS_CHANNEL,
  CLASS_QUEUE,
  CLASS_BASIC,
  CONNECTION_START,
  CONNECTION_START_OK,
  CONNECTION_TUNE,
  CONNECTION_TUNE_OK,
  CONNECTION_OPEN,
  CONNECTION_OPEN_OK,
  CONNECTION_CLOSE,
  CONNECTION_CLOSE_OK,
  CHANNEL_OPEN,
  CHANNEL_OPEN_OK,
  CHANNEL_CLOSE,
  CHANNEL_CLOSE_OK,
  QUEUE_DECLARE,
  QUEUE_DECLARE_OK,
  BASIC_QOS,
  BASIC_QOS_OK,
  BASIC_CONSUME,
  BASIC_CONSUME_OK,
  BASIC_CANCEL,
  BASIC_CANCEL_OK,
  BASIC_PUBLISH,
  BASIC_DELIVER,
  BASIC_ACK,
  BASIC_NACK,
  AmqpEncoder,
  AmqpDecoder,
  type AmqpFrame,
  type BasicProperties,
  type DeliveredMessage,
  type ContentHeader,
} from './amqp-protocol';

export interface RmqConnectionOptions {
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  vhost?: string;
  heartbeat?: number;
  frameMax?: number;
  channelMax?: number;
  reconnect?: boolean;
  reconnectAttempts?: number;
  reconnectDelay?: number;
}

interface PendingMessage {
  channel: number;
  deliveryInfo: {
    consumerTag: string;
    deliveryTag: bigint;
    redelivered: boolean;
    exchange: string;
    routingKey: string;
  };
  header: ContentHeader | null;
  bodyChunks: Buffer[];
  expectedSize: bigint;
}

interface ConsumerRegistration {
  channel: number;
  queue: string;
  handler: MessageHandler;
  options: {
    consumerTag: string;
    noLocal: boolean;
    noAck: boolean;
    exclusive: boolean;
    args: Record<string, any>;
  };
}

type MessageHandler = (msg: DeliveredMessage) => void;
type MethodWaiter = { resolve: (args: Buffer) => void; reject: (err: Error) => void };

export class RmqConnection {
  private socket: ReturnType<typeof Bun.connect> | null = null;
  private decoder = new AmqpDecoder();
  private isConnected = false;
  private options: Required<RmqConnectionOptions>;
  
  private channelMax = 0;
  private frameMax = 131072;
  private heartbeatInterval = 0;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private lastHeartbeat = Date.now();
  
  private methodWaiters: Map<string, MethodWaiter> = new Map();
  private consumers: Map<string, MessageHandler> = new Map();
  private consumerRegistrations: Map<string, ConsumerRegistration> = new Map();
  private pendingMessages: Map<string, PendingMessage> = new Map();
  private channelDeliveryKeys: Map<number, string> = new Map();
  
  private onErrorCallback: ((err: Error) => void) | null = null;
  private onCloseCallback: (() => void) | null = null;
  private onReconnectCallback: (() => void) | null = null;
  
  private reconnectAttempts = 0;
  private isReconnecting = false;
  private isClosed = false;

  constructor(options: RmqConnectionOptions = {}) {
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 5672,
      username: options.username || 'guest',
      password: options.password || 'guest',
      vhost: options.vhost || '/',
      heartbeat: options.heartbeat || 60,
      frameMax: options.frameMax || 131072,
      channelMax: options.channelMax || 2047,
      reconnect: options.reconnect ?? true,
      reconnectAttempts: options.reconnectAttempts || 10,
      reconnectDelay: options.reconnectDelay || 1000,
    };
  }

  onError(callback: (err: Error) => void): void {
    this.onErrorCallback = callback;
  }

  onClose(callback: () => void): void {
    this.onCloseCallback = callback;
  }

  onReconnect(callback: () => void): void {
    this.onReconnectCallback = callback;
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;

    return new Promise((resolve, reject) => {
      const connectPromise = Bun.connect({
        hostname: this.options.host,
        port: this.options.port,
        socket: {
          open: async (socket) => {
            socket.write(AMQP_PROTOCOL_HEADER);
          },
          data: (socket, data) => {
            this.handleData(Buffer.from(data));
          },
          error: (socket, error) => {
            const err = error instanceof Error ? error : new Error(String(error));
            this.onErrorCallback?.(err);
            reject(err);
          },
          close: () => {
            this.handleClose();
          },
        },
      });

      connectPromise.then(async (socket) => {
        this.socket = socket as any;
        
        try {
          await this.waitForMethod(CLASS_CONNECTION, CONNECTION_START);
          await this.sendConnectionStartOk();
          
          const tuneArgs = await this.waitForMethod(CLASS_CONNECTION, CONNECTION_TUNE);
          const tune = AmqpDecoder.parseConnectionTune(tuneArgs);
          
          this.channelMax = Math.min(tune.channelMax || this.options.channelMax, this.options.channelMax);
          this.frameMax = Math.min(tune.frameMax || this.options.frameMax, this.options.frameMax);
          this.heartbeatInterval = Math.min(tune.heartbeat || this.options.heartbeat, this.options.heartbeat);
          
          await this.sendConnectionTuneOk();
          await this.sendConnectionOpen();
          await this.waitForMethod(CLASS_CONNECTION, CONNECTION_OPEN_OK);
          
          this.isConnected = true;
          this.reconnectAttempts = 0;
          this.startHeartbeat();
          
          resolve();
        } catch (err) {
          reject(err);
        }
      }).catch(reject);
    });
  }

  private handleData(data: Buffer): void {
    this.decoder.append(data);
    
    for (const frame of this.decoder.parseFrames()) {
      this.processFrame(frame);
    }
  }

  private processFrame(frame: AmqpFrame): void {
    this.lastHeartbeat = Date.now();
    
    switch (frame.type) {
      case FRAME_METHOD:
        this.handleMethodFrame(frame);
        break;
      case FRAME_HEADER:
        this.handleHeaderFrame(frame);
        break;
      case FRAME_BODY:
        this.handleBodyFrame(frame);
        break;
      case FRAME_HEARTBEAT:
        break;
    }
  }

  private handleMethodFrame(frame: AmqpFrame): void {
    const method = AmqpDecoder.parseMethod(frame.payload);
    const key = `${method.classId}:${method.methodId}`;
    
    const waiter = this.methodWaiters.get(key);
    if (waiter) {
      this.methodWaiters.delete(key);
      waiter.resolve(method.args);
      return;
    }
    
    if (method.classId === CLASS_BASIC && method.methodId === BASIC_DELIVER) {
      const deliver = AmqpDecoder.parseBasicDeliver(method.args);
      const deliveryKey = `${frame.channel}:${deliver.deliveryTag}`;
      this.channelDeliveryKeys.set(frame.channel, deliveryKey);
      this.pendingMessages.set(deliveryKey, {
        channel: frame.channel,
        deliveryInfo: deliver,
        header: null,
        bodyChunks: [],
        expectedSize: 0n,
      });
    } else if (method.classId === CLASS_CONNECTION && method.methodId === CONNECTION_CLOSE) {
      const close = AmqpDecoder.parseConnectionClose(method.args);
      console.error(`[RmqConnection] Server closed connection: ${close.replyCode} - ${close.replyText}`);
      this.sendConnectionCloseOk();
    } else if (method.classId === CLASS_CHANNEL && method.methodId === CHANNEL_CLOSE) {
      const close = AmqpDecoder.parseConnectionClose(method.args);
      console.error(`[RmqConnection] Channel closed: ${close.replyCode} - ${close.replyText}`);
      this.sendChannelCloseOk(frame.channel);
    }
  }

  private handleHeaderFrame(frame: AmqpFrame): void {
    const deliveryKey = this.channelDeliveryKeys.get(frame.channel);
    if (!deliveryKey) return;
    
    const pending = this.pendingMessages.get(deliveryKey);
    if (!pending) return;
    
    const header = AmqpDecoder.parseContentHeader(frame.payload);
    pending.header = header;
    pending.expectedSize = header.bodySize;
    
    if (header.bodySize === 0n) {
      this.deliverMessage(deliveryKey, pending, frame.channel);
    }
  }

  private handleBodyFrame(frame: AmqpFrame): void {
    const deliveryKey = this.channelDeliveryKeys.get(frame.channel);
    if (!deliveryKey) return;
    
    const pending = this.pendingMessages.get(deliveryKey);
    if (!pending) return;
    
    pending.bodyChunks.push(frame.payload);
    
    const receivedSize = pending.bodyChunks.reduce((sum, chunk) => sum + BigInt(chunk.length), 0n);
    if (receivedSize >= pending.expectedSize) {
      this.deliverMessage(deliveryKey, pending, frame.channel);
    }
  }

  private deliverMessage(deliveryKey: string, pending: PendingMessage, channel: number): void {
    this.pendingMessages.delete(deliveryKey);
    this.channelDeliveryKeys.delete(channel);
    
    const content = Buffer.concat(pending.bodyChunks);
    const message: DeliveredMessage = {
      ...pending.deliveryInfo,
      properties: pending.header?.properties || {},
      content,
    };
    
    const handler = this.consumers.get(pending.deliveryInfo.consumerTag);
    if (handler) {
      try {
        handler(message);
      } catch (err) {
        console.error('[RmqConnection] Error in message handler:', err);
      }
    }
  }

  private handleClose(): void {
    this.isConnected = false;
    this.stopHeartbeat();
    
    if (this.isClosed) {
      this.onCloseCallback?.();
      return;
    }
    
    if (this.options.reconnect && !this.isReconnecting) {
      this.attemptReconnect();
    } else {
      this.onCloseCallback?.();
    }
  }

  private async attemptReconnect(): Promise<void> {
    if (this.isReconnecting || this.isClosed) return;
    this.isReconnecting = true;
    
    while (this.reconnectAttempts < this.options.reconnectAttempts && !this.isClosed) {
      this.reconnectAttempts++;
      console.log(`[RmqConnection] Reconnecting... attempt ${this.reconnectAttempts}/${this.options.reconnectAttempts}`);
      
      await new Promise(resolve => setTimeout(resolve, this.options.reconnectDelay));
      
      try {
        this.decoder = new AmqpDecoder();
        this.methodWaiters.clear();
        this.pendingMessages.clear();
        
        await this.connect();
        await this.resubscribeConsumers();
        this.isReconnecting = false;
        this.onReconnectCallback?.();
        return;
      } catch (err) {
        console.error(`[RmqConnection] Reconnect failed:`, err);
      }
    }
    
    this.isReconnecting = false;
    this.onCloseCallback?.();
  }

  private waitForMethod(classId: number, methodId: number, timeout = 30000): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const key = `${classId}:${methodId}`;
      
      const timer = setTimeout(() => {
        this.methodWaiters.delete(key);
        reject(new Error(`Timeout waiting for method ${classId}.${methodId}`));
      }, timeout);
      
      this.methodWaiters.set(key, {
        resolve: (args) => {
          clearTimeout(timer);
          resolve(args);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      });
    });
  }

  private sendFrame(type: number, channel: number, payload: Buffer): void {
    if (!this.socket) throw new Error('Not connected');
    const frame = AmqpEncoder.encodeFrame(type, channel, payload);
    (this.socket as any).write(frame);
  }

  private sendMethod(channel: number, classId: number, methodId: number, args: Buffer): void {
    const payload = AmqpEncoder.encodeMethod(classId, methodId, args);
    this.sendFrame(FRAME_METHOD, channel, payload);
  }

  private async sendConnectionStartOk(): Promise<void> {
    const clientProperties = {
      product: 'Orbit',
      version: '1.0.0',
      platform: 'Bun',
      capabilities: {
        'publisher_confirms': true,
        'consumer_cancel_notify': true,
        'connection.blocked': true,
        'authentication_failure_close': true,
      },
    };
    
    const response = `\x00${this.options.username}\x00${this.options.password}`;
    const args = AmqpEncoder.encodeConnectionStartOk(clientProperties, 'PLAIN', response, 'en_US');
    this.sendMethod(0, CLASS_CONNECTION, CONNECTION_START_OK, args);
  }

  private async sendConnectionTuneOk(): Promise<void> {
    const args = AmqpEncoder.encodeConnectionTuneOk(this.channelMax, this.frameMax, this.heartbeatInterval);
    this.sendMethod(0, CLASS_CONNECTION, CONNECTION_TUNE_OK, args);
  }

  private async sendConnectionOpen(): Promise<void> {
    const args = AmqpEncoder.encodeConnectionOpen(this.options.vhost);
    this.sendMethod(0, CLASS_CONNECTION, CONNECTION_OPEN, args);
  }

  private sendConnectionCloseOk(): void {
    const args = AmqpEncoder.encodeConnectionCloseOk();
    this.sendMethod(0, CLASS_CONNECTION, CONNECTION_CLOSE_OK, args);
  }

  private sendChannelCloseOk(channel: number): void {
    const args = AmqpEncoder.encodeChannelCloseOk();
    this.sendMethod(channel, CLASS_CHANNEL, CHANNEL_CLOSE_OK, args);
  }

  async openChannel(channelNumber: number): Promise<void> {
    const args = AmqpEncoder.encodeChannelOpen();
    this.sendMethod(channelNumber, CLASS_CHANNEL, CHANNEL_OPEN, args);
    await this.waitForMethod(CLASS_CHANNEL, CHANNEL_OPEN_OK);
  }

  async closeChannel(channelNumber: number): Promise<void> {
    const args = AmqpEncoder.encodeChannelClose(200, 'Normal', 0, 0);
    this.sendMethod(channelNumber, CLASS_CHANNEL, CHANNEL_CLOSE, args);
    await this.waitForMethod(CLASS_CHANNEL, CHANNEL_CLOSE_OK);
  }

  async declareQueue(
    channel: number,
    queue: string,
    options: { passive?: boolean; durable?: boolean; exclusive?: boolean; autoDelete?: boolean; args?: Record<string, any> } = {}
  ): Promise<{ queue: string; messageCount: number; consumerCount: number }> {
    const args = AmqpEncoder.encodeQueueDeclare(
      queue,
      options.passive || false,
      options.durable ?? true,
      options.exclusive || false,
      options.autoDelete || false,
      false,
      options.args || {}
    );
    
    this.sendMethod(channel, CLASS_QUEUE, QUEUE_DECLARE, args);
    const result = await this.waitForMethod(CLASS_QUEUE, QUEUE_DECLARE_OK);
    return AmqpDecoder.parseQueueDeclareOk(result);
  }

  async qos(channel: number, prefetchCount: number, global = false): Promise<void> {
    const args = AmqpEncoder.encodeBasicQos(0, prefetchCount, global);
    this.sendMethod(channel, CLASS_BASIC, BASIC_QOS, args);
    await this.waitForMethod(CLASS_BASIC, BASIC_QOS_OK);
  }

  async consume(
    channel: number,
    queue: string,
    handler: MessageHandler,
    options: { consumerTag?: string; noLocal?: boolean; noAck?: boolean; exclusive?: boolean; args?: Record<string, any> } = {}
  ): Promise<string> {
    const consumerTag = options.consumerTag || `orbit-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    const args = AmqpEncoder.encodeBasicConsume(
      queue,
      consumerTag,
      options.noLocal || false,
      options.noAck || false,
      options.exclusive || false,
      false,
      options.args || {}
    );
    
    this.sendMethod(channel, CLASS_BASIC, BASIC_CONSUME, args);
    const result = await this.waitForMethod(CLASS_BASIC, BASIC_CONSUME_OK);
    const consumeOk = AmqpDecoder.parseBasicConsumeOk(result);
    
    this.consumers.set(consumeOk.consumerTag, handler);
    this.consumerRegistrations.set(consumeOk.consumerTag, {
      channel,
      queue,
      handler,
      options: {
        consumerTag: consumeOk.consumerTag,
        noLocal: options.noLocal || false,
        noAck: options.noAck || false,
        exclusive: options.exclusive || false,
        args: options.args || {},
      },
    });
    return consumeOk.consumerTag;
  }

  async cancel(channel: number, consumerTag: string): Promise<void> {
    const args = AmqpEncoder.encodeBasicCancel(consumerTag, false);
    this.sendMethod(channel, CLASS_BASIC, BASIC_CANCEL, args);
    await this.waitForMethod(CLASS_BASIC, BASIC_CANCEL_OK);
    this.consumers.delete(consumerTag);
    this.consumerRegistrations.delete(consumerTag);
  }

  private async resubscribeConsumers(): Promise<void> {
    const registrations = Array.from(this.consumerRegistrations.values());
    this.consumers.clear();
    
    for (const reg of registrations) {
      try {
        await this.openChannel(reg.channel);
        await this.declareQueue(reg.channel, reg.queue, { durable: true });
        
        const args = AmqpEncoder.encodeBasicConsume(
          reg.queue,
          reg.options.consumerTag,
          reg.options.noLocal,
          reg.options.noAck,
          reg.options.exclusive,
          false,
          reg.options.args
        );
        
        this.sendMethod(reg.channel, CLASS_BASIC, BASIC_CONSUME, args);
        await this.waitForMethod(CLASS_BASIC, BASIC_CONSUME_OK);
        this.consumers.set(reg.options.consumerTag, reg.handler);
      } catch (err) {
        console.error(`[RmqConnection] Failed to resubscribe consumer ${reg.options.consumerTag}:`, err);
      }
    }
  }

  publish(
    channel: number,
    exchange: string,
    routingKey: string,
    content: Buffer,
    properties: BasicProperties = {},
    options: { mandatory?: boolean; immediate?: boolean } = {}
  ): void {
    const publishArgs = AmqpEncoder.encodeBasicPublish(
      exchange,
      routingKey,
      options.mandatory || false,
      options.immediate || false
    );
    this.sendMethod(channel, CLASS_BASIC, BASIC_PUBLISH, publishArgs);
    
    const headerPayload = AmqpEncoder.encodeContentHeader(CLASS_BASIC, BigInt(content.length), properties);
    this.sendFrame(FRAME_HEADER, channel, headerPayload);
    
    const maxBodySize = this.frameMax - 8;
    for (let offset = 0; offset < content.length; offset += maxBodySize) {
      const chunk = content.subarray(offset, Math.min(offset + maxBodySize, content.length));
      this.sendFrame(FRAME_BODY, channel, chunk);
    }
  }

  ack(channel: number, deliveryTag: bigint, multiple = false): void {
    const args = AmqpEncoder.encodeBasicAck(deliveryTag, multiple);
    this.sendMethod(channel, CLASS_BASIC, BASIC_ACK, args);
  }

  nack(channel: number, deliveryTag: bigint, multiple = false, requeue = true): void {
    const args = AmqpEncoder.encodeBasicNack(deliveryTag, multiple, requeue);
    this.sendMethod(channel, CLASS_BASIC, BASIC_NACK, args);
  }

  private startHeartbeat(): void {
    if (this.heartbeatInterval <= 0) return;
    
    this.heartbeatTimer = setInterval(() => {
      if (Date.now() - this.lastHeartbeat > this.heartbeatInterval * 2000) {
        console.warn('[RmqConnection] Heartbeat timeout, closing connection');
        (this.socket as any)?.end();
        return;
      }
      
      const heartbeat = AmqpEncoder.encodeFrame(FRAME_HEARTBEAT, 0, Buffer.alloc(0));
      (this.socket as any)?.write(heartbeat);
    }, this.heartbeatInterval * 1000);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  async close(): Promise<void> {
    this.isClosed = true;
    this.stopHeartbeat();
    
    if (this.isConnected && this.socket) {
      try {
        const args = AmqpEncoder.encodeConnectionClose(200, 'Normal shutdown', 0, 0);
        this.sendMethod(0, CLASS_CONNECTION, CONNECTION_CLOSE, args);
        await this.waitForMethod(CLASS_CONNECTION, CONNECTION_CLOSE_OK).catch(() => {});
      } catch {}
    }
    
    this.consumers.clear();
    this.methodWaiters.clear();
    this.pendingMessages.clear();
    
    (this.socket as any)?.end();
    this.socket = null;
    this.isConnected = false;
  }

  get connected(): boolean {
    return this.isConnected;
  }
}
