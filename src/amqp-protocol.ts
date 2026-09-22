export const AMQP_PROTOCOL_HEADER = Buffer.from([0x41, 0x4d, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01]);

export const FRAME_METHOD = 1;
export const FRAME_HEADER = 2;
export const FRAME_BODY = 3;
export const FRAME_HEARTBEAT = 8;
export const FRAME_END = 0xce;

export const CLASS_CONNECTION = 10;
export const CLASS_CHANNEL = 20;
export const CLASS_EXCHANGE = 40;
export const CLASS_QUEUE = 50;
export const CLASS_BASIC = 60;
export const CLASS_TX = 90;

export const CONNECTION_START = 10;
export const CONNECTION_START_OK = 11;
export const CONNECTION_TUNE = 30;
export const CONNECTION_TUNE_OK = 31;
export const CONNECTION_OPEN = 40;
export const CONNECTION_OPEN_OK = 41;
export const CONNECTION_CLOSE = 50;
export const CONNECTION_CLOSE_OK = 51;

export const CHANNEL_OPEN = 10;
export const CHANNEL_OPEN_OK = 11;
export const CHANNEL_CLOSE = 40;
export const CHANNEL_CLOSE_OK = 41;

export const QUEUE_DECLARE = 10;
export const QUEUE_DECLARE_OK = 11;
export const QUEUE_BIND = 20;
export const QUEUE_BIND_OK = 21;
export const QUEUE_DELETE = 40;
export const QUEUE_DELETE_OK = 41;

export const BASIC_QOS = 10;
export const BASIC_QOS_OK = 11;
export const BASIC_CONSUME = 20;
export const BASIC_CONSUME_OK = 21;
export const BASIC_CANCEL = 30;
export const BASIC_CANCEL_OK = 31;
export const BASIC_PUBLISH = 40;
export const BASIC_DELIVER = 60;
export const BASIC_ACK = 80;
export const BASIC_NACK = 120;

export interface AmqpFrame {
  type: number;
  channel: number;
  payload: Buffer;
}

export interface AmqpMethod {
  classId: number;
  methodId: number;
  args: Buffer;
}

export interface ContentHeader {
  classId: number;
  weight: number;
  bodySize: bigint;
  propertyFlags: number;
  properties: BasicProperties;
}

export interface BasicProperties {
  contentType?: string;
  contentEncoding?: string;
  headers?: Record<string, any>;
  deliveryMode?: number;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
  expiration?: string;
  messageId?: string;
  timestamp?: number;
  type?: string;
  userId?: string;
  appId?: string;
  clusterId?: string;
}

export interface DeliveredMessage {
  consumerTag: string;
  deliveryTag: bigint;
  redelivered: boolean;
  exchange: string;
  routingKey: string;
  properties: BasicProperties;
  content: Buffer;
}

export class AmqpEncoder {
  static encodeFrame(type: number, channel: number, payload: Buffer): Buffer {
    const frame = Buffer.alloc(7 + payload.length + 1);
    frame.writeUInt8(type, 0);
    frame.writeUInt16BE(channel, 1);
    frame.writeUInt32BE(payload.length, 3);
    payload.copy(frame, 7);
    frame.writeUInt8(FRAME_END, 7 + payload.length);
    return frame;
  }

  static encodeMethod(classId: number, methodId: number, args: Buffer): Buffer {
    const payload = Buffer.alloc(4 + args.length);
    payload.writeUInt16BE(classId, 0);
    payload.writeUInt16BE(methodId, 2);
    args.copy(payload, 4);
    return payload;
  }

  static encodeShortString(str: string): Buffer {
    const strBuf = Buffer.from(str, 'utf8');
    const buf = Buffer.alloc(1 + strBuf.length);
    buf.writeUInt8(strBuf.length, 0);
    strBuf.copy(buf, 1);
    return buf;
  }

  static encodeLongString(str: string): Buffer {
    const strBuf = Buffer.from(str, 'utf8');
    const buf = Buffer.alloc(4 + strBuf.length);
    buf.writeUInt32BE(strBuf.length, 0);
    strBuf.copy(buf, 4);
    return buf;
  }

  static encodeTable(table: Record<string, any>): Buffer {
    const entries: Buffer[] = [];
    
    for (const [key, value] of Object.entries(table)) {
      const keyBuf = this.encodeShortString(key);
      const valueBuf = this.encodeFieldValue(value);
      entries.push(Buffer.concat([keyBuf, valueBuf]));
    }
    
    const tableContent = Buffer.concat(entries);
    const buf = Buffer.alloc(4 + tableContent.length);
    buf.writeUInt32BE(tableContent.length, 0);
    tableContent.copy(buf, 4);
    return buf;
  }

  static encodeFieldValue(value: any): Buffer {
    if (typeof value === 'string') {
      const strBuf = this.encodeLongString(value);
      return Buffer.concat([Buffer.from('S'), strBuf]);
    }
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        const buf = Buffer.alloc(5);
        buf.writeUInt8(0x49, 0); // 'I' for signed 32-bit
        buf.writeInt32BE(value, 1);
        return buf;
      }
      const buf = Buffer.alloc(9);
      buf.writeUInt8(0x64, 0); // 'd' for double
      buf.writeDoubleBE(value, 1);
      return buf;
    }
    if (typeof value === 'boolean') {
      return Buffer.from([0x74, value ? 1 : 0]); // 't' for boolean
    }
    if (value === null || value === undefined) {
      return Buffer.from([0x56]); // 'V' for void
    }
    if (typeof value === 'object' && !Array.isArray(value)) {
      const tableBuf = this.encodeTable(value);
      return Buffer.concat([Buffer.from('F'), tableBuf]);
    }
    return Buffer.from([0x56]);
  }

  static encodeConnectionStartOk(
    clientProperties: Record<string, any>,
    mechanism: string,
    response: string,
    locale: string
  ): Buffer {
    const props = this.encodeTable(clientProperties);
    const mech = this.encodeShortString(mechanism);
    const resp = this.encodeLongString(response);
    const loc = this.encodeShortString(locale);
    return Buffer.concat([props, mech, resp, loc]);
  }

  static encodeConnectionTuneOk(channelMax: number, frameMax: number, heartbeat: number): Buffer {
    const buf = Buffer.alloc(8);
    buf.writeUInt16BE(channelMax, 0);
    buf.writeUInt32BE(frameMax, 2);
    buf.writeUInt16BE(heartbeat, 6);
    return buf;
  }

  static encodeConnectionOpen(vhost: string): Buffer {
    const vhostBuf = this.encodeShortString(vhost);
    return Buffer.concat([vhostBuf, Buffer.from([0, 0])]);
  }

  static encodeChannelOpen(): Buffer {
    return this.encodeShortString('');
  }

  static encodeQueueDeclare(
    queue: string,
    passive: boolean,
    durable: boolean,
    exclusive: boolean,
    autoDelete: boolean,
    noWait: boolean,
    args: Record<string, any>
  ): Buffer {
    const queueBuf = this.encodeShortString(queue);
    const flags = (passive ? 1 : 0) | (durable ? 2 : 0) | (exclusive ? 4 : 0) | (autoDelete ? 8 : 0) | (noWait ? 16 : 0);
    const argsBuf = this.encodeTable(args);
    const buf = Buffer.alloc(2 + queueBuf.length + 1 + argsBuf.length);
    buf.writeUInt16BE(0, 0); // reserved
    queueBuf.copy(buf, 2);
    buf.writeUInt8(flags, 2 + queueBuf.length);
    argsBuf.copy(buf, 2 + queueBuf.length + 1);
    return buf;
  }

  static encodeBasicQos(prefetchSize: number, prefetchCount: number, global: boolean): Buffer {
    const buf = Buffer.alloc(7);
    buf.writeUInt32BE(prefetchSize, 0);
    buf.writeUInt16BE(prefetchCount, 4);
    buf.writeUInt8(global ? 1 : 0, 6);
    return buf;
  }

  static encodeBasicConsume(
    queue: string,
    consumerTag: string,
    noLocal: boolean,
    noAck: boolean,
    exclusive: boolean,
    noWait: boolean,
    args: Record<string, any>
  ): Buffer {
    const queueBuf = this.encodeShortString(queue);
    const tagBuf = this.encodeShortString(consumerTag);
    const flags = (noLocal ? 1 : 0) | (noAck ? 2 : 0) | (exclusive ? 4 : 0) | (noWait ? 8 : 0);
    const argsBuf = this.encodeTable(args);
    
    const parts = [
      Buffer.alloc(2), // reserved
      queueBuf,
      tagBuf,
      Buffer.from([flags]),
      argsBuf,
    ];
    return Buffer.concat(parts);
  }

  static encodeBasicPublish(exchange: string, routingKey: string, mandatory: boolean, immediate: boolean): Buffer {
    const exchangeBuf = this.encodeShortString(exchange);
    const routingKeyBuf = this.encodeShortString(routingKey);
    const flags = (mandatory ? 1 : 0) | (immediate ? 2 : 0);
    
    const parts = [
      Buffer.alloc(2), // reserved
      exchangeBuf,
      routingKeyBuf,
      Buffer.from([flags]),
    ];
    return Buffer.concat(parts);
  }

  static encodeBasicAck(deliveryTag: bigint, multiple: boolean): Buffer {
    const buf = Buffer.alloc(9);
    buf.writeBigUInt64BE(deliveryTag, 0);
    buf.writeUInt8(multiple ? 1 : 0, 8);
    return buf;
  }

  static encodeBasicNack(deliveryTag: bigint, multiple: boolean, requeue: boolean): Buffer {
    const buf = Buffer.alloc(9);
    buf.writeBigUInt64BE(deliveryTag, 0);
    buf.writeUInt8((multiple ? 1 : 0) | (requeue ? 2 : 0), 8);
    return buf;
  }

  static encodeBasicCancel(consumerTag: string, noWait: boolean): Buffer {
    const tagBuf = this.encodeShortString(consumerTag);
    return Buffer.concat([tagBuf, Buffer.from([noWait ? 1 : 0])]);
  }

  static encodeContentHeader(classId: number, bodySize: bigint, properties: BasicProperties): Buffer {
    let propertyFlags = 0;
    const propParts: Buffer[] = [];

    if (properties.contentType) {
      propertyFlags |= 0x8000;
      propParts.push(this.encodeShortString(properties.contentType));
    }
    if (properties.contentEncoding) {
      propertyFlags |= 0x4000;
      propParts.push(this.encodeShortString(properties.contentEncoding));
    }
    if (properties.headers) {
      propertyFlags |= 0x2000;
      propParts.push(this.encodeTable(properties.headers));
    }
    if (properties.deliveryMode !== undefined) {
      propertyFlags |= 0x1000;
      propParts.push(Buffer.from([properties.deliveryMode]));
    }
    if (properties.priority !== undefined) {
      propertyFlags |= 0x0800;
      propParts.push(Buffer.from([properties.priority]));
    }
    if (properties.correlationId) {
      propertyFlags |= 0x0400;
      propParts.push(this.encodeShortString(properties.correlationId));
    }
    if (properties.replyTo) {
      propertyFlags |= 0x0200;
      propParts.push(this.encodeShortString(properties.replyTo));
    }
    if (properties.expiration) {
      propertyFlags |= 0x0100;
      propParts.push(this.encodeShortString(properties.expiration));
    }
    if (properties.messageId) {
      propertyFlags |= 0x0080;
      propParts.push(this.encodeShortString(properties.messageId));
    }
    if (properties.timestamp !== undefined) {
      propertyFlags |= 0x0040;
      const tsBuf = Buffer.alloc(8);
      tsBuf.writeBigUInt64BE(BigInt(properties.timestamp), 0);
      propParts.push(tsBuf);
    }
    if (properties.type) {
      propertyFlags |= 0x0020;
      propParts.push(this.encodeShortString(properties.type));
    }
    if (properties.userId) {
      propertyFlags |= 0x0010;
      propParts.push(this.encodeShortString(properties.userId));
    }
    if (properties.appId) {
      propertyFlags |= 0x0008;
      propParts.push(this.encodeShortString(properties.appId));
    }

    const header = Buffer.alloc(12);
    header.writeUInt16BE(classId, 0);
    header.writeUInt16BE(0, 2); // weight
    header.writeBigUInt64BE(bodySize, 4);
    
    const flagsBuf = Buffer.alloc(2);
    flagsBuf.writeUInt16BE(propertyFlags, 0);
    
    return Buffer.concat([header, flagsBuf, ...propParts]);
  }

  static encodeConnectionClose(replyCode: number, replyText: string, classId: number, methodId: number): Buffer {
    const replyTextBuf = this.encodeShortString(replyText);
    const buf = Buffer.alloc(2 + replyTextBuf.length + 4);
    buf.writeUInt16BE(replyCode, 0);
    replyTextBuf.copy(buf, 2);
    buf.writeUInt16BE(classId, 2 + replyTextBuf.length);
    buf.writeUInt16BE(methodId, 4 + replyTextBuf.length);
    return buf;
  }

  static encodeConnectionCloseOk(): Buffer {
    return Buffer.alloc(0);
  }

  static encodeChannelClose(replyCode: number, replyText: string, classId: number, methodId: number): Buffer {
    return this.encodeConnectionClose(replyCode, replyText, classId, methodId);
  }

  static encodeChannelCloseOk(): Buffer {
    return Buffer.alloc(0);
  }
}

export class AmqpDecoder {
  private buffer: Buffer = Buffer.alloc(0);

  append(data: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, data]);
  }

  *parseFrames(): Generator<AmqpFrame> {
    while (this.buffer.length >= 7) {
      const type = this.buffer.readUInt8(0);
      const channel = this.buffer.readUInt16BE(1);
      const size = this.buffer.readUInt32BE(3);
      
      if (this.buffer.length < 8 + size) break;
      
      const frameEnd = this.buffer.readUInt8(7 + size);
      if (frameEnd !== FRAME_END) {
        throw new Error(`Invalid frame end marker: ${frameEnd}`);
      }
      
      const payload = this.buffer.subarray(7, 7 + size);
      this.buffer = this.buffer.subarray(8 + size);
      
      yield { type, channel, payload };
    }
  }

  static parseMethod(payload: Buffer): AmqpMethod {
    const classId = payload.readUInt16BE(0);
    const methodId = payload.readUInt16BE(2);
    const args = payload.subarray(4);
    return { classId, methodId, args };
  }

  static parseShortString(buf: Buffer, offset: number): { value: string; length: number } {
    const len = buf.readUInt8(offset);
    const value = buf.toString('utf8', offset + 1, offset + 1 + len);
    return { value, length: 1 + len };
  }

  static parseLongString(buf: Buffer, offset: number): { value: string; length: number } {
    const len = buf.readUInt32BE(offset);
    const value = buf.toString('utf8', offset + 4, offset + 4 + len);
    return { value, length: 4 + len };
  }

  static parseTable(buf: Buffer, offset: number): { value: Record<string, any>; length: number } {
    const tableLen = buf.readUInt32BE(offset);
    const table: Record<string, any> = {};
    let pos = offset + 4;
    const endPos = offset + 4 + tableLen;
    
    while (pos < endPos) {
      const key = this.parseShortString(buf, pos);
      pos += key.length;
      
      const fieldValue = this.parseFieldValue(buf, pos);
      pos += fieldValue.length;
      
      table[key.value] = fieldValue.value;
    }
    
    return { value: table, length: 4 + tableLen };
  }

  static parseFieldValue(buf: Buffer, offset: number): { value: any; length: number } {
    const type = buf.readUInt8(offset);
    offset++;
    
    switch (type) {
      case 0x74: // t - boolean
        return { value: buf.readUInt8(offset) !== 0, length: 2 };
      case 0x62: // b - signed 8-bit
        return { value: buf.readInt8(offset), length: 2 };
      case 0x42: // B - unsigned 8-bit
        return { value: buf.readUInt8(offset), length: 2 };
      case 0x73: // s - signed 16-bit
        return { value: buf.readInt16BE(offset), length: 3 };
      case 0x75: // u - unsigned 16-bit
        return { value: buf.readUInt16BE(offset), length: 3 };
      case 0x49: // I - signed 32-bit
        return { value: buf.readInt32BE(offset), length: 5 };
      case 0x69: // i - unsigned 32-bit
        return { value: buf.readUInt32BE(offset), length: 5 };
      case 0x6c: // l - signed 64-bit
        return { value: buf.readBigInt64BE(offset), length: 9 };
      case 0x66: // f - 32-bit float
        return { value: buf.readFloatBE(offset), length: 5 };
      case 0x64: // d - 64-bit double
        return { value: buf.readDoubleBE(offset), length: 9 };
      case 0x53: { // S - long string
        const str = this.parseLongString(buf, offset);
        return { value: str.value, length: 1 + str.length };
      }
      case 0x46: { // F - nested table
        const table = this.parseTable(buf, offset);
        return { value: table.value, length: 1 + table.length };
      }
      case 0x56: // V - void/null
        return { value: null, length: 1 };
      default:
        return { value: null, length: 1 };
    }
  }

  static parseConnectionStart(args: Buffer): {
    versionMajor: number;
    versionMinor: number;
    serverProperties: Record<string, any>;
    mechanisms: string;
    locales: string;
  } {
    let offset = 0;
    const versionMajor = args.readUInt8(offset++);
    const versionMinor = args.readUInt8(offset++);
    
    const serverProps = this.parseTable(args, offset);
    offset += serverProps.length;
    
    const mechanisms = this.parseLongString(args, offset);
    offset += mechanisms.length;
    
    const locales = this.parseLongString(args, offset);
    
    return {
      versionMajor,
      versionMinor,
      serverProperties: serverProps.value,
      mechanisms: mechanisms.value,
      locales: locales.value,
    };
  }

  static parseConnectionTune(args: Buffer): {
    channelMax: number;
    frameMax: number;
    heartbeat: number;
  } {
    return {
      channelMax: args.readUInt16BE(0),
      frameMax: args.readUInt32BE(2),
      heartbeat: args.readUInt16BE(6),
    };
  }

  static parseQueueDeclareOk(args: Buffer): {
    queue: string;
    messageCount: number;
    consumerCount: number;
  } {
    let offset = 0;
    const queue = this.parseShortString(args, offset);
    offset += queue.length;
    const messageCount = args.readUInt32BE(offset);
    offset += 4;
    const consumerCount = args.readUInt32BE(offset);
    
    return {
      queue: queue.value,
      messageCount,
      consumerCount,
    };
  }

  static parseBasicConsumeOk(args: Buffer): { consumerTag: string } {
    const tag = this.parseShortString(args, 0);
    return { consumerTag: tag.value };
  }

  static parseBasicDeliver(args: Buffer): {
    consumerTag: string;
    deliveryTag: bigint;
    redelivered: boolean;
    exchange: string;
    routingKey: string;
  } {
    let offset = 0;
    
    const consumerTag = this.parseShortString(args, offset);
    offset += consumerTag.length;
    
    const deliveryTag = args.readBigUInt64BE(offset);
    offset += 8;
    
    const redelivered = args.readUInt8(offset) !== 0;
    offset += 1;
    
    const exchange = this.parseShortString(args, offset);
    offset += exchange.length;
    
    const routingKey = this.parseShortString(args, offset);
    
    return {
      consumerTag: consumerTag.value,
      deliveryTag,
      redelivered,
      exchange: exchange.value,
      routingKey: routingKey.value,
    };
  }

  static parseContentHeader(payload: Buffer): ContentHeader {
    const classId = payload.readUInt16BE(0);
    const weight = payload.readUInt16BE(2);
    const bodySize = payload.readBigUInt64BE(4);
    const propertyFlags = payload.readUInt16BE(12);
    
    let offset = 14;
    const properties: BasicProperties = {};
    
    if (propertyFlags & 0x8000) {
      const ct = this.parseShortString(payload, offset);
      properties.contentType = ct.value;
      offset += ct.length;
    }
    if (propertyFlags & 0x4000) {
      const ce = this.parseShortString(payload, offset);
      properties.contentEncoding = ce.value;
      offset += ce.length;
    }
    if (propertyFlags & 0x2000) {
      const headers = this.parseTable(payload, offset);
      properties.headers = headers.value;
      offset += headers.length;
    }
    if (propertyFlags & 0x1000) {
      properties.deliveryMode = payload.readUInt8(offset++);
    }
    if (propertyFlags & 0x0800) {
      properties.priority = payload.readUInt8(offset++);
    }
    if (propertyFlags & 0x0400) {
      const cid = this.parseShortString(payload, offset);
      properties.correlationId = cid.value;
      offset += cid.length;
    }
    if (propertyFlags & 0x0200) {
      const rt = this.parseShortString(payload, offset);
      properties.replyTo = rt.value;
      offset += rt.length;
    }
    if (propertyFlags & 0x0100) {
      const exp = this.parseShortString(payload, offset);
      properties.expiration = exp.value;
      offset += exp.length;
    }
    if (propertyFlags & 0x0080) {
      const mid = this.parseShortString(payload, offset);
      properties.messageId = mid.value;
      offset += mid.length;
    }
    if (propertyFlags & 0x0040) {
      properties.timestamp = Number(payload.readBigUInt64BE(offset));
      offset += 8;
    }
    if (propertyFlags & 0x0020) {
      const t = this.parseShortString(payload, offset);
      properties.type = t.value;
      offset += t.length;
    }
    if (propertyFlags & 0x0010) {
      const uid = this.parseShortString(payload, offset);
      properties.userId = uid.value;
      offset += uid.length;
    }
    if (propertyFlags & 0x0008) {
      const aid = this.parseShortString(payload, offset);
      properties.appId = aid.value;
      offset += aid.length;
    }
    
    return { classId, weight, bodySize, propertyFlags, properties };
  }

  static parseConnectionClose(args: Buffer): {
    replyCode: number;
    replyText: string;
    classId: number;
    methodId: number;
  } {
    let offset = 0;
    const replyCode = args.readUInt16BE(offset);
    offset += 2;
    
    const replyText = this.parseShortString(args, offset);
    offset += replyText.length;
    
    const classId = args.readUInt16BE(offset);
    offset += 2;
    
    const methodId = args.readUInt16BE(offset);
    
    return { replyCode, replyText: replyText.value, classId, methodId };
  }
}
