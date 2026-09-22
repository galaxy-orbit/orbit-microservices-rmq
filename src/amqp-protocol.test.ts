import { describe, test, expect } from 'bun:test';
import {
  AmqpEncoder, AmqpDecoder, AMQP_PROTOCOL_HEADER,
  FRAME_METHOD, FRAME_HEARTBEAT, FRAME_END,
  CLASS_CONNECTION, CONNECTION_START, BASIC_PUBLISH,
} from './amqp-protocol';

describe('AmqpEncoder', () => {
  test('encodeFrame: type + channel + size + payload + end marker', () => {
    const payload = Buffer.from([1, 2, 3]);
    const frame = AmqpEncoder.encodeFrame(FRAME_METHOD, 1, payload);
    expect(frame[0]).toBe(FRAME_METHOD);
    expect(frame.readUInt16BE(1)).toBe(1);
    expect(frame.readUInt32BE(3)).toBe(3);
    expect(frame.subarray(7, 10)).toEqual(payload);
    expect(frame[frame.length - 1]).toBe(FRAME_END);
  });

  test('encodeMethod prefixes classId/methodId', () => {
    const args = Buffer.from([0x01]);
    const payload = AmqpEncoder.encodeMethod(CLASS_CONNECTION, CONNECTION_START, args);
    expect(payload.readUInt16BE(0)).toBe(CLASS_CONNECTION);
    expect(payload.readUInt16BE(2)).toBe(CONNECTION_START);
    expect(payload.subarray(4)).toEqual(args);
  });

  test('encodeShortString / encodeLongString', () => {
    const short = AmqpEncoder.encodeShortString('abc');
    expect(short[0]).toBe(3);
    expect(short.toString('utf8', 1)).toBe('abc');

    const long = AmqpEncoder.encodeLongString('orbit');
    expect(long.readUInt32BE(0)).toBe(5);
    expect(long.toString('utf8', 4)).toBe('orbit');
  });

  test('encodeTable round-trips via AmqpDecoder.parseTable', () => {
    const table = { durable: true, prefetch: 10, exchange: 'events', ratio: 1.5, meta: { nested: 'yes' } };
    const encoded = AmqpEncoder.encodeTable(table);
    const parsed = AmqpDecoder.parseTable(encoded, 0);
    expect(parsed.value).toEqual(table);
    expect(parsed.length).toBe(encoded.length);
  });

  test('encodeFieldValue types', () => {
    const boolField = AmqpEncoder.encodeFieldValue(true);
    expect(boolField[0]).toBe(0x74);
    expect(boolField[1]).toBe(1);

    const voidField = AmqpEncoder.encodeFieldValue(null);
    expect(voidField[0]).toBe(0x56);
  });
});

describe('AmqpDecoder frames', () => {
  test('parseFrames yields frames fed in one batch', () => {
    const decoder = new AmqpDecoder();
    const f1 = AmqpEncoder.encodeFrame(FRAME_METHOD, 1, AmqpEncoder.encodeMethod(CLASS_CONNECTION, CONNECTION_START, Buffer.alloc(0)));
    const f2 = AmqpEncoder.encodeFrame(FRAME_HEARTBEAT, 0, Buffer.alloc(0));
    decoder.append(Buffer.concat([f1, f2]));
    const frames = [...decoder.parseFrames()];
    expect(frames).toHaveLength(2);
    expect(frames[0].type).toBe(FRAME_METHOD);
    expect(frames[1].type).toBe(FRAME_HEARTBEAT);
  });

  test('partial frame waits for more data', () => {
    const decoder = new AmqpDecoder();
    const frame = AmqpEncoder.encodeFrame(FRAME_METHOD, 1, Buffer.alloc(4));
    decoder.append(frame.subarray(0, 5));
    expect([...decoder.parseFrames()]).toHaveLength(0);
    decoder.append(frame.subarray(5));
    const frames = [...decoder.parseFrames()];
    expect(frames).toHaveLength(1);
  });

  test('invalid frame end marker throws', () => {
    const decoder = new AmqpDecoder();
    const frame = AmqpEncoder.encodeFrame(FRAME_METHOD, 1, Buffer.alloc(2));
    frame[frame.length - 1] = 0x00; // corrupt end marker
    decoder.append(frame);
    expect(() => [...decoder.parseFrames()]).toThrow(/Invalid frame end marker/);
  });

  test('protocol header constant matches AMQP 0-9-1', () => {
    expect(AMQP_PROTOCOL_HEADER.toString()).toBe('AMQP\x00\x00\x09\x01');
  });
});

describe('BASIC_PUBLISH frame round-trip', () => {
  test('method frame parses class/method ids', () => {
    const args = Buffer.concat([AmqpEncoder.encodeShortString('ex'), AmqpEncoder.encodeShortString('rk')]);
    const frame = AmqpEncoder.encodeFrame(FRAME_METHOD, 1, AmqpEncoder.encodeMethod(CLASS_CONNECTION, BASIC_PUBLISH, args));
    const decoder = new AmqpDecoder();
    decoder.append(frame);
    const parsed = [...decoder.parseFrames()][0];
    expect(parsed.type).toBe(FRAME_METHOD);
    const method = AmqpDecoder.parseMethod(parsed.payload);
    expect(method.classId).toBe(CLASS_CONNECTION);
    expect(method.methodId).toBe(BASIC_PUBLISH);
  });
});
