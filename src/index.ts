import { registerTransport, Transport } from '@galaxy-stack/orbit-microservices';
import { RmqServer, type RmqServerOptions } from './rmq-server';
import { RmqClient, type RmqClientOptions } from './rmq-client';

registerTransport(Transport.RMQ, RmqServer, RmqClient);

export { RmqServer, type RmqServerOptions } from './rmq-server';
export { RmqClient, type RmqClientOptions } from './rmq-client';
export { RmqConnection, type RmqConnectionOptions } from './rmq-connection';
export {
  AmqpEncoder,
  AmqpDecoder,
  type AmqpFrame,
  type AmqpMethod,
  type BasicProperties,
  type ContentHeader,
  type DeliveredMessage,
} from './amqp-protocol';
