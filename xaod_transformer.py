#!/usr/bin/env python
from __future__ import division
import ROOT
import json

from servicex.transformer.transformer_argument_parser import TransformerArgumentParser
from servicex.transformer.kafka_messaging import KafkaMessaging
from servicex.transformer.object_store_manager import ObjectStoreManager
from servicex.transformer.xaod_events import XAODEvents
from servicex.transformer.xaod_transformer import XAODTransformer
from servicex.transformer.arrow_writer import ArrowWriter


# How many bytes does an average awkward array cell take up. This is just
# a rule of thumb to calculate chunksize
from servicex.transformer.rabbit_mq_manager import RabbitMQManager

avg_cell_size = 42

messaging = None


# Use a heuristic to guess at an optimum message chunk to fill the
# max_message_size
def _compute_chunk_size(attr_list, max_message_size):
    print("Chunks comp", max_message_size * 1e6, len(attr_list), avg_cell_size)
    return int(max_message_size * 1e6 / len(attr_list) / avg_cell_size)


# noinspection PyUnusedLocal
def callback(channel, method, properties, body):
    transform_request = json.loads(body)
    _request_id = transform_request['request-id']
    _file_path = transform_request['file-path']
    _file_id = transform_request['file-id']
    _server_endpoint = transform_request['service-endpoint']
    columns = list(map(lambda b: b.strip(),
                       transform_request['columns'].split(",")))

    arrow_writer = ArrowWriter(file_format=args.result_format,
                               server_endpoint=_server_endpoint,
                               object_store=object_store, messaging=messaging)

    print(_file_path)
    try:
        event_iterator = XAODEvents(file_path=_file_path, attr_name_list=columns,
                                    chunk_size=chunk_size)
        transformer = XAODTransformer(event_iterator)

        arrow_writer.write_branches_to_arrow(transformer=transformer, topic_name=_request_id,
                                             file_id=_file_id,
                                             request_id=args.request_id)

    except Exception as error:
        transform_request['error'] = str(error)
        channel.basic_publish(exchange='transformation_failures',
                              routing_key=_request_id + '_errors',
                              body=json.dumps(transform_request))
        arrow_writer.put_file_complete(file_path=_file_path, file_id=_file_id,
                                       status='failure', num_messages=0, total_time=0,
                                       total_events=0, total_bytes=0)
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def transform_single_file(file_path, tree, attr_list, chunk_size):
    print("Transforming a single path: " + str(args.path))

    arrow_writer = ArrowWriter(file_format=args.result_format,
                               server_endpoint=None,
                               object_store=object_store, messaging=messaging)

    event_iterator = XAODEvents(file_path=file_path,
                                attr_name_list=attr_list, chunk_size=chunk_size)
    transformer = XAODTransformer(event_iterator)

    arrow_writer.write_branches_to_arrow(transformer=transformer,
                                         topic_name=args.topic,
                                         file_id=None,
                                         request_id=args.request_id)


if __name__ == "__main__":
    ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')

    parser = TransformerArgumentParser(description="xAOD Transformer")
    args = parser.parse_args()

    kafka_brokers = TransformerArgumentParser.extract_kafka_brokers(args.brokerlist)

    if args.result_destination == 'kafka':
        messaging = KafkaMessaging(kafka_brokers, args.max_message_size)
        object_store = None
    elif args.result_destination == 'object-store':
        messaging = None
        object_store = ObjectStoreManager()

    _attr_list = TransformerArgumentParser.extract_attr_list(args.attr_names)

    if args.chunks:
        chunk_size = args.chunks
    else:
        chunk_size = _compute_chunk_size(_attr_list, args.max_message_size)

    if args.request_id and not args.path:
        rabbitmq = RabbitMQManager(args.rabbit_uri, args.request_id, callback)

    if args.path:
        transform_single_file(args.path, args.tree, _attr_list, chunk_size)
