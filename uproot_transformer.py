# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
from __future__ import division

import json

from servicex.transformer.transformer_argument_parser import TransformerArgumentParser
from servicex.transformer.kafka_messaging import KafkaMessaging
from servicex.transformer.nanoaod_transformer import NanoAODTransformer
from servicex.transformer.object_store_manager import ObjectStoreManager
from servicex.transformer.nanoaod_events import NanoAODEvents
from servicex.transformer.arrow_writer import ArrowWriter
from servicex.transformer.servicex_adapter import ServiceXAdapter


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
    _tree_name = transform_request['tree-name']
    _file_path = transform_request['file-path']
    _file_id = transform_request['file-id']
    _server_endpoint = transform_request['service-endpoint']
    columns = list(map(lambda b: b.strip(),
                       transform_request['columns'].split(",")))

    servicex = ServiceXAdapter(_server_endpoint)

    arrow_writer = ArrowWriter(file_format=args.result_format,
                               servicex=servicex,
                               object_store=object_store, messaging=messaging)

    print(_file_path)
    try:
        event_iterator = NanoAODEvents(_file_path, _tree_name, columns, chunk_size)
        transformer = NanoAODTransformer(event_iterator)

        arrow_writer.write_branches_to_arrow(transformer=transformer, topic_name=_request_id,
                                             file_id=_file_id,
                                             request_id=args.request_id)

    except Exception as error:
        transform_request['error'] = str(error)
        channel.basic_publish(exchange='transformation_failures',
                              routing_key=_request_id + '_errors',
                              body=json.dumps(transform_request))
        servicex.put_file_complete(file_path=_file_path, file_id=_file_id,
                                   status='failure', num_messages=0, total_time=0,
                                   total_events=0, total_bytes=0)
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def transform_single_file(file_path, tree, attr_list, chunk_size):
    print("Transforming a single path: " + str(args.path))

    arrow_writer = ArrowWriter(file_format=args.result_format,
                               servicex=None,
                               object_store=object_store, messaging=messaging)

    event_iterator = NanoAODEvents(file_path=file_path, tree_name=tree,
                                   attr_name_list=attr_list, chunk_size=chunk_size)
    transformer = NanoAODTransformer(event_iterator)

    arrow_writer.write_branches_to_arrow(transformer=transformer,
                                         topic_name=args.topic,
                                         file_id=None,
                                         request_id=args.request_id)


if __name__ == "__main__":
    parser = TransformerArgumentParser(description="Flat N-Tuple Transformer")
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
