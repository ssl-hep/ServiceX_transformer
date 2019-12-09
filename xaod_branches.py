#!/usr/bin/env python
from __future__ import division

import datetime
import json
import os

import time
import pika
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from servicex.transformer.TransformerArgumentParser import TransformerArgumentParser
from servicex.transformer.kafka_messaging import KafkaMessaging
from servicex.transformer.nanoaod_transformer import NanoAODTransformer
from servicex.transformer.object_store_manager import ObjectStoreManager
from servicex.transformer.nanoaod_events import NanoAODEvents


# How many bytes does an average awkward array cell take up. This is just
# a rule of thumb to calculate chunksize
avg_cell_size = 42

messaging = None


# Use a heuristic to guess at an optimum message chunk to fill the
# max_message_size
def _compute_chunk_size(attr_list, max_message_size):
    print("Chunks comp", max_message_size * 1e6, len(attr_list), avg_cell_size)
    return int(max_message_size * 1e6 / len(attr_list) / avg_cell_size)


def _open_scratch_file(file_format, pa_table):
    if file_format == 'parquet':
        return pq.ParquetWriter("/tmp/out", pa_table.schema)


def _append_table_to_scratch(file_format, scratch_writer, pa_table):
    if file_format == 'parquet':
        scratch_writer.write_table(table=pa_table)


def _close_scratch_file(file_format, scratch_writer):
    if file_format == 'parquet':
        scratch_writer.close()


def post_status_update(endpoint, status_msg):
    requests.post(endpoint+"/status", data={
        "timestamp": datetime.datetime.now().isoformat(),
        "status": status_msg
    })


def put_file_complete(endpoint, file_path, file_id, status,
                      num_messages=None, total_time=None, total_events=None,
                      total_bytes=None):
    avg_rate = 0 if not total_time else total_events/total_time
    doc = {
        "file-path": file_path,
        "file-id": file_id,
        "status": status,
        "num-messages": num_messages,
        "total-time": total_time,
        "total-events": total_events,
        "total-bytes": total_bytes,
        "avg-rate": avg_rate
    }
    print("------< ", doc)
    if endpoint:
        requests.put(endpoint+"/file-complete", json=doc)


def write_branches_to_arrow(messaging, topic_name, file_path, file_id, tree_name,
                            attr_name_list, chunk_size, server_endpoint, event_limit=None,
                            object_store=None):
    tick = time.time()

    scratch_writer = None

    event_iterator = NanoAODEvents(file_path, tree_name, attr_name_list, chunk_size)
    transformer = NanoAODTransformer(event_iterator)

    batch_number = 0
    total_events = 0
    total_bytes = 0
    for pa_table in transformer.arrow_table(chunk_size, event_limit):
        if object_store:
            if not scratch_writer:
                scratch_writer = _open_scratch_file(args.result_format, pa_table)
            _append_table_to_scratch(args.result_format, scratch_writer, pa_table)

        total_events = total_events + pa_table.num_rows
        batches = pa_table.to_batches(chunksize=chunk_size)

        for batch in batches:
            if messaging:
                key = file_path + "-" + str(batch_number)

                sink = pa.BufferOutputStream()
                writer = pa.RecordBatchStreamWriter(sink, batch.schema)
                writer.write_batch(batch)
                writer.close()
                messaging.publish_message(
                    topic_name,
                    key,
                    sink.getvalue())

                total_bytes = total_bytes + len(sink.getvalue().to_pybytes())

                avg_cell_size = len(sink.getvalue().to_pybytes()) / len(
                    attr_name_list) / batch.num_rows
                print("Batch number " + str(batch_number) + ", "
                      + str(batch.num_rows) +
                      " events published to " + topic_name,
                      "Avg Cell Size = " + str(avg_cell_size) + " bytes")
                batch_number += 1

    if object_store:
        _close_scratch_file(args.result_format, scratch_writer)
        print("Writing parquet to ", args.request_id, " as ", file_path.replace('/', ':'))
        object_store.upload_file(args.request_id, file_path.replace('/', ':'), "/tmp/out")
        os.remove("/tmp/out")

    if server_endpoint:
        post_status_update(server_endpoint, "File " + file_path + " complete")

    tock = time.time()
    print("Real time: " + str(round(tock - tick / 60.0, 2)) + " minutes")
    put_file_complete(server_endpoint, file_path, file_id, "success",
                      num_messages=batch_number, total_time=round(tock - tick / 60.0, 2),
                      total_events=total_events, total_bytes=total_bytes)


def transform_dataset(dataset, messaging, topic_name, servicex_id, attr_list, chunk_size,
                      limit):
    with open(dataset, 'r') as f:
        datasets = json.load(f)
        for rec in datasets:
            print("Transforming ", rec[u'file_path'], rec[u'file_events'])
            write_branches_to_arrow(messaging, topic_name, rec[u'file_path'], servicex_id,
                                    attr_list, chunk_size, None, limit)


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

    print(_file_path)
    try:
        write_branches_to_arrow(messaging=messaging, topic_name=_request_id,
                                file_path=_file_path, file_id=_file_id,
                                attr_name_list=columns, tree_name=_tree_name,
                                chunk_size=chunk_size, server_endpoint=_server_endpoint,
                                object_store=object_store)
    except Exception as error:
        transform_request['error'] = str(error)
        channel.basic_publish(exchange='transformation_failures',
                              routing_key=_request_id + '_errors',
                              body=json.dumps(transform_request))
        put_file_complete(_server_endpoint, _file_path, _file_id,
                          "failure", 0, 0.0)
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


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
        rabbitmq = pika.BlockingConnection(
            pika.URLParameters(args.rabbit_uri)
        )
        _channel = rabbitmq.channel()

        # Set to one since our ops take a long time.
        # Give another client a chance
        _channel.basic_qos(prefetch_count=1)

        _channel.basic_consume(queue=args.request_id,
                               auto_ack=False,
                               on_message_callback=callback)
        _channel.start_consuming()

    print("Atlas xAOD Transformer")
    print(_attr_list)
    print("Chunk size ", chunk_size)

    limit = int(args.limit) if args.limit else None

    if args.path:
        print("Transforming a single path: " + str(args.path))
        write_branches_to_arrow(messaging, args.topic, args.path, "cli", args.tree,
                                _attr_list, chunk_size, None, limit,
                                object_store=object_store)
    elif args.dataset:
        print("Transforming files from saved dataset ", args.dataset)
        transform_dataset(args.dataset, messaging, args.topic, "cli", args.tree,
                          _attr_list, chunk_size, limit)
