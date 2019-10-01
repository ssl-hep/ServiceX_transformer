#!/usr/bin/env python
from __future__ import division

import json
import os
import sys

# noinspection PyPackageRequirements
import ROOT
import argparse
# Set up ROOT, uproot, and RootCore:
import datetime
import pika
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from servicex.servicex_adaptor import ServiceX
from servicex.transformer.kafka_messaging import KafkaMessaging
from servicex.transformer.object_store_manager import ObjectStoreManager
from servicex.transformer.xaod_events import XAODEvents
from servicex.transformer.xaod_transformer import XAODTransformer

default_brokerlist = "servicex-kafka-0.slateci.net:19092, " \
                     "servicex-kafka-1.slateci.net:19092," \
                     "servicex-kafka-2.slateci.net:19092"

default_attr_names = "Electrons.pt(), " \
                     "Electrons.eta(), " \
                     "Electrons.phi(), " \
                     "Electrons.e()"

default_servicex_endpoint = 'https://servicex.slateci.net'

# How many bytes does an average awkward array cell take up. This is just
# a rule of thumb to calculate chunksize
avg_cell_size = 42

# What is the largest message we want to send (in megabytes).
# Note this must be less than the kafka broker setting if we are using kafka
default_max_message_size = 14.5

messaging = None

parser = argparse.ArgumentParser(
    description='Transform xAOD files into flat n-tuples.')

parser.add_argument("--brokerlist", dest='brokerlist', action='store',
                    default=default_brokerlist,
                    help='List of Kafka broker to connect to')

parser.add_argument("--topic", dest='topic', action='store',
                    default='servicex',
                    help='Kafka topic to publish arrays to')

parser.add_argument("--chunks", dest='chunks', action='store',
                    default=None,
                    help='Arrow Buffer Chunksize')

parser.add_argument("--attrs", dest='attr_names', action='store',
                    default=default_attr_names,
                    help='List of attributes to extract')

parser.add_argument("--servicex", dest='servicex_endpoint', action='store',
                    default=default_servicex_endpoint,
                    help='Endpoint for servicex')

parser.add_argument("--path", dest='path', action='store',
                    default=None,
                    help='Path to single Root file to transform')

parser.add_argument("--limit", dest='limit', action='store',
                    default=None,
                    help='Max number of events to process')

parser.add_argument('--result-destination', dest='result_destination', action='store',
                    default='kafka', help='kafka, object-store',
                    choices=['kafka', 'object-store'])

parser.add_argument('--result-format', dest='result_format', action='store',
                    default='arrow', help='arrow, parquet', choices=['arrow', 'parquet'])


parser.add_argument("--dataset", dest='dataset', action='store',
                    default=None,
                    help='JSON Dataset document from DID Finder')

parser.add_argument("--max-message-size", dest='max_message_size',
                    action='store', default=default_max_message_size,
                    help='Max message size in megabytes')

parser.add_argument('--rabbit-uri', dest="rabbit_uri", action='store',
                    default='host.docker.internal')

parser.add_argument('--request-id', dest='request_id', action='store',
                    default=None, help='Request ID to read from queue')

ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')


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


def make_event_table(tree, branches, f_evt, l_evt):
    n_entries = tree.GetEntries()
    for j_entry in xrange(f_evt, l_evt):
        tree.GetEntry(j_entry)
        if j_entry % 1000 == 0:
            print("Processing run #" + str(tree.EventInfo.runNumber())
                  + ", event #" + str(tree.EventInfo.eventNumber())
                  + " (" + str(round(100.0 * j_entry / n_entries, 2)) + "%)")

        particles = {}
        full_event = {}
        for branch_name in branches:
            full_event[branch_name] = []
            particles[branch_name] = getattr(tree, branch_name)
            for i in xrange(particles[branch_name].size()):
                particle = particles[branch_name].at(i)
                single_particle_attr = {}
                for a_name in branches[branch_name]:
                    single_particle_attr[a_name] = \
                        getattr(particle, a_name.strip('()'))()
                full_event[branch_name].append(single_particle_attr)

        yield full_event

        # if j_entry == 6000: break


def post_status_update(endpoint, status_msg):
    requests.post(endpoint+"/status", data={
        "timestamp": datetime.datetime.now().isoformat(),
        "status": status_msg
    })


def put_file_complete(endpoint, file_path, status, num_messages=None,
                      total_time=None):
    doc = {
        "file-path": file_path,
        "status": status,
        "num-messages": num_messages,
        "total-time": total_time
    }
    print("------< ", doc)
    if endpoint:
        requests.put(endpoint+"/file-complete", json={
            "file-path": file_path,
            "status": status,
            "num-messages": num_messages,
            "total-time": total_time
        })


def write_branches_to_arrow(messaging, topic_name, file_path, servicex_id, attr_name_list,
                            chunk_size, server_endpoint, event_limit=None,
                            object_store=None):
    sw = ROOT.TStopwatch()
    sw.Start()

    scratch_writer = None

    event_iterator = XAODEvents(file_path, attr_name_list)
    transformer = XAODTransformer(event_iterator)

    file_in = ROOT.TFile.Open(file_path)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    batch_number = 0
    for pa_table in transformer.arrow_table(chunk_size, event_limit):
        if object_store:
            if not scratch_writer:
                scratch_writer = _open_scratch_file(args.result_format, pa_table)
            _append_table_to_scratch(args.result_format, scratch_writer, pa_table)

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

                avg_cell_size = len(sink.getvalue().to_pybytes()) / len(
                    attr_name_list) / batch.num_rows
                print("Batch number " + str(batch_number) + ", "
                      + str(batch.num_rows) +
                      " events published to " + topic_name,
                      "Avg Cell Size = " + str(avg_cell_size) + " bytes")
                batch_number += 1

                if server_endpoint:
                    post_status_update(server_endpoint, "Processed " +
                                       str(batch.num_rows))

    if object_store:
        _close_scratch_file(args.result_format, scratch_writer)
        print("Writing parquet to ", args.request_id, " as ", file_path.replace('/', ':'))
        object_store.upload_file(args.request_id, file_path.replace('/', ':'), "/tmp/out")
        os.remove("/tmp/out")

    ROOT.xAOD.ClearTransientTrees()

    if server_endpoint:
        post_status_update(server_endpoint, "File " + file_path + " complete")

    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")
    put_file_complete(server_endpoint, file_path, "success",
                      batch_number, sw.RealTime())


def transform_dataset(dataset, messaging, topic_name, servicex_id, attr_list, chunk_size,
                      limit):
    with open(dataset, 'r') as f:
        datasets = json.load(f)
        for rec in datasets:
            print "Transforming ", rec[u'file_path'], rec[u'file_events']
            write_branches_to_arrow(messaging, topic_name, rec[u'file_path'], servicex_id,
                                    attr_list, chunk_size, None, limit)


# noinspection PyUnusedLocal
def callback(channel, method, properties, body):
    transform_request = json.loads(body)
    _request_id = transform_request['request-id']
    _file_path = transform_request['file-path']
    _server_endpoint = transform_request['service-endpoint']
    _id = 1
    columns = list(map(lambda b: b.strip(),
                       transform_request['columns'].split(",")))

    print(_file_path)

    write_branches_to_arrow(messaging=messaging, topic_name=_request_id,
                            file_path=_file_path, servicex_id=_id, attr_name_list=columns,
                            chunk_size=chunk_size, server_endpoint=_server_endpoint,
                            object_store=object_store)

    channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":

    # Print help if no args are provided
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()

    args = parser.parse_args()

    # Convert comma separated broker string to a list
    kafka_brokers = list(map(lambda b: b.strip(), args.brokerlist.split(",")))

    # Convert comma separated attribute string to a list
    _attr_list = list(map(lambda b: b.strip(), args.attr_names.split(",")))

    print("\n\n---->", args.result_destination)
    if args.result_destination == 'kafka':
        messaging = KafkaMessaging(kafka_brokers, float(args.max_message_size))
        object_store = None
    elif args.result_destination == 'object-store':
        messaging = None
        object_store = ObjectStoreManager(os.environ['MINIO_URL'],
                                          os.environ['MINIO_ACCESS_KEY'],
                                          os.environ['MINIO_SECRET_KEY'])
        print("Object store initialized to ", object_store.minio_client)

    if args.chunks:
        chunk_size = int(args.chunks)
    else:
        chunk_size = _compute_chunk_size(_attr_list,
                                         float(args.max_message_size))

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

    servicex = ServiceX(args.servicex_endpoint)

    limit = int(args.limit) if args.limit else None

    if args.path:
        print("Transforming a single path: ", args.path)
        write_branches_to_arrow(messaging, args.topic, args.path, "cli", _attr_list,
                                chunk_size, None, limit, object_store=object_store)
    elif args.dataset:
        print("Transforming files from saved dataset ", args.dataset)
        transform_dataset(args.dataset, messaging, args.topic, "cli", _attr_list,
                          chunk_size, limit)
