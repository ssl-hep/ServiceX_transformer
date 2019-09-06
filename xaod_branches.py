#!/usr/bin/env python
from __future__ import division

# Set up ROOT, uproot, and RootCore:
import json

import math
import os
import sys
import ROOT
import argparse
import numpy as np
import pyarrow as pa
import awkward
import requests
import time
from servicex.transformer.kafka_messaging import KafkaMessaging
from servicex.transformer.redis_messaging import RedisMessaging
from servicex.servicex_adaptor import ServiceX
from pympler import muppy, summary



default_brokerlist = "servicex-kafka-0.slateci.net:19092, " \
                     "servicex-kafka-1.slateci.net:19092," \
                     "servicex-kafka-2.slateci.net:19092"

default_attr_names = "Electrons.pt(), " \
                     "Electrons.eta(), " \
                     "Electrons.phi(), " \
                     "Electrons.e()," \
                     "Muons.pt(), " \
                     "Muons.eta(), " \
                     "Muons.phi(), " \
                     "Muons.e()"

# default_servicex_endpoint = 'https://servicex.slateci.net'
default_servicex_endpoint = 'https://servicex-frontend.uc.ssl-hep.org:80'

# How many bytes does an average awkward array cell take up. This is just
# a rule of thumb to calculate chunksize
avg_cell_size = 42

# What is the largest message we want to send (in megabytes).
# Note this must be less than the kafka broker setting if we are using kafka
default_max_message_size = 5

# Number of seconds to wait for consumer to wake up
default_wait_for_consumer = 600

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

parser.add_argument("--wait", dest='wait', action='store',
                    default=os.environ.get('WAIT_FOR_CONSUMER',
                                           default_wait_for_consumer),
                    help="Number of seconds to wait for consumer to restart")

parser.add_argument('--kafka', dest='kafka_messaging', action='store_true',
                    default=False, help='Use Kafka Backend for messages')

parser.add_argument('--redis', dest='redis_messaging', action='store_true',
                    default=False, help='Use Redis Backend for messages')

parser.add_argument("--redis-host", dest='redis_host', action='store',
                    default='redis.slateci.net',
                    help='Host for redis messaging backend')

parser.add_argument("--redis-port", dest='redis_port', action='store',
                    default='6379',
                    help='Port for redis messaging backend')

parser.add_argument("--dataset", dest='dataset', action='store',
                    default=None,
                    help='JSON Dataset document from DID Finder')

parser.add_argument("--max-message-size", dest='max_message_size',
                    action='store', default=default_max_message_size,
                    help='Max message size in megabytes')

ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')


# Use a heuristic to guess at an optimum message chunk to fill the
# max_message_size
def _compute_chunk_size(attr_list, max_message_size):
    print("Chunks comp", max_message_size * 1e6, len(attr_list), avg_cell_size)
    return int(max_message_size * 1e6 / len(attr_list) / avg_cell_size)


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
                    single_particle_attr[a_name] = getattr(particle, a_name.strip('()'))()
                full_event[branch_name].append(single_particle_attr)

        yield full_event


def print_branches(file_name, branch_name):
    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    ROOT.gSystem.RedirectOutput('temp.txt', 'w')
    tree_in.Print()

    # To print the attributes of a particle collection
    ROOT.gSystem.RedirectOutput('xaod_branch_attributes.txt', 'w')
    tree_in.GetEntry(0)
    particles = getattr(tree_in, branch_name)
    if particles.size() >= 1:
        for method_name in dir(particles.at(0)):
            print(method_name)

    ROOT.gROOT.ProcessLine("gSystem->RedirectOutput(0);")


def write_branches_to_ntuple(file_name, attr_name_list):
    sw = ROOT.TStopwatch()
    sw.Start()

    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    branches = {}
    for attr_name in attr_name_list:
        if not attr_name.split('.')[0].strip(' ') in branches:
            branches[attr_name.split('.')[0].strip(' ')] = [attr_name.split('.')[1]]
        else:
            branches[attr_name.split('.')[0].strip(' ')].append(attr_name.split('.')[1])

    tree_in.SetBranchStatus('*', 0)
    tree_in.SetBranchStatus('EventInfo', 1)
    for branch_name in branches:
        tree_in.SetBranchStatus(branch_name, 1)

    n_entries = tree_in.GetEntries()
    print("Total entries: " + str(n_entries))

    file_out = ROOT.TFile('flat_file.root', 'recreate')
    tree_out = ROOT.TTree('flat_tree', '')

    particle_attr = {}
    for branch_name in branches:
        particle_attr[branch_name] = np.zeros(1, dtype=int)
    for attr_name in attr_name_list:
        particle_attr[attr_name] = ROOT.std.vector('float')()

    b_particle_attr = {}
    for branch_name in branches:
        b_particle_attr[branch_name] = tree_out.Branch(
            'n_' + branch_name,
            particle_attr[branch_name],
            'n_' + branch_name + '/I'
        )
    for attr_name in attr_name_list:
        b_particle_attr[attr_name] = tree_out.Branch(
            attr_name.split('.')[0].strip(' ') + '_' + attr_name.split('.')[1].strip('()'),
            particle_attr[attr_name]
        )

    for j_entry in xrange(n_entries):
        tree_in.GetEntry(j_entry)
        if j_entry % 1000 == 0:
            print("Processing run #" + str(tree_in.EventInfo.runNumber())
                  + ", event #" + str(tree_in.EventInfo.eventNumber())
                  + " (" + str(round(100.0 * j_entry / n_entries, 2)) + "%)")

        particles = {}
        for branch_name in branches:
            particles[branch_name] = getattr(tree_in, branch_name)
            particle_attr[branch_name][0] = particles[branch_name].size()
            for a_name in branches[branch_name]:
                attr_name = branch_name + '.' + a_name
                particle_attr[attr_name].clear()
                for i in xrange(particle_attr[branch_name]):
                    particle = particles[branch_name].at(i)
                    exec('particle_attr[attr_name].push_back(particle.' + a_name + ')')

        tree_out.Fill()

    file_out.Write()
    file_out.Close()
    ROOT.xAOD.ClearTransientTrees()

    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")


def poll_for_root_files(servicex, messaging, chunk_size, wait_for_consumer, event_limit=None):
    waited = 0
    rpath_output = servicex.get_transform_requests()

    if not rpath_output:
        print("nothing to do...")
        time.sleep(60)
        return

    _id = rpath_output['_id']
    _file_path = rpath_output['_source']['file_path']
    _request_id = rpath_output['_source']['req_id']
    print("Received ID: " + _id + ", path: " + _file_path)

    request_output = servicex.get_request_info(_request_id)

    attr_name_list = request_output['_source']['columns']
    print("Received request: " + _request_id + ", columns: " + str(attr_name_list))
    write_branches_to_arrow(messaging, _request_id, _file_path, _id, attr_name_list, chunk_size, wait_for_consumer, servicex, event_limit)


def write_branches_to_arrow(messaging, topic_name, file_path, id, attr_name_list, chunk_size, wait_for_consumer, servicex, event_limit=None):
    sw = ROOT.TStopwatch()
    sw.Start()

    try:
        file_in = ROOT.TFile.Open(file_path)
        tree_in = ROOT.xAOD.MakeTransientTree(file_in)

        branches = {}
        for attr_name in attr_name_list:
            attr_name = str(attr_name)
            if not attr_name.split('.')[0].strip(' ') in branches:
                branches[attr_name.split('.')[0].strip(' ')] = [attr_name.split('.')[1]]
            else:
                branches[attr_name.split('.')[0].strip(' ')].append(attr_name.split('.')[1])

        tree_in.SetBranchStatus('*', 0)
        tree_in.SetBranchStatus('EventInfo', 1)
        for branch_name in branches:
            tree_in.SetBranchStatus(branch_name, 1)

        n_entries = tree_in.GetEntries()
        print("Total entries: " + str(n_entries))
        if event_limit:
            n_entries = min(n_entries, event_limit)
            print("Limiting to the first "+str(n_entries)+" events")

        n_chunks = int(math.ceil(n_entries / chunk_size))
        for i_chunk in xrange(n_chunks):
            first_event = i_chunk * chunk_size
            last_event = min((i_chunk + 1) * chunk_size, n_entries)

            object_array = awkward.fromiter(
                make_event_table(tree_in, branches, first_event, last_event)
            )

            attr_dict = {}
            for attr_name in attr_name_list:
                branch_name = attr_name.split('.')[0].strip(' ')
                a_name = attr_name.split('.')[1]
                attr_dict[branch_name + '_' + a_name.strip('()')] = object_array[branch_name][a_name]

            object_table = awkward.Table(**attr_dict)
            pa_table = awkward.toarrow(object_table)
            batches = pa_table.to_batches(chunksize=chunk_size)

            # Leaving this for now; currently batches is a list of size 1,
            # but it gives us the flexibility to define an iterator over
            # multiple batches in the future
            batch_number = i_chunk * len(batches)
            for batch in batches:
                sink = pa.BufferOutputStream()
                writer = pa.RecordBatchStreamWriter(sink, batch.schema)
                writer.write_batch(batch)
                writer.close()
                while True:
                    key = file_path + "-" + str(batch_number)
                    published = messaging.publish_message(topic_name, key, sink.getvalue())

                    if published:
                        avg_cell_size = len(sink.getvalue().to_pybytes()) / len(attr_name_list) / batch.num_rows
                        print("Batch number " + str(batch_number) + ", "
                                 + str(batch.num_rows) +
                              " events published to " + topic_name,
                              "Avg Cell Size = "+ str(avg_cell_size) + " bytes")
                        batch_number += 1

                        if servicex:
                            servicex.update_request_events_served(topic_name, batch.num_rows)
                            servicex.update_path_events_served(id, batch.num_rows)

                        waited = 0

                        # Memory check
                        all_objects = muppy.get_objects()
                        sum1 = summary.summarize(all_objects)
                        summary.print_(sum1)
                        del all_objects
                        del sum1
                        break
                    else:
                        print("not published. Waiting 10 seconds before retry.")
                        time.sleep(10)
                        waited += 10
                        if waited > wait_for_consumer:
                            if servicex:
                                servicex.post_validated_status(id)
                            sys.exit(0)
        ROOT.xAOD.ClearTransientTrees()

        if servicex:
            servicex.post_transformed_status(id)
    except:
        print("Problem reading file " + file_path)
        MY_POD_NAME = os.environ.get('MY_POD_NAME')
        servicex.post_failed_status(id, MY_POD_NAME)

    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")


def transform_dataset(dataset, messaging, topic_name, id, attr_list, chunk_size,
                      wait_for_consumer, limit):
    with open(dataset, 'r') as f:
        datasets = json.load(f)
        for rec in datasets:
            print "Transforming ", rec[u'file_path'], rec[u'file_events']
            write_branches_to_arrow(messaging, topic_name, rec[u'file_path'],
                                    id, attr_list, chunk_size,
                                    wait_for_consumer, None, limit)


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

    if args.kafka_messaging == args.redis_messaging:
        print("You must specify one and only one messaging backend")
        exit(-1)

    if args.kafka_messaging:
        messaging = KafkaMessaging(kafka_brokers, float(args.max_message_size))
    elif args.redis_messaging:
        messaging = RedisMessaging(args.redis_host, args.redis_port)

    if args.chunks:
        chunk_size = int(args.chunks)
    else:
        chunk_size = _compute_chunk_size(_attr_list, float(args.max_message_size))

    wait_for_consumer = int(args.wait)

    print("Atlas xAOD Transformer")
    print(_attr_list)
    print("Chunk size ", chunk_size)

    servicex = ServiceX(args.servicex_endpoint)

    limit = int(args.limit) if args.limit else None

    if args.path:
        print("Transforming a single path: ", args.path)
        write_branches_to_arrow(messaging, args.topic, args.path, "cli",
                                _attr_list, chunk_size, wait_for_consumer,
                                None, limit)
    elif args.dataset:
        print("Transforming files from saved dataset ", args.dataset)
        transform_dataset(args.dataset, messaging, args.topic, "cli",
                          _attr_list, chunk_size, wait_for_consumer, limit)
    else:
        print("Polling for files from ", args.servicex_endpoint)
        while True:
            poll_for_root_files(servicex, messaging, chunk_size,
                                wait_for_consumer, limit)
