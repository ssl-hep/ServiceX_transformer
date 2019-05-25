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
#!/usr/bin/env python

# Set up ROOT, uproot, and RootCore:
import ROOT
import numpy as np
import awkward
import sys
import argparse
import pyarrow as pa


ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')
from kafka import KafkaProducer

kakfa_brokers = ['localhost:9092']

def connect_kafka_producer(brokers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=brokers, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, key, value_bytes):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def make_event_table(tree, branches):
    n_entries = tree.GetEntries()
    print("Total entries: " + str(n_entries))

    for j_entry, event in enumerate(tree):
        if j_entry % 1000 == 0:
            print("Processing run #" + str(event.EventInfo.runNumber())
                  + ", event #" + str(event.EventInfo.eventNumber())
                  + " (" + str(round(100.0 * j_entry / n_entries, 2)) + "%)")

        full_event = []
        for branch_name in branches:
            for particle in getattr(event, branch_name):
                particle_attr = {}
                for a_name in branches[branch_name]:
                    attr_name = branch_name + '.' + a_name
                    exec('particle_attr[attr_name] = particle.' + a_name)
                full_event.append(particle_attr)

        yield full_event

        # if j_entry == 5: break


def write_branches_to_kafka(file_name, attr_name_list, topic):

    sw = ROOT.TStopwatch()
    sw.Start()

    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    branches = {}
    for attr_name in attr_name_list:
        if not attr_name.split('.')[0] in branches:
            branches[attr_name.split('.')[0]] = [attr_name.split('.')[1]]
        else:
            branches[attr_name.split('.')[0]].append(attr_name.split('.')[1])

    tree_in.SetBranchStatus('*', 0)
    tree_in.SetBranchStatus('EventInfo', 1)
    for branch_name in branches:
        tree_in.SetBranchStatus(branch_name, 1)

    object_array = awkward.fromiter(make_event_table(tree_in, branches))
    object_table = awkward.Table(pt=object_array['Electrons.pt()'],
                                 eta=object_array['Electrons.eta()'])

    pa_table = awkward.toarrow(object_table)
    batches = pa_table.to_batches()
    event_number  = 1 # Need to replace this with value from Root event
    for batch in batches:
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()

        publish_message(producer, topic_name=topic, key=event_number,
                        value_bytes=sink.getvalue())

    ROOT.xAOD.ClearTransientTrees()

    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")

parser = argparse.ArgumentParser(description='Transform root file and send to kafka.')
parser.add_argument('--file', dest='file_name', action='store',
                    default="/xaodfiles/AOD.11182705._000001.pool.root.1",
                    help='Path to Root file to transform)')

parser.add_argument("--cols", dest='columns', action='store',
                    default='Electrons',
                    help='List of branches to extract as columns')

parser.add_argument("--broker", dest='broker', action='store',
                    default='localhost:9092',
                    help='Kafka broker to connect to')


parser.add_argument("--topic", dest='topic', action='store',
                    default='servicex',
                    help='Kafka topic to publish to')
args = parser.parse_args()

producer = connect_kafka_producer([args.broker])

write_branches_to_kafka(args.file_name, args.columns, args.topic)
