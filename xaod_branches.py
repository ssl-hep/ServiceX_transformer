#!/usr/bin/env python
from __future__ import division

# Set up ROOT, uproot, and RootCore:
import math
import os
import ROOT
import numpy as np
import pyarrow as pa
import awkward
import requests
import time
import messaging
# import uproot_methods



ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')
chunk_size = 500   # Events per Kafka message



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
                    exec('single_particle_attr[a_name] = particle.' + a_name)
                full_event[branch_name].append(single_particle_attr)

        yield full_event

        # if j_entry == 6000: break



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



def write_branches_to_arrow():
    while True:
        rpath_output = requests.get('https://servicex.slateci.net/dpath/transform', verify=False)

        if not rpath_output.text == 'false':
            _id = rpath_output.json()['_id']
            _file_path = rpath_output.json()['_source']['file_path']
            _request_id = rpath_output.json()['_source']['req_id']
            print("Received ID: " + _id + ", path: " + _file_path)

            request_output = requests.get('https://servicex.slateci.net/drequest/' + _request_id, verify=False)
            attr_name_list = request_output.json()['_source']['columns']
            print("Received request: " + _request_id + ", columns: " + str(attr_name_list))

            # requests.put('https://servicex.slateci.net/dpath/transform/' + str(_request_id) + '/Transforming', verify=False)

            sw = ROOT.TStopwatch()
            sw.Start()

            file_in = ROOT.TFile.Open(_file_path)
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

            n_chunks = int(math.ceil(n_entries / chunk_size))
            for i_chunk in xrange(n_chunks):
                first_event = i_chunk * chunk_size
                last_event = min((i_chunk + 1) * chunk_size, n_entries)

                object_array = awkward.fromiter(
                    make_event_table(tree_in, branches, first_event, last_event)
                    )

                table_def = "awkward.Table("
                for attr_name in attr_name_list[:-1]:
                    branch_name = attr_name.split('.')[0].strip(' ')
                    a_name = attr_name.split('.')[1]
                    table_def = (table_def + branch_name + '_' + a_name.strip('()')
                                + "=object_array['" + branch_name + "']['"
                                + a_name + "'], ")
                last_branch_name = attr_name_list[-1].split('.')[0].strip(' ')
                last_a_name = attr_name_list[-1].split('.')[1]
                table_def = (table_def + last_branch_name + '_' + last_a_name.strip('()')
                            + "=object_array['" + last_branch_name + "']['"
                            + last_a_name + "'])")
                object_table = eval(table_def)

                # object_table = awkward.Table(Electrons_pt=object_array['Electrons']['pt()'],
                                            # Electrons_eta=object_array['Electrons']['eta()'],
                                            # Electrons_phi=object_array['Electrons']['phi()'],
                                            # Electrons_e=object_array['Electrons']['e()'],
                                            # Muons_pt=object_array['Muons']['pt()'],
                                            # Muons_eta=object_array['Muons']['eta()'],
                                            # Muons_phi=object_array['Muons']['phi()'],
                                            # Muons_e=object_array['Muons']['e()'])

                producer = messaging.connect_kafka_producer(messaging.kafka_brokers)
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
                    messaging.publish_message(producer, topic_name=_request_id,
                                              key=batch_number,
                                              value_buffer=sink.getvalue())
                    print("Batch number " + str(batch_number) + ", " + str(batch.num_rows) + " events published to " + _request_id)
                    batch_number += 1
                    requests.put('https://servicex.slateci.net/drequest/events_served/' + _request_id + '/' + str(batch.num_rows), verify=False)

            ROOT.xAOD.ClearTransientTrees()

            requests.put('https://servicex.slateci.net/dpath/status/' + _id + '/Transformed', verify=False)

            sw.Stop()
            print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
            print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")

        else:
            time.sleep(10)
