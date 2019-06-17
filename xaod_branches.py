#!/usr/bin/env python

# Set up ROOT, uproot, and RootCore:
import os
import ROOT
import numpy as np
import pyarrow as pa
import awkward
import requests
from kafka import KafkaProducer
import time
# import uproot_methods

ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')
kafka_brokers = ['servicex-kafka-0.slateci.net:19092',
                 'servicex-kafka-1.slateci.net:19092',
                 'servicex-kafka-2.slateci.net:19092']

# How many events to include in each Kafka message. This needs to be small
# enough to keep below the broker and consumer max bytes
chunk_size = 5000



def connect_kafka_producer(brokers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=brokers,
                                  api_version=(0, 10))
        print("Kafka connected successfully")
    except Excception as ex:
        print("Exception while connecting Kafka")
        raise
    finally:
        return _producer



def publish_message(producer_instance, topic_name, key, value_buffer):
    try:
        producer_instance.send(topic_name, key=str(key),
                               value=value_buffer.to_pybytes())
        producer_instance.flush()
        print("Message published successfully")
    except Exception as ex:
        print("Exception in publishing message")
        raise



def make_event_table(tree, branches):
    n_entries = tree.GetEntries()
    print("Total entries: " + str(n_entries))

    for j_entry in xrange(n_entries):
        tree.GetEntry(j_entry)
        if j_entry % 1000 == 0:
            print("Processing run #" + str(tree.EventInfo.runNumber())
                  + ", event #" + str(tree.EventInfo.eventNumber())
                  + " (" + str(round(100.0 * j_entry / n_entries, 2)) + "%)")

        particles = {}
        full_event = []
        for branch_name in branches:
            particles[branch_name] = getattr(tree, branch_name)
            for i in xrange(particles[branch_name].size()):
                particle = particles[branch_name].at(i)
                single_particle_attr = {}
                for a_name in branches[branch_name]:
                    attr_name = branch_name + '.' + a_name
                    exec('single_particle_attr[attr_name] = particle.' + a_name)
                full_event.append(single_particle_attr)

        yield full_event

        # if j_entry == 5: break



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
        if not attr_name.split('.')[0] in branches:
            branches[attr_name.split('.')[0]] = [attr_name.split('.')[1]]
        else:
            branches[attr_name.split('.')[0]].append(attr_name.split('.')[1])

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
            attr_name.split('.')[0] + '_' + attr_name.split('.')[1].strip('()'),
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
        
        if not rpath_output == 'false':
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
                if not attr_name.split('.')[0] in branches:
                    branches[attr_name.split('.')[0]] = [attr_name.split('.')[1]]
                else:
                    branches[attr_name.split('.')[0]].append(attr_name.split('.')[1])
            
            tree_in.SetBranchStatus('*', 0)
            tree_in.SetBranchStatus('EventInfo', 1)
            for branch_name in branches:
                tree_in.SetBranchStatus(branch_name, 1)

            object_array = awkward.fromiter(make_event_table(tree_in, branches))

            table_def = "awkward.Table("
            for attr_name in attr_name_list[:-1]:
                table_def = (table_def + attr_name.split('.')[0] + '_'
                            + attr_name.split('.')[1].strip('()') + "=object_array['"
                            + attr_name + "'], ")
            table_def = (table_def + attr_name_list[-1].split('.')[0] + '_'
                        + attr_name_list[-1].split('.')[1].strip('()')
                        + "=object_array['" + attr_name_list[-1] + "'])")
            object_table = eval(table_def)
            # object_table = awkward.Table(Electrons_pt=object_array['Electrons.pt()'], Electrons_eta=object_array['Electrons.eta()'], Electrons_phi=object_array['Electrons.phi()'], Electrons_e=object_array['Electrons.e()'])

            producer = connect_kafka_producer(kafka_brokers)
            pa_table = awkward.toarrow(object_table)
            batches = pa_table.to_batches(chunksize=chunk_size)
            
            # TODO: batch number should be changed so it is unique for the request.
            batch_number = 0
            for batch in batches:
                sink = pa.BufferOutputStream()
                writer = pa.RecordBatchStreamWriter(sink, batch.schema)
                writer.write_batch(batch)
                writer.close()
                publish_message(producer, topic_name='servicex', key=batch_number,
                                value_buffer=sink.getvalue())
                batch_number += 1

            ROOT.xAOD.ClearTransientTrees()
            
            requests.put('https://servicex.slateci.net/dpath/transform/' + str(_request_id) + '/Transformed', verify=False)

            sw.Stop()
            print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
            print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")

        else:
            time.sleep(10)
