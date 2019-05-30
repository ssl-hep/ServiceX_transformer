#!/usr/bin/env python

# Set up ROOT, uproot, and RootCore:
import os
import ROOT
import numpy as np
import pyarrow as pa
import awkward
from kafka import KafkaProducer

ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')
kafka_brokers = ['servicex-kafka.kafka.svc.cluster.local:9092']



def connect_kafka_producer(brokers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=brokers,
                                  api_version=(0, 10))
    except Excception as ex:
        print("Exception while connecting Kafka")
        # print(ex)
    finally:
        return _producer



def publish_message(producer_instance, topic_name, key, value_bytes):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print("Message published successfully")
    except Exception as ex:
        print("Exception in publishing message")
        # print(ex)



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

        if j_entry == 5: break



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



def write_branches_to_arrow(file_name, attr_name_list):
    sw = ROOT.TStopwatch()
    sw.Start()
    
    print(file_name)
    
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
    batches = pa_table.to_batches()
    file_number = 1
    for batch in batches:
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        publish_message(producer, topic_name='servicex', key=file_number,
                        value_bytes=sink.getvalue())

    ROOT.xAOD.ClearTransientTrees()
    
    # Needs work:
    # os.system("curl -XPUT https://servicex/slateci.net/dpath/transform/[id]/Transformed")

    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")
