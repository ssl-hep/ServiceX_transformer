#!/usr/bin/env python

# Set up ROOT and RootCore:
import ROOT
import numpy as np
ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')



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



def write_branches_to_ntuple(file_name, branch_name, attr_name_list):
    sw = ROOT.TStopwatch()
    sw.Start()
    
    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    tree_in.SetBranchStatus('*', 0)
    tree_in.SetBranchStatus('EventInfo', 1)
    tree_in.SetBranchStatus(branch_name, 1)
    
    n_entries = tree_in.GetEntries()
    print("Total entries: " + str(n_entries))

    file_out = ROOT.TFile('flat_file.root', 'recreate')
    tree_out = ROOT.TTree('flat_tree', '')

    n_particles = np.zeros(1, dtype=int)
    particle_attr = {}
    for attr_name in attr_name_list:
        particle_attr[attr_name] = ROOT.std.vector('float')()

    b_n_particles = tree_out.Branch('n_particles', n_particles, 'n_particles/I')
    b_particle_attr = {}
    for attr_name in attr_name_list:
        b_particle_attr[attr_name] = tree_out.Branch('particle_' + attr_name, particle_attr[attr_name])

    for j_entry in xrange(n_entries):
        tree_in.GetEntry(j_entry)
        if j_entry % 1000 == 0:
            print("Processing run #" + str(tree_in.EventInfo.runNumber())
                  + ", event #" + str(tree_in.EventInfo.eventNumber())
                  + " (" + str(round(100.0 * j_entry / n_entries, 2)) + "%)")

        particles = getattr(tree_in, branch_name)
        n_particles[0] = particles.size()
        for attr_name in attr_name_list:
            particle_attr[attr_name].clear()
        for i in xrange(particles.size()):
            particle = particles.at(i)

            # Maybe enforce branch type with:
            # if exec("type(particle." + attr_name + "())") == float:
            for attr_name in attr_name_list:
                exec("particle_attr[attr_name].push_back(particle." + attr_name + "())")
        
        tree_out.Fill()

    file_out.Write()
    file_out.Close()
    ROOT.xAOD.ClearTransientTrees()
    
    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")
