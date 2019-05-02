#!/usr/bin/env python

# Set up ROOT and RootCore:
import ROOT
import numpy as np
ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')

# Initialize the xAOD infrastructure:
if not ROOT.xAOD.Init().isSuccess():
    print("Failed xAOD.Init()")



def print_branches(file_name):
    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    ROOT.gSystem.RedirectOutput('temp.txt', 'w')
    tree_in.Print()
    ROOT.gROOT.ProcessLine("gSystem->RedirectOutput(0);")



def write_branches_to_ntuple(file_name, branch_name):
    sw = ROOT.TStopwatch()
    sw.Start()
    
    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    tree_in.SetBranchStatus('*', 0)
    tree_in.SetBranchStatus('EventInfo', 1)
    tree_in.SetBranchStatus(branch_name, 1)
    
    n_entries = tree_in.GetEntries()
    print("Total entries: " + str(n_entries))

    file_out = ROOT.TFile('flat_ntuple.root', 'recreate')
    tree_out = ROOT.TTree('flat_ntuple', "Flat ntuple")

    n_particles = np.zeros(1, dtype=int)
    particle_pt = ROOT.std.vector('float')()
    particle_eta = ROOT.std.vector('float')()
    particle_phi = ROOT.std.vector('float')()
    particle_e = ROOT.std.vector('float')()

    b_n_particles = tree_out.Branch('n_particles', n_particles, 'n_particles/I')
    b_particle_pt = tree_out.Branch('particle_pt', particle_pt)
    b_particle_eta = tree_out.Branch('particle_eta', particle_eta)
    b_particle_phi = tree_out.Branch('particle_phi', particle_phi)
    b_particle_e = tree_out.Branch('particle_e', particle_e)

    for j_entry in xrange(n_entries):
        tree_in.GetEntry(j_entry)
        if j_entry % 1000 == 0:
            print("Processing run #" + str(tree_in.EventInfo.runNumber())
                  + ", event #" + str(tree_in.EventInfo.eventNumber())
                  + " (" + str(round(100.0 * j_entry / n_entries, 2)) + "%)")

        exec("particles = tree_in." + branch_name)

        n_particles[0] = particles.size()
        particle_pt.clear()
        for i in xrange(particles.size()):
            particle = particles.at(i)
            particle_pt.push_back(particle.trackParticle().pt())
            particle_eta.push_back(particle.trackParticle().eta())
            particle_phi.push_back(particle.trackParticle().phi())
            particle_e.push_back(particle.trackParticle().e())
        
        tree_out.Fill()

    file_out.Write()
    file_out.Close()
    ROOT.xAOD.ClearTransientTrees()
    
    sw.Stop()
    print("Real time: " + str(round(sw.RealTime() / 60.0, 2)) + " minutes")
    print("CPU time:  " + str(round(sw.CpuTime() / 60.0, 2)) + " minutes")
