#!/usr/bin/env python

import numpy as np
import ROOT



def find_m(vList):
    v1 = ROOT.TLorentzVector()
    for i in vList:
        v1 += i
    return v1.M()



sw = ROOT.TStopwatch()
sw.Start()

file_in = ROOT.TFile.Open("flat_file.root")
tree_in = file_in.Get("flat_tree")

n_entries = tree_in.GetEntries()

file_out = ROOT.TFile('flat_file_test.root', 'recreate')
tree_out = ROOT.TTree('flat_tree_test', '')

mDiparticle = np.zeros(1, dtype=float)

b_mDiparticle = tree_out.Branch('mDiparticle', mDiparticle, 'mDiparticle/D')

for j_entry in range(n_entries):
    tree_in.GetEntry(j_entry)
    if j_entry % 1000 == 0:
        print('Processing entry ' + str(j_entry)
              + ' (' + str(round(100.0 * j_entry / n_entries, 2)) + '%)')

    v_particleList = []
    mDiparticle[0] = -9.9
    for i in range(tree_in.n_particles):
        if (tree_in.particle_pt[i] > 25.0
            and abs(tree_in.particle_eta[i]) < 1.444):
            v_particle = ROOT.TLorentzVector()
            v_particle.SetPtEtaPhiE(tree_in.particle_pt[i], tree_in.particle_eta[i],
                                    tree_in.particle_phi[i], tree_in.particle_e[i])
            v_particleList.append(v_particle)
    if len(v_particleList) >= 2:
        mDiparticle[0] = find_m([v_particleList[0], v_particleList[1]])
        tree_out.Fill()

file_out.Write()
file_out.Close()

sw.Stop()
print('Real time: ' + str(round(sw.RealTime() / 60.0, 2)) + ' minutes')
print('CPU time:  ' + str(round(sw.CpuTime() / 60.0, 2)) + ' minutes')
