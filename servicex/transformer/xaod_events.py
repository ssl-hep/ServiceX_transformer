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

import ROOT


class XAODEvents:
    def __init__(self, file_path, attr_name_list, chunk_size, event_limit=None):
        self.file_path = file_path
        print("Openning ", file_path)
        self.file_in = ROOT.TFile.Open(file_path)
        print(self.file_in)
        self.tree = ROOT.xAOD.MakeTransientTree(self.file_in)
        self.attr_name_list = attr_name_list
        self.event_limit = event_limit
        self.chunk_size = chunk_size

    def _create_branch_dict(self):
        branches = {}
        for attr_name in self.attr_name_list:
            attr_name = str(attr_name)
            if not attr_name.split('.')[0].strip(' ') in branches:
                branches[attr_name.split('.')[0].strip(' ')] = [
                    attr_name.split('.')[1]]
            else:
                branches[attr_name.split('.')[0].strip(' ')].append(
                    attr_name.split('.')[1])
        return branches

    def _select_branches(self):
        self.branches = self._create_branch_dict()

        self.tree.SetBranchStatus('*', 0)
        self.tree.SetBranchStatus('EventInfo', 1)
        for branch_name in self.branches:
            self.tree.SetBranchStatus(branch_name, 1)

    def get_entry_count(self):
        return self.tree.GetEntries()

    def iterate(self):
        self._select_branches()

        n_entries = self.tree.GetEntries()
        if self.event_limit:
            n_entries = min(n_entries, self.event_limit)
            print("Limiting to the first " + str(n_entries) + " events")

        for j_entry in range(n_entries):
            self.tree.GetEntry(j_entry)
            if j_entry % 1000 == 0:
                print("Processing run #" + str(self.tree.EventInfo.runNumber())
                      + ", event #" + str(self.tree.EventInfo.eventNumber())
                      + " (" + str(
                            round(100.0 * j_entry / n_entries, 2)) + "%)")

            particles = {}
            full_event = {}
            for branch_name in self.branches:
                full_event[branch_name] = []
                particles[branch_name] = getattr(self.tree, branch_name)
                for i in xrange(particles[branch_name].size()):
                    particle = particles[branch_name].at(i)
                    single_particle_attr = {}
                    for a_name in self.branches[branch_name]:
                        single_particle_attr[a_name] = \
                            getattr(particle, a_name.strip('()'))()
                    full_event[branch_name].append(single_particle_attr)

            yield full_event
