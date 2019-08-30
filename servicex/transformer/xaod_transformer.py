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
import os
import sys

import awkward
import math

import ROOT


class XAODTransformer:
    def __init__(self, event_iterator):
        self.event_iterator = event_iterator

    def arrow_table(self, chunk_size, event_limit=sys.maxint):
        n_entries = self.event_iterator.get_entry_count()
        print("Total entries: " + str(n_entries))
        if event_limit:
            n_entries = min(n_entries, event_limit)
            print("Limiting to the first " + str(n_entries) + " events")

        n_chunks = int(math.ceil(n_entries / chunk_size))
        print("n_chunks ", n_chunks)
        for i_chunk in xrange(n_chunks):
            first_event = i_chunk * chunk_size
            last_event = min((i_chunk + 1) * chunk_size, n_entries)

            print("chunks ", first_event, last_event)
            object_array = awkward.fromiter(
                self.event_iterator.iterate(first_event, last_event)
            )

            attr_dict = {}
            for attr_name in self.event_iterator.attr_name_list:
                branch_name = attr_name.split('.')[0].strip(' ')
                a_name = attr_name.split('.')[1]

                attr_dict[branch_name + '_' + a_name.strip('()')] = \
                    object_array[branch_name][a_name]

            object_table = awkward.Table(**attr_dict)
            return awkward.toarrow(object_table)
