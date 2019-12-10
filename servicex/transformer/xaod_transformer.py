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
import awkward


class XAODTransformer:
    def __init__(self, event_iterator):
        self.event_iterator = event_iterator

    @property
    def file_path(self):
        return self.event_iterator.file_path

    @property
    def attr_name_list(self):
        return self.event_iterator.attr_name_list

    @property
    def chunk_size(self):
        return self.event_iterator.chunk_size

    def arrow_table(self):

        def group(iterator, n):
            """
            Batch together chunks of events into a single yield
            :param iterator: Iterator from which events are drown
            :param n: Number of events to include in each yield
            :return: Yields a list of n or fewer events
            """
            done = False
            while not done:
                results = []
                try:
                    for i in range(n):
                        results.append(iterator.next())
                    yield results
                except StopIteration:
                    done = True
                    yield results

        for events in group(self.event_iterator.iterate(), self.chunk_size):
            object_array = awkward.fromiter(events)
            attr_dict = {}
            for attr_name in self.event_iterator.attr_name_list:
                branch_name = attr_name.split('.')[0].strip(' ')
                a_name = attr_name.split('.')[1]
                full_name = branch_name + '_' + a_name.strip('()')
                if object_array.size >= 1:
                    attr_dict[full_name] = object_array[branch_name][a_name]

            object_table = awkward.Table(**attr_dict)
            yield awkward.toarrow(object_table)
