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
import time
import logging

import pyarrow as pa


class ArrowWriter:

    def __init__(self, file_format=None, object_store=None, messaging=None):
        self.file_format = file_format
        self.object_store = object_store
        self.messaging = messaging
        self.messaging_timings = []
        self.object_store_timing = 0
        self.avg_cell_size = []
        self.__init_logger()

    def __init_logger(self):
        # Default logger doesn't print so that code that uses library
        # can override
        handler = logging.NullHandler()
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handler)

    def write_branches_to_arrow(self, transformer,
                                topic_name, file_id, request_id):
        from .scratch_file_writer import ScratchFileWriter
        tick = time.time()
        scratch_writer = None
        total_messages = 0

        for pa_table in transformer.arrow_table():
            if self.object_store:
                if not scratch_writer:
                    scratch_writer = ScratchFileWriter(file_format=self.file_format)
                    scratch_writer.open_scratch_file(pa_table)

                scratch_writer.append_table_to_scratch(pa_table)

            if self.messaging:
                batches = pa_table.to_batches(max_chunksize=transformer.chunk_size)

                for batch in batches:
                    messaging_tick = time.time()

                    # Just need to make key unique to shard messages across brokers
                    key = str.encode(transformer.file_path + "-" + str(total_messages))

                    sink = pa.BufferOutputStream()
                    writer = pa.RecordBatchStreamWriter(sink, batch.schema)
                    writer.write_batch(batch)
                    writer.close()
                    self.messaging.publish_message(
                        topic_name,
                        key,
                        sink.getvalue())

                    self.avg_cell_size.append(len(sink.getvalue().to_pybytes()) /
                                              len(transformer.attr_name_list) /
                                              batch.num_rows)
                    total_messages += 1
                    self.messaging_timings.append(time.time() - messaging_tick)

        if self.object_store:
            object_store_tick = time.time()
            scratch_writer.close_scratch_file()

            self.logger.info("Writing parquet to {0} as ".format(request_id),
                             transformer.file_path.replace('/', ':'))

            self.object_store.upload_file(request_id,
                                          transformer.file_path.replace('/', ':'),
                                          scratch_writer.file_path)

            scratch_writer.remove_scratch_file()
            self.object_store_timing = time.time() - object_store_tick

        tock = time.time()

        if self.messaging:
            # need the float type conversion so that / operation doesn't use integer div
            # TODO: remove float call when we drop support for python 2
            print(self.avg_cell_size)
            avg_avg_cell_size = float(sum(self.avg_cell_size)) / len(self.avg_cell_size) \
                if len(self.avg_cell_size) else 0
            self.logger.info("Wrote {0} events to {1} ".format(total_messages, topic_name) +
                             "Avg Cell Size = {0} bytes".format(avg_avg_cell_size))

        self.logger.info("Real time: {0} minutes".format(round((tock - tick) / 60.0, 2)))
