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
import pyarrow as pa


class ArrowWriter:

    def __init__(self, file_format=None, servicex=None,
                 object_store=None, messaging=None):
        self.file_format = file_format
        self.servicex = servicex
        self.object_store = object_store
        self.messaging = messaging

    def write_branches_to_arrow(self, transformer,
                                topic_name, file_id, request_id):
        from .scratch_file_writer import ScratchFileWriter

        tick = time.time()

        scratch_writer = None

        batch_number = 0
        total_events = 0
        total_bytes = 0
        for pa_table in transformer.arrow_table():
            if self.object_store:
                if not scratch_writer:
                    scratch_writer = ScratchFileWriter(file_format=self.file_format)
                    scratch_writer.open_scratch_file(pa_table)

                scratch_writer.append_table_to_scratch(pa_table)

            total_events = total_events + pa_table.num_rows
            batches = pa_table.to_batches(max_chunksize=transformer.chunk_size)

            for batch in batches:
                if self.messaging:
                    key = str.encode(transformer.file_path + "-" + str(batch_number))

                    sink = pa.BufferOutputStream()
                    writer = pa.RecordBatchStreamWriter(sink, batch.schema)
                    writer.write_batch(batch)
                    writer.close()
                    self.messaging.publish_message(
                        topic_name,
                        key,
                        sink.getvalue())

                    total_bytes = total_bytes + len(sink.getvalue().to_pybytes())

                    avg_cell_size = len(sink.getvalue().to_pybytes()) / len(
                        transformer.attr_name_list) / batch.num_rows
                    print("Batch number " + str(batch_number) + ", "
                          + str(batch.num_rows) +
                          " events published to " + topic_name,
                          "Avg Cell Size = " + str(avg_cell_size) + " bytes")
                    batch_number += 1

                    # if server_endpoint:
                    #     post_status_update(server_endpoint, "Processed " +
                    #                        str(batch.num_rows))

        if self.object_store:
            scratch_writer.close_scratch_file()

            print("Writing parquet to ", request_id, " as ",
                  transformer.file_path.replace('/', ':'))

            self.object_store.upload_file(request_id,
                                          transformer.file_path.replace('/', ':'),
                                          scratch_writer.file_path)

            scratch_writer.remove_scratch_file()

        if self.servicex:
            self.servicex. post_status_update("File " + transformer.file_path + " complete")

        tock = time.time()
        print("Real time: " + str(round(tock - tick / 60.0, 2)) + " minutes")
        if self.servicex:
            self.servicex.put_file_complete(transformer.file_path, file_id, "success",
                                            num_messages=batch_number,
                                            total_time=round(tock - tick / 60.0, 2),
                                            total_events=total_events,
                                            total_bytes=total_bytes)
