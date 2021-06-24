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

import argparse

default_brokerlist = "servicex-kafka-0.slateci.net:19092, " \
                     "servicex-kafka-1.slateci.net:19092," \
                     "servicex-kafka-2.slateci.net:19092"

default_attr_names = "Events.Electron_pt,Events.Electron_eta,Muon_phi"

# What is the largest message we want to send (in megabytes).
# Note this must be less than the kafka broker setting if we are using kafka
default_max_message_size = 14.5


class TransformerArgumentParser(argparse.ArgumentParser):

    def __init__(self, description="ServiceX Transformer"):
        super(TransformerArgumentParser, self).__init__(description=description)

        self.add_argument("--brokerlist", dest='brokerlist', action='store',
                          default=default_brokerlist,
                          help='List of Kafka broker to connect to')

        self.add_argument("--topic", dest='topic', action='store',
                          default='servicex',
                          help='Kafka topic to publish arrays to')

        self.add_argument("--chunks", dest='chunks', action='store',
                          default=None, type=int,
                          help='Arrow Buffer Chunksize')

        self.add_argument("--tree", dest='tree', action='store',
                          default="Events",
                          help='Tree from which columns will be inspected')

        self.add_argument("--path", dest='path', action='store',
                          default=None,
                          help='Path to single Root file to transform')

        self.add_argument("--limit", dest='limit', action='store',
                          default=None, type=int,
                          help='Max number of events to process')

        self.add_argument('--result-destination', dest='result_destination',
                          action='store',
                          default='kafka', help='kafka, object-store, output-dir',
                          choices=['kafka', 'object-store', 'output-dir', 'volume'])

        self.add_argument('--output-dir', dest='output_dir',
                          action='store',
                          default=None, help='Local directory to output results')

        self.add_argument('--result-format', dest='result_format', action='store',
                          default='arrow', help='arrow, parquet, root-file',
                          choices=['arrow', 'parquet', 'root-file'])

        self.add_argument("--max-message-size", dest='max_message_size',
                          action='store', default=default_max_message_size,
                          type=int,
                          help='Max message size in megabytes')

        self.add_argument('--rabbit-uri', dest="rabbit_uri", action='store',
                          default='host.docker.internal')

        self.add_argument('--request-id', dest='request_id', action='store',
                          default='servicex', help='Request ID to read from queue')

        self.add_argument('--subdir', dest='subdir', action='store',
                          default='servicex',
                          help='subdirectory in the persistence volume to write result to')

    @classmethod
    def extract_kafka_brokers(cls, brokerlist):
        return list(map(lambda b: b.strip(), brokerlist.split(",")))

    @classmethod
    def extract_attr_list(cls, attr_names):
        return list(map(lambda b: b.strip(), attr_names.split(",")))
