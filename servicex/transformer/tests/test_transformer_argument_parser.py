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
import sys
from servicex.transformer.transformer_argument_parser import TransformerArgumentParser


class TestTransformerArgumentParser:
    def test_init(self, mocker):
        argparse.ArgumentParser.exit = mocker.Mock()
        arg_parser = TransformerArgumentParser(description="Test Transformer")
        sys.argv = ["foo", "bar"]
        arg_parser.parse_args()
        argparse.ArgumentParser.exit.assert_called()

    def test_parse(self, mocker):
        arg_parser = TransformerArgumentParser(description="Test Transformer")
        sys.argv = ["foo",
                    "--path", "/foo/bar",
                    "--brokerlist", "kafka1.org",
                    "--topic", "mytopic",
                    "--chunks", "100",
                    "--tree", "Events",
                    "--limit", "10",
                    '--result-destination', 'kafka',
                    '--result-format', 'arrow',
                    "--max-message-size", "42",
                    '--rabbit-uri', "http://rabbit.org",
                    '--request-id', "123-45-678"
                    ]

        args = arg_parser.parse_args()
        assert args.path == '/foo/bar'
        assert args.brokerlist == "kafka1.org"
        assert args.topic == 'mytopic'
        assert args.chunks == 100
        assert args.tree == 'Events'
        assert args.limit == 10
        assert args.result_destination == 'kafka'
        assert args.result_format == 'arrow'
        assert args.max_message_size == 42
        assert args.rabbit_uri == "http://rabbit.org"
        assert args.request_id == "123-45-678"

    def test_extract_broker_list(self):
        brokers = TransformerArgumentParser.extract_kafka_brokers("kafka1,kafka2")
        assert len(brokers) == 2
        assert brokers[0] == 'kafka1'
        assert brokers[1] == 'kafka2'

    def test_extract_attr_list(self):
        attrs = TransformerArgumentParser.extract_attr_list("a,b,c")
        assert len(attrs) == 3
        assert attrs == ['a', 'b', 'c']
