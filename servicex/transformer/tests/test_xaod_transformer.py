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

from collections import OrderedDict


# noinspection PyClassHasNoInit
class TestXAODTransformer:

    def test_arrow_table(self, mocker):
        import mock
        from servicex.transformer.xaod_transformer import XAODTransformer

        attr_names = [
            "Electrons.pt()", "Electrons.eta()",  "Muons.e()"
        ]

        from servicex.transformer.xaod_events import XAODEvents
        iterator = mock.MagicMock(XAODEvents)
        iterator.iterate = mock.Mock(return_value=iter([
            {'Electrons': [
                {'pt()': 4.0, 'eta()': 5.0},
                {'pt()': 8.0, 'eta()': 10.0}
            ],
                'Muons': [
                    {'e()': 1.0}, {'e()': 2.0}, {'e()': 3.0}
                ]
            },
            {'Electrons': [
                {'pt()': 14.0, 'eta()': 15.0},
                {'pt()': 18.0, 'eta()': 20.0}
            ],
                'Muons': [
                    {'e()': 1.0}, {'e()': 2.0}, {'e()': 3.0}
                ]
            }
        ]))

        iterator.attr_name_list = attr_names
        iterator.get_entry_count = mock.Mock(return_value=1000)
        iterator.chunk_size = 2
        transformer = XAODTransformer(iterator)
        table = transformer.arrow_table().next()

        assert table.column_names == ['Electrons_pt',
                                      'Muons_e',
                                      'Electrons_eta']

        assert len(table) == 2
        assert table.num_rows == 2
        assert table.num_columns == 3
        assert table.shape == (2, 3)
        pydict = table.to_pydict()
        print(pydict)
        assert pydict == OrderedDict([
            ('Electrons_pt', [[4.0, 8.0], [14.0, 18.0]]),
            ('Muons_e', [[1.0, 2.0, 3.0],  [1.0, 2.0, 3.0]]),
            ('Electrons_eta', [[5.0, 10.0], [15.0, 20.0]])
        ])

        iterator.iterate.assert_called()
