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


# noinspection PyClassHasNoInit,PyMethodMayBeStatic
class TestXAODEvents:
    def _generate_mock_phys_obj(self, mock, mock_map):
        a_key = list(mock_map.keys())[0]
        size_vals = [len(o) for o in mock_map[a_key]]

        obj = mock.Mock()
        obj.size = mock.Mock(side_effect=size_vals)
        obj_at_mock = mock.Mock()
        for key in mock_map.keys():
            flatened = [o2 for o in mock_map[key] for o2 in o]
            add_attr_expr = "type(obj_at_mock).%s = mock.Mock(side_effect=%s)" % \
                (key, str(flatened))
            exec add_attr_expr

        obj.at = mock.Mock(return_value=obj_at_mock)
        return obj

    def test_get_entry_count(self, mocker):
        import mock
        import ROOT

        from servicex.transformer.xaod_events import XAODEvents

        attr_names = [
            "Electrons.pt()", "Electrons.eta()",  "Muons.e()"
        ]

        mock_tree = mock.Mock()
        xaod_mock = mocker.patch.object(ROOT, "xAOD")
        mock_file = mock.Mock()
        mocker.patch.object(ROOT.TFile, 'Open',
                                        return_value=mock_file)
        xaod_mock.MakeTransientTree = mock.Mock(return_value=mock_tree)

        mock_tree.GetEntries = mock.Mock(return_value=2)
        mock_tree.GetEntry = mock.Mock()
        event_iterator = XAODEvents("foo/bar", attr_names, chunk_size=100, event_limit=None)

        assert event_iterator.get_entry_count() == 2
        xaod_mock.MakeTransientTree.assert_called()

    def test_event_iter_no_limit(self, mocker):
        import ROOT
        import mock
        import pytest
        from servicex.transformer.xaod_events import XAODEvents

        attr_names = [
            "Electrons.pt()", "Electrons.eta()",  "Muons.e()"
        ]

        mock_file = mock.Mock()
        open_mock = mocker.patch.object(ROOT.TFile, 'Open',
                                        return_value=mock_file)

        mock_tree = mock.Mock()
        xaod_mock = mocker.patch.object(ROOT, "xAOD")
        xaod_mock.MakeTransientTree = mock.Mock(return_value=mock_tree)

        event_iterator = XAODEvents("foo/bar", attr_names, chunk_size=100, event_limit=None)

        mock_tree.GetEntries = mock.Mock(return_value=2)
        mock_tree.GetEntry = mock.Mock()

        mock_tree.Muons = self._generate_mock_phys_obj(mock, {
            "e": [
                [1, 2, 3],
                [4, 5]
            ]
        })

        mock_tree.Electrons = self._generate_mock_phys_obj(mock, {
            "pt": [
                [4, 8],
                [16, 20, 24]
            ],
            "eta": [
                [5, 10],
                [20, 25, 30]
            ]
        })

        array_gen = event_iterator.iterate()

        array = array_gen.next()
        print(array)
        mock_tree.GetEntry.assert_called_with(0)
        mock_tree.Muons.at.assert_called_with(2)
        mock_tree.Electrons.at.assert_called_with(1)
        open_mock.assert_called_with("foo/bar")

        assert len(array['Muons']) == 3
        assert len(array['Electrons']) == 2
        assert array['Electrons'] == [{'pt()': 4, 'eta()': 5},
                                      {'pt()': 8, 'eta()': 10}]
        assert array['Muons'] == [{'e()': 1}, {'e()': 2}, {'e()': 3}]

        array2 = array_gen.next()
        print(array2)
        mock_tree.GetEntry.assert_called_with(1)
        mock_tree.Muons.at.assert_called_with(1)
        mock_tree.Electrons.at.assert_called_with(2)
        assert len(array2['Muons']) == 2
        assert len(array2['Electrons']) == 3

        assert array2['Electrons'] == [{'pt()': 16, 'eta()': 20},
                                       {'pt()': 20, 'eta()': 25},
                                       {'pt()': 24, 'eta()': 30}]

        assert array2['Muons'] == [{'e()': 4}, {'e()': 5}]

        with pytest.raises(StopIteration):
            array_gen.next()
        xaod_mock.MakeTransientTree.assert_called()

    def test_chunksize_greater_than_events(self, mocker):
        assert True

    def batch_larger_than_max_message_bytes(self, mocker):
        assert True
