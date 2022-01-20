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
import logging
from collections import OrderedDict

from servicex.transformer.arrow_writer import ArrowWriter
from servicex.transformer.object_store_manager import ObjectStoreManager
from servicex.transformer.uproot_transformer import UprootTransformer
import pyarrow as pa

# Mock module moved in python 3.3
import sys
if sys.version_info >= (3, 3):
    from unittest.mock import call  # noqa: E402
else:
    from mock import call  # noqa: E402


class CaptureHandler(logging.StreamHandler):
    """
    Handler that captures messages being logged so that they can be checked
    """

    def __init__(self):
        super(CaptureHandler, self).__init__()
        self.__messages = []

    def emit(self, record):
        self.__messages.append(self.format(record))

    def get_messages(self):
        return self.__messages

    def reset_messages(self):
        self.__messages = []


class TestArrowWriter:
    def test_init(self, mocker):
        mock_object_store = mocker.MagicMock(ObjectStoreManager)

        aw = ArrowWriter(file_format='hdf5',
                         object_store=mock_object_store)

        assert aw.object_store == mock_object_store
        assert aw.file_format == 'hdf5'

    def test_transform_file_object_store(self, mocker):
        from servicex.transformer.scratch_file_writer import ScratchFileWriter

        mock_object_store = mocker.MagicMock(ObjectStoreManager)
        mock_scratch_file = mocker.MagicMock(ScratchFileWriter)
        scratch_file_init = mocker.patch(
            "servicex.transformer.scratch_file_writer.ScratchFileWriter",
            return_value=mock_scratch_file)

        mock_scratch_file.file_path = "/tmp/foo"

        aw = ArrowWriter(file_format='parquet',
                         object_store=mock_object_store)

        mock_transformer = mocker.MagicMock(UprootTransformer)
        mock_transformer.file_path = '/tmp/foo'
        mock_transformer.attr_name_list = ['a', 'b']

        data = OrderedDict([('strs', [chr(c) for c in range(ord('a'), ord('n'))]),
                            ('ints', list(range(1, 14)))])
        table = pa.Table.from_pydict(data)
        table2 = pa.Table.from_pydict(data)

        mock_transformer.arrow_table = mocker.Mock(return_value=iter([table, table2]))
        aw.write_branches_to_arrow(transformer=mock_transformer, request_id="123-45")

        scratch_file_init.assert_called_with(file_format='parquet')
        mock_transformer.arrow_table.assert_called_with()
        mock_scratch_file.open_scratch_file.assert_called_once_with(table)
        mock_scratch_file.append_table_to_scratch.assert_has_calls(
            [call(table), call(table2)])

        mock_scratch_file.close_scratch_file.assert_called_once()

        mock_object_store.upload_file.assert_called_once_with("123-45", ":tmp:foo",
                                                              "/tmp/foo")
        mock_scratch_file.remove_scratch_file.assert_called_once()
