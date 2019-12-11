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
from pyarrow.parquet import ParquetWriter
import pyarrow as pa


class TestScratchFileWriter:
    def _test_table(self):
        schema = pa.schema([('field1', pa.int32())])
        return pa.Table.from_arrays([[1]], schema=schema)

    def test_init(self, mocker):
        from servicex.transformer.scratch_file_writer import ScratchFileWriter

        writer = ScratchFileWriter('parquet')
        assert writer.file_path == '/tmp/out'
        assert not writer.scratch_file
        assert writer.file_format == 'parquet'

    def test_open_scratch_file(self, mocker):
        table = self._test_table()

        mock_parquet = mocker.MagicMock(ParquetWriter)
        foo = mocker.patch("pyarrow.parquet.ParquetWriter", return_value=mock_parquet)

        from servicex.transformer.scratch_file_writer import ScratchFileWriter
        writer = ScratchFileWriter('parquet')

        writer.open_scratch_file(table)
        foo.assert_called_with("/tmp/out", table.schema)

    def test_append_table_to_scratch(self, mocker):
        mock_parquet = mocker.MagicMock(ParquetWriter)
        mocker.patch("pyarrow.parquet.ParquetWriter", return_value=mock_parquet)

        table = self._test_table()
        from servicex.transformer.scratch_file_writer import ScratchFileWriter
        writer = ScratchFileWriter('parquet')

        writer.open_scratch_file(table)
        writer.append_table_to_scratch(table)
        mock_parquet.write_table.assert_called()

    def test_close_scratch_file(self, mocker):
        table = self._test_table()

        mock_parquet = mocker.MagicMock(ParquetWriter)
        mocker.patch("pyarrow.parquet.ParquetWriter", return_value=mock_parquet)

        from servicex.transformer.scratch_file_writer import ScratchFileWriter
        writer = ScratchFileWriter('parquet')
        writer.open_scratch_file(table)
        writer.close_scratch_file()
        mock_parquet.close.assert_called()
        assert not writer.scratch_file

    def test_remove_scratch_file(self, mocker):
        mock_remove = mocker.patch("os.remove")
        from servicex.transformer.scratch_file_writer import ScratchFileWriter
        writer = ScratchFileWriter('parquet')
        writer.remove_scratch_file()
        mock_remove.assert_called_with("/tmp/out")
