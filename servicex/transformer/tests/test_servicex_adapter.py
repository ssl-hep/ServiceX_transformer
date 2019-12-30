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
from servicex.transformer.servicex_adapter import ServiceXAdapter


class TestServiceXAdapter:

    def test_put_file_complete(self, mocker):
        mock_requests_put = mocker.patch('requests.put')
        adapter = ServiceXAdapter("http://foo.com")
        adapter.put_file_complete("my-root.root", 42, "testing", 1, 2, 3, 4)
        mock_requests_put.assert_called()
        args = mock_requests_put.call_args
        assert args[0][0] == 'http://foo.com/file-complete'
        doc = args[1]['json']
        assert doc['status'] == 'testing'
        assert doc['total-events'] == 3
        assert doc['total-time'] == 2
        assert doc['file-path'] == 'my-root.root'
        assert doc['num-messages'] == 1
        assert doc['file-id'] == 42
        assert doc['avg-rate'] == 1

    def test_post_status_update(self, mocker):
        mock_requests_post = mocker.patch('requests.post')
        adapter = ServiceXAdapter("http://foo.com")
        adapter.post_status_update("testing")
        mock_requests_post.assert_called()
        args = mock_requests_post.call_args
        assert args[0][0] == 'http://foo.com/status'
        doc = args[1]['data']
        assert doc['status'] == 'testing'
