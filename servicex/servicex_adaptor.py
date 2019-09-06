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
import requests


class ServiceX:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def _servicex(self, path):
        return self.endpoint+path

    def get_transform_requests(self):
        rpath_output = requests.get(
            self._servicex('/dpath/to_transform'),
            verify=False)

        if rpath_output.text == 'false':
            return None
        else:
            return rpath_output.json()

    def get_request_info(self, request_id):
        request_output = requests.get(
            self._servicex('/drequest/' + request_id),
            verify=False)
        return request_output.json()

    def update_events_served(self, request_id, path_id, num_events):
        requests.put(self._servicex('/events_served/' + request_id + '/'
                     + path_id + '/' + str(num_events)), verify=False)

    # def update_request_events_served(self, request_id, num_events):
        # requests.put(self._servicex('/drequest/events_served/' + request_id +
                     # '/' + str(num_events)), verify=False)

    # def update_path_events_served(self, path_id, num_events):
        # requests.put(self._servicex('/dpath/events_served/' + path_id +
                     # '/' + str(num_events)), verify=False)

    def post_validated_status(self, path_id):
        requests.put(self._servicex('/dpath/status/' + path_id + '/Validated/OK'),
                     verify=False)

    def post_transformed_status(self, path_id):
        requests.put(self._servicex('/dpath/status/' + path_id + '/Transformed/OK'),
                     verify=False)

    def post_failed_status(self, path_id, pod_id):
        requests.put(self._servicex('/dpath/status/' + path_id + '/Failed/' + pod_id),
                     verify=False)
