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
import os
import time
import redis
import codecs


class RedisMessaging:
    def __init__(self, host='redis.slateci.net', port=6379):

        self.MAX_MESSAGES_PER_REQUEST = 100000
        if 'MAX_MESSAGES_PER_REQUEST' in os.environ:
            self.MAX_MESSAGES_PER_REQUEST = int(os.environ['MAX_MESSAGES_PER_REQUEST'])
        print("max messages per request:", self.MAX_MESSAGES_PER_REQUEST)
        self.host = host
        self.port = port
        self.client = None

        print('Configured Redis backend')

    def set_redis_client(self):
        self.client = redis.Redis(self.host, self.port, db=0)

    def publish_message(self, topic_name, key, value_buffer):
        # connect if not already done
        try:
            while True:
                if self.client:
                    # print('Redis client connected.')
                    break
                self.set_redis_client()
                print('waiting to connect redis client...')
                time.sleep(60)
        except Exception as ex:
            print("Exception in publishing message:", ex)
            raise
        request_id = 'req_id:' + topic_name
        # check if queue is still full, if yes return false.
        if self.client.xlen(request_id) > self.MAX_MESSAGES_PER_REQUEST:
            return False

        # add message
        self.client.xadd(request_id, {
            'pa': key,
            'data': codecs.encode(value_buffer, 'bz2')
        })
        return True

    def request_status_redis(self, topic_name):
        try:
            topic_name = 'req_id:' + topic_name
            request_info = self.client.xinfo_stream(topic_name)
            print('req_info:', request_info)
        except redis.exceptions.ResponseError as redis_e:
            print(redis_e)
