import sys
import time


class Messaging:
    def __init__(self, backend='redis'):
        self.backend = backend
        if backend == 'redis':
            try:
                import redis
                import codecs
                global redis
                global codecs
            except ImportError:
                print("Redis messaging requested by Module not available.")
                sys.exit(1)
            self.configure_redis()
            print('Configured Redis backend')

        if backend == 'kafka':
            try:
                from kafka import KafkaProducer
                global KafkaProducer
            except ImportError:
                print("Kafka messaging requested by Module not available.")
                sys.exit(1)
            self.configure_kafka()
            print('Configured Kafka backend')

    def configure_redis(self, host='redis.slateci.net', port=6379, max_messages_per_request=500):
        self.host = host
        self.port = port
        self.max_messages_per_request = max_messages_per_request
        self.client = None

    def configure_kafka(self, brokers=None):
        if not brokers:
            self.brokers = ['servicex-kafka-0.slateci.net:19092',
                            'servicex-kafka-1.slateci.net:19092',
                            'servicex-kafka-2.slateci.net:19092']
        self.producer = None

    def set_redis_client(self):
        self.client = redis.Redis(self.host, self.port, db=0)

    def set_kafka_producer(self):

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.brokers,
                                          api_version=(0, 10))
            print("Kafka producer created successfully")
        except Exception as ex:
            print("Exception while getting Kafka producer", ex)
            sys.exit(1)

    def publish_message(self, topic_name, key, value_buffer):
        if self.backend == 'kafka':
            self.publish_message_kafka(topic_name, key, value_buffer)
        if self.backend == 'redis':
            self.publish_message_redis(topic_name, key, value_buffer)

    def publish_message_kafka(self, topic_name, key, value_buffer):
        try:
            while True:
                self.set_kafka_producer()
                if self.producer:
                    print('Kafka producer connected.')
                    break
                print('waiting to connect kafka producer...')
                time.sleep(60)
            self.producer.send(topic_name, key=str(key),
                               value=value_buffer.to_pybytes())
            self.producer.flush()
            print("Message published successfully")
        except Exception as ex:
            print("Exception in publishing message", ex)
            raise

    def publish_message_redis(self, request_id, key, value_buffer):
        # connect if not already done
        try:
            while True:
                self.set_redis_client()
                if self.client:
                    # print('Redis client connected.')
                    break
                print('waiting to connect redis client...')
                time.sleep(60)
        except Exception as ex:
            print("Exception in publishing message:", ex)
            raise

        # check if queue is still full, if yes return false.
        if self.client.xlen(request_id) > self.max_messages_per_request:
            return False

        # add message
        self.client.xadd(request_id, {'pa': key, 'data': codecs.encode(value_buffer, 'bz2')})

    def request_status_redis(self, request_id):
        try:
            request_info = self.client.xinfo_stream(request_id)
            print('req_info:', request_info)
        except redis.exceptions.ResponseError as redis_e:
            print(redis_e)
