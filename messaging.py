from kafka import KafkaProducer



kafka_brokers = ['servicex-kafka-0.slateci.net:19092',
                 'servicex-kafka-1.slateci.net:19092',
                 'servicex-kafka-2.slateci.net:19092']



def connect_kafka_producer(brokers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=brokers,
                                  api_version=(0, 10))
        print("Kafka connected successfully")
    except Excception as ex:
        print("Exception while connecting Kafka")
        raise
    finally:
        return _producer



def publish_message(producer_instance, topic_name, key, value_buffer):
    try:
        producer_instance.send(topic_name, key=str(key),
                               value=value_buffer.to_pybytes())
        producer_instance.flush()
        print("Message published successfully")
    except Exception as ex:
        print("Exception in publishing message")
        raise
