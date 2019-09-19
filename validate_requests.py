#!/usr/bin/env python

# this code gets requests in state: Created, Validates request on one file
# if request valid (all branches exist) it sets request state to Defined
# if not it sets state to Failed, deletes all the paths belonging to that request.
import datetime
import json
import sys

import time
import ROOT
import requests
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import argparse
import pika

ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')

# What is the largest message we want to send (in megabytes).
# Note this must be less than the kafka broker setting if we are using kafka
default_max_message_size = 14.5


default_brokerlist = "servicex-kafka-0.slateci.net:19092, " \
                     "servicex-kafka-1.slateci.net:19092," \
                     "servicex-kafka-2.slateci.net:19092"

default_servicex_endpoint = 'https://servicex.slateci.net'

parser = argparse.ArgumentParser(
    description='Validate a request and create kafka topic.')

parser.add_argument("--create-topic", dest="create_topic", action='store_true',
                    default=False, help="Just create the topic")

parser.add_argument("--brokerlist", dest='brokerlist', action='store',
                    default=default_brokerlist,
                    help='List of Kafka broker to connect to')

parser.add_argument("--topic", dest='topic', action='store',
                    default='servicex',
                    help='Kafka topic to publish arrays to')

parser.add_argument("--chunks", dest='chunks', action='store',
                    default=None,
                    help='Arrow Buffer Chunksize')

parser.add_argument("--servicex", dest='servicex_endpoint', action='store',
                    default=default_servicex_endpoint,
                    help='Endpoint for servicex')

parser.add_argument("--path", dest='path', action='store',
                    default=None,
                    help='Path to single Root file to transform')

parser.add_argument("--max-message-size", dest='max_message_size',
                    action='store', default=default_max_message_size,
                    help='Max message size in megabytes')

parser.add_argument('--rabbit-uri', dest="rabbit_uri", action='store',
                    default='host.docker.internal')


CONFIG = {
    'bootstrap.servers': 'servicex-kafka-1.slateci.net:19092',
    'group.id': 'monitor',
    'client.id': 'monitor',
    'session.timeout.ms': 5000,
}

ADMIN = AdminClient(CONFIG)


def validate_branches(file_name, branch_names):
    print("Validating file: " + file_name)
    # file_in = ROOT.TFile.Open('AOD.11182705._000001.pool.root.1')
    file_in = ROOT.TFile.Open(file_name)
    tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    for branch_name in branch_names:
        if '.' not in branch_name:
            print(branch_name + " is not valid collection + attribute")
            return(False, "Not valid collection.attribute")

        branch = branch_name.split('.')[0].strip(' ')
        attr = branch_name.split('.')[1].strip('()')
        for i_evt in range(10):
            tree_in.GetEntry(i_evt)
            try:
                particles = getattr(tree_in, branch)
                if particles.size() >= 1:
                    if not attr in dir(particles.at(0)):
                        print(attr + " is not an attribute of " + branch)
                        return(False, attr + " is not an attribute of " + branch)
                    break
            except Exception:
                return(False, "No collection with name:" + branch)

    return(True, {
        "max_event_size": 1440
    })


def create_kafka_topic(admin, topic):
    config = {
        'compression.type': 'lz4',
        'max.message.bytes': 14500000
    }

    new_topics = [NewTopic(topic, num_partitions=100, replication_factor=1,
                           config=config)]
    response = admin.create_topics(new_topics, request_timeout=15.0)
    for topic, res in response.items():
        try:
            res.result()   # The result itself is None
            print("Topic {} created".format(topic))
        except KafkaException as k_execpt:
            k_error = k_execpt.args[0]
            print(k_error.str())
            return(k_error.code() == 36)


def post_status_update(endpoint, status_msg):
    requests.post(endpoint + "/status", data={
        "timestamp": datetime.datetime.now().isoformat(),
        "status": status_msg
    })


def post_transform_start(endpoint, info):
    requests.post(endpoint+"/start", json={
        "timestamp": datetime.datetime.now().isoformat(),
        "info": info
    })


def callback(channel, method, properties, body):
    validation_request = json.loads(body)

    columns = list(map(lambda b: b.strip(),
                       validation_request['columns'].split(",")))

    service_endpoint = validation_request[u'service-endpoint']
    post_status_update(service_endpoint,
                       "Validation Request received")

    # checks the file
    (valid, info) = validate_branches(
        validation_request[u'file-path'], columns
    )

    if valid:
        post_status_update(service_endpoint,  "Request validated")
        post_transform_start(service_endpoint, info)
    else:
        post_status_update(service_endpoint, "Validation Request failed "+info)

    print(valid, info)
    channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    args = parser.parse_args()
    rabbitmq = pika.BlockingConnection(
        pika.URLParameters(args.rabbit_uri)
    )
    _channel = rabbitmq.channel()
    _channel.queue_declare('validated_requests')

    _channel.basic_consume(queue="validation_requests",
                           auto_ack=False,
                           on_message_callback=callback)
    _channel.start_consuming()

    # Convert comma separated broker string to a list
    kafka_brokers = list(map(lambda b: b.strip(), args.brokerlist.split(",")))

    CONFIG = {
        'bootstrap.servers': kafka_brokers[0],
        'group.id': 'monitor',
        'client.id': 'monitor',
        'session.timeout.ms': 5000,
    }

    kafka_admin = AdminClient(CONFIG)

    if args.create_topic:
        print("Just creating topic "+args.topic+" in kafka")
        create_kafka_topic(kafka_admin, args.topic)
        sys.exit(0)

    while True:
        # gets request in Created
        req_resp = requests.get(
            'https://servicex.slateci.net/drequest/status/LookedUp',
            verify=False)
        try:
            req = req_resp.json()
        except ValueError:
            print("Decoding request response failed. Cont.")
            time.sleep(10)
            continue
        if not req:
            continue
        # print(req)

        req_id = req['_id']
        branches = req['_source']['columns']

        # gets one file belonging to this request
        path_res = requests.get('https://servicex.slateci.net/dpath/' +
                                req_id + '/Created',
                                verify=False)
        try:
            pat = path_res.json()
        except ValueError:
            print("Decoding path response failed. Cont.")
            time.sleep(10)
            continue

        if not pat:
            continue
        # print(pat)

        # checks the file
        (valid, info) = validate_branches(pat['_source']['file_path'], branches)

        if valid:
            # sets all the files to "Validated"
            while True:
                path_res = requests.get('https://servicex.slateci.net/dpath/' +
                                        req_id + '/Created',
                                        verify=False)
                pat = path_res.json()
                if not pat:
                    break
                path_res = requests.put('https://servicex.slateci.net/dpath/status/' +
                                        pat['_id'] + '/Validated',
                                        verify=False)
                print('path: ' + pat['_id'] + ' validation: ' + str(path_res.status_code))
            # sets request to "Validated"
            requests.put('https://servicex.slateci.net/drequest/status/' +
                         req_id + '/Validated/' + info,
                         verify=False)

            create_kafka_topic(ADMIN, req_id)

        else:
            # fails all files
            while True:
                path_res = requests.get('https://servicex.slateci.net/dpath/' +
                                        req_id + '/Created',
                                        verify=False)
                pat = path_res.json()
                if not pat:
                    break
                path_res = requests.put('https://servicex.slateci.net/dpath/status/' +
                                        pat['_id'] + '/Failed',
                                        verify=False)
                print('path: ' + pat['_id'] + ' failing: ' + str(path_res.status_code))
            # sets request to "Failed"
            requests.put('https://servicex.slateci.net/drequest/status/' +
                         req_id + '/Failed/' + info,
                         verify=False)
