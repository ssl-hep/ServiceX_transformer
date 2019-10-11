#!/usr/bin/env python

# this code gets requests in state: Created, Validates request on one file
# if request valid (all branches exist) it sets request state to Defined
# if not it sets state to Failed, deletes all the paths belonging to that request.
import os

import datetime
import json
import sys

import time
import uproot
import awkward
import requests
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import argparse
import pika

# What is the largest message we want to send (in megabytes).
# Note this must be less than the kafka broker setting if we are using kafka
default_max_message_size = 14.5

default_attr_names = "Electron_pt,Electron_eta,Muon_phi"

parser = argparse.ArgumentParser(
    description='Validate a request and create kafka topic.')

parser.add_argument('--rabbit-uri', dest="rabbit_uri", action='store',
                    default='host.docker.internal')

parser.add_argument('--avg-bytes', dest="avg_bytes_per_column", action='store',
                    help='Average number of bytes per column per event',
                    default='40')

parser.add_argument("--path", dest='path', action='store',
                    default=None,
                    help='Path to single Root file to transform')

parser.add_argument("--tree", dest='tree', action='store',
                    default="Events",
                    help='Tree from which columns will be inspected')

parser.add_argument("--attrs", dest='attr_names', action='store',
                    default=default_attr_names,
                    help='List of attributes to extract')


def parse_column_name(attr):
    attr_parts = attr.split('.')
    tree_name = attr_parts[0]
    branch_name = '.'.join(attr_parts[1:])
    return tree_name, branch_name


def validate_branches(file_name, tree_name, column_names):
    print("Validating file: " + file_name + " inside tree " + tree_name)
    file_in = uproot.open(file_name)

    estimated_size = int(args.avg_bytes_per_column) * len(column_names)

    # if tree_name not in file_in.keys(cycle='None'):
    #     return False, "Could not find tree {} in file".format(tree_name)
    try:
        tree = file_in[tree_name]
        print(tree.keys())
        for column in column_names:
            if column not in tree.keys():
                return False, "No branch with name: {} in {} Tree".\
                    format(column, tree_name)
    except KeyError as key_error:
        return False, "Could not find tree {} in file".format(key_error)

    return(True, {
        "max-event-size": estimated_size
    })


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

    if 'tree-name' in validation_request:
        tree_name = validation_request['tree-name']
    else:
        tree_name = None

    service_endpoint = validation_request[u'service-endpoint']
    post_status_update(service_endpoint,
                       "Validation Request received")

    # checks the file
    (valid, info) = validate_branches(
        validation_request[u'file-path'], tree_name, columns
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

    if args.path:
        tree_name = args.tree
        attrs = args.attr_names.split(",")
        # checks the file
        (valid, info) = validate_branches(args.path, tree_name, attrs)
        print(valid, info)
        sys.exit(0)

    rabbitmq = pika.BlockingConnection(
        pika.URLParameters(args.rabbit_uri)
    )
    _channel = rabbitmq.channel()
    _channel.queue_declare('validated_requests')

    _channel.basic_consume(queue="validation_requests",
                           auto_ack=False,
                           on_message_callback=callback)
    _channel.start_consuming()
