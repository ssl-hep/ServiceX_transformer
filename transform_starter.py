#!/usr/bin/env python

import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import ROOT
import numpy as np
ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')


with open('config/config.json') as json_file:
    conf = json.load(json_file)

print('configuration:\n', conf)

print('sleeping until CAs are there...')

time.sleep(60)

es = Elasticsearch([conf['ES_HOST']], timeout=60)

while True:
    res = es.search(index="servicex", body={"size": 1, "query": {"match": {"status": "Prescreened"}}})
    if res['hits']['total']:
        print("Got %d Hits:" % res['hits']['total'])
    else:
        time.sleep(10)
        continue

    hit = res['hits']['hits'][0]
    print(hit)

    print('lookin up the first file.')
    ff = es.search(index="servicex_paths", body={"size": 1, "query": {"match": {"req_id": hit['_id']}}})
    if res['hits']['total']:
        print("Got %d files:" % res['hits']['total'])
    else:
        print("Serious issue: Could not get the path to a file.")
        continue
    fh = ff['hits']['hits'][0]
    print(fh)
