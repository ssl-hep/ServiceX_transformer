import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

with open('config/config.json') as json_file:
    conf = json.load(json_file)

print('configuration:\n', conf)

print('sleeping until CAs are there...')

time.sleep(60)

es = Elasticsearch([conf['ES_HOST']], timeout=60)

while True:
    res = es.search(index="servicex", body={"query": {"match": {"status": "Prescreened"}}})
    if res['hits']['total']:
        print("Got %d Hits:" % res['hits']['total'])

    time.sleep(10)
