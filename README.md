# ServiceX_transformer Repo for input-output transformation code for ServiceX

[![Build Status](https://travis-ci.org/ssl-hep/ServiceX_transformer.svg?branch=pytest)](https://travis-ci.org/ssl-hep/ServiceX_transformer)
[![codecov](https://codecov.io/gh/ssl-hep/ServiceX_transformer/branch/master/graph/badge.svg)](https://codecov.io/gh/ssl-hep/ServiceX_transformer)


Allows user to access an xAOD-formatted TTree in a ROOT file via a standalone
ATLAS analysis release. This is based a Docker image found here:
    https://hub.docker.com/r/atlas/analysisbase
    
The transformer accepts a list of columns (Root Branches) to extract from the 
file along with Root files. It extracts the data into awkward arrays and 
publishes arrow buffers to a backend messaging system.

# How to Build
Build the docker image as:
```bash
docker build -t sslhep/servicex-transformer:latest .
```

# How to Run
The container is designed to run inside a kubernetes cluster as part of the 
serviceX application. It can also be run standalone as a CLI utility for 
transforming ROOT files.

You can open up a shell into the container with

```bash
%  docker run -it sslhep/servicex-transformer bash         
```

## xaod_branches.py
This is the main script for the transformer. It has several command line options
that control its operation. It can be used in the context of the serviceX 
application where it will request files to transform from the serviceX REST api.
Alternativly, it can be fed files to transform via command line arguments.

### Obtain Requests from ServiceX REST API
By default, the transformer will contact ServiceX at `servicex.slateci.net`
to request the next file to transform. If no files are available, the 
script will poll, waiting 10 seconds between requests.

### Obtain Requests from Command Line Arguments
For testing and benchmarking, it is often convenient to supply files from the
command line. There are two mechanisms for that:

1. Supply a path to the root file in the command line
2. Provide a JSON file with output from the DID finder with paths to each file
in a dataset

See the argument reference for the exact paramters for each of these options

### Messaging Backend
The transformer writes the generated awkward arrays to a messaging backend for
delivery to analysis applications. You can choose between:
* Kafka - for a fully cached, resource intensive topic as output
* Object-Store - For saving modest result sets that can be downloaded

These can be selected from command line options. There are numerous options for
each backend.

### Command Line Reference
|Option| Description | Default |
| ------ | ----------- | ------- |
| --servicex SERVICEX_ENDPOINT | Endpoint for servicex REST API | servicex.slateci.net  |
| --dataset DATASET | Path to JSON Dataset document from DID Finder with collection of ROOT Files to process | |
| --path PATH | Path to single Root file to transform | |
| --max-message-size | Maximum size for any message in Megabytes | 14.5 Mb |
| --chunks CHUNKS | Number of events to include in each message. If ommitted, it will compute a best guess based on heuristics and max message size | None |
| --attrs ATTR_NAMES | List of attributes to extract | Electrons.pt(), Electrons.eta(), Electrons.phi(), Electrons.e()|
| --limit LIMIT | Max number of events to process | |
| --result-destination | Where to send the results: kafka or object-store | kafka
| --result-format | Binary format for the results: arrow or parquet | arrow
| --topic TOPIC | Kafka topic to publish arrays to | servicex |
| --brokerlist BROKERLIST | List of Kafka broker to connect to | servicex-kafka-0.slateci.net:19092, servicex-kafka-1.slateci.net:19092, servicex-kafka-2.slateci.net:19092" |                      

## Development
There are several command line options available for exercising the service
in a development environment. Since the service relies on some specific Atlas
tooling we usually execute inside the container. 

To build a local image:
```bash
docker build -t docker build -t sslhep/servicex-transformer:rabbitmq .
```

For ease, we often will launch a bash shell with this repo's source code
mounted as a volume. You can edit the source code using your desktop's tools
and execute inside the container.

To launch this container, cd to root of this repo.

```bash
docker run -it \
    --mount type=bind,source=$(pwd),target=/code \
    --mount type=bind,source=$(pwd)/../data,target=/data \
    --mount type=volume,source=x509,target=/etc/grid-security-ro \
    sslhep/servicex-transformer:latest bash
```

This assumes that you have a directory above this repo called `data` that has 
some sample ROOT files.

To run the transformer from the command line you can `cd /code` to move
to the copy of the code served up from your host.

```bash
python xaod_branches.py --path /data/AOD.11182705._000001.pool.root.1 --result-destination kafka --request-id my-bucket --chunks 5000
```

If you want to run it from the command line and use the object storage:
```bash
export MINIO_ACCESS_KEY='miniouser'
export MINIO_SECRET_KEY='leftfoot1'
export MINIO_URL='host.docker.internal:9000'
python xaod_branches.py --path /data/AOD.11182705._000001.pool.root.1 --result-destination object-store --result-format parquet --request-id my-bucket --chunks 5000
```

### To Debug RabbitMQ interactions
It's possible to run the transformer from the command line and have it interact
with RabbitMQ deployed into the local kubernetes cluster.

1. Turn off the App's transformer manager
- Run a local copy of the ServiceX_App on port 5000
- In app.conf set `TRANSFORMER_MANAGER_ENABLED = False`
- Submit a transform request
- Inside the container run transformer against the rabbit mq topic
```bash
python xaod_branches.py --rabbit-uri amqp://user:leftfoot1@host.docker.internal:30672/%2F?heartbeat=9000 --result-destination kafka --request-id 52b40f91-103c-4c64-b732-a7ac96992682 --chunks 5000

```
## Debuggging Kafka
We have a debugging pod deployed to the cluster that has some handy tools.
You can get a shell into this pod with the command:
```bash
% kubectl -n kafka exec -it testclient bash
```

From here you can:
1. Delete the topic to clear out old data 
```bash
% bin/kafka-topics.sh --bootstrap-server servicex-kafka.kafka.svc.cluster.local:9092  --delete --topic servicex 
```
2. Follow a topic to print the binary (opaque) data to the console:
```bash
% bin/kafka-console-consumer.sh --bootstrap-server servicex-kafka:9092 --topic servicex
```
3. See the size of each partition in the topic:
```bash
%  bin/kafka-log-dirs.sh --describe --bootstrap-server servicex-kafka-0.slateci.net:19092 --topic-list servicex | grep '{' | jq '.brokers[].logDirs[].partitions[].size' | grep '^[^0]'
```

## Running Tests
Validation of the code logic is performed using 
[pytest](https://docs.pytest.org/en/latest/) and 
[pytest-mock](https://github.com/pytest-dev/pytest-mock). Unit test fixtures are
in `test` directories inside each package. The tests depend on imports that 
are found inside the atlas docker container, so they are usually run inside 
a container. 

Start up the container and run unit tests with:
```bash
 docker run sslhep/servicex-transformer:rabbitmq bash -c "source /home/atlas/.bashrc && pytest -s"
```

The tests are instrumented with code coverage reporting via 
[codecov](https://codecov.io/gh/ssl-hep/ServiceX_transformer). The travis
job has a the codecov upload token set as an environment variable which is
passed into the docker container so the report can be uploaded upon successful
conclusion of the tests.

## Coding Standards
To make it easier for multiple people to work on the codebase, we enforce PEP8
standards, verified by flake8. The community has found that the 80 character
limit is a bit awkward, so we have a local config setting the `max_line_length`
to 99.

