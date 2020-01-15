# ServiceX_transformer Repo for input-output transformation code for ServiceX

[![Build Status](https://travis-ci.org/ssl-hep/ServiceX_transformer.svg?branch=pytest)](https://travis-ci.org/ssl-hep/ServiceX_transformer)
[![codecov](https://codecov.io/gh/ssl-hep/ServiceX_transformer/branch/master/graph/badge.svg)](https://codecov.io/gh/ssl-hep/ServiceX_transformer)

Library of common classes for building serviceX transformers. Also contains
code for legacy transformer images.

To use this library:
```bash
pip install servicex-transformer
```
Allows a user to extract columns from a Root file. There are two supported 
transformers available in this repo:
1. xaod_transformer - Uses pyroot to extract data from xAOD files
2. uproot_transformer - Uses uproot to pull data out of a tree in a flat
Root file.

Both of these are based a Docker image found here:
    https://hub.docker.com/r/atlas/analysisbase
    
The transformer accepts a list of columns (Root Branches) to extract from the 
file along with Root files. It extracts the data into awkward arrays and 
publishes arrow buffers to a backend messaging system.

# How to Build
The base docker image can be built as:
```bash
docker build -t sslhep/servicex-transformer-base:develop .
```

Once you have this image, you can build the two variations as
```bash
docker build --build-arg BASE_VERSION=develop -f Dockerfile.uprootTransformer -t sslhep/servicex-transformer-uproot:develop .
docker build --build-arg BASE_VERSION=develop -f Dockerfile.xAODTransformer -t sslhep/servicex-transformer-xaod:develop .
```
# How to Run
The container is designed to run inside a kubernetes cluster as part of the 
serviceX application. It can also be run standalone as a CLI utility for 
transforming ROOT files.


You can launch a container with an X509 proxy mounted in a docker volume as:

```bash
docker run --rm \
    --mount type=bind,source=$HOME/.globus,readonly,target=/etc/grid-certs \
    --mount type=bind,source="$(pwd)"/secrets/secrets.txt,target=/servicex/secrets.txt \
    --mount type=volume,source=x509,target=/etc/grid-security \
    --name=x509-secrets sslhep/x509-secrets:latest
    

docker run --rm -it \
    --mount type=volume,source=x509,target=/etc/grid-security-ro \
    sslhep/servicex-transformer:develop bash  
```

## Transformer Scripts
Both transformer scripts are included in the container:
* `uproot_transformer.py`
* `xaod_transformer.py`

Both of them have a common set of command line arguments. The scripts are 
usually launched by the ServiceX Transformer Manager as job where each 
instance of the transformer pulls requests for ROOT files from a RabbitMQ.

The scripts can also be called from the command line with a path to a 
Root file.
 
# Transformed Result Output
Command line arguments determine a destination for the results as well as 
an output format.

* Kafka - Streaming system. Write messages formatted as Arrow tables. The 
`chunks` parameter determines how many events are included in each message.
* Object Store - Each transformed file is written as an object to an
S3 compatible object store. The only currently supported output file
format is parquet. The objects are stored in a bucket named after the 
transformation request ID.


### Command Line Reference
|Option| Description | Default |
| ------ | ----------- | ------- |
| --brokerlist BROKERLIST | List of Kafka broker to connect to if streaming is selected | servicex-kafka-0.slateci.net:19092, servicex-kafka-1.slateci.net:19092, servicex-kafka-2.slateci.net:19092" |
| --topic TOPIC | Kafka topic to publish arrays to | servicex |   
| --chunks CHUNKS | Number of events to include in each message. If ommitted, it will compute a best guess based on heuristics and max message size | None |                   
| --tree TREE | Root Tree to extract data from. Only valid for uproot transformer | Events
| --attrs ATTR_NAMES | List of attributes to extract | Electrons.pt(), Electrons.eta(), Electrons.phi(), Electrons.e()|
| --path PATH | Path to single Root file to transform. Any file path readable by xrootd | |
| --limit LIMIT | Max number of events to process | |
| --result-destination DEST| Where to send the results: kafka or object-store | kafka
| --result-format | Binary format for the results: arrow or parquet | arrow
| --max-message-size | Maximum size for any message in Megabytes | 14.5 Mb |
| --rabbit-uri URI | RabbitMQ Connection URI | host.docker.internal |
| --request-id GUID| ID associated with this transformation request. Used as RabbitMQ Topic Name as well as object-store bucket | servicex


## Development

For ease, we often will launch a bash shell with this repo's source code
mounted as a volume. You can edit the source code using your desktop's tools
and execute inside the container.

To launch this container, cd to root of this repo.

```bash
docker run -it \
    --mount type=bind,source=$(pwd),target=/code \
    --mount type=bind,source=$(pwd)/../data,target=/data \
    --mount type=volume,source=x509,target=/etc/grid-security-ro \
    sslhep/servicex-transformer:develop bash
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

