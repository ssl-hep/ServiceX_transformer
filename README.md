# ServiceX_transformer Repo for input-output transformation code for ServiceX

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
streaming to analysis applications. You can choose between:
* Kafka - for a fully cached, resource intensive topic as output
* Redis - for a size constrained queue which will reduce output in response to 
back pressure from the reading application

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
| --wait WAIT | Number of seconds to wait for consumer to restart | 600 seconds|
| --kafka | Use Kafka Backend for messages | False |
| --topic TOPIC | Kafka topic to publish arrays to | servicex |
| --brokerlist BROKERLIST | List of Kafka broker to connect to | servicex-kafka-0.slateci.net:19092, servicex-kafka-1.slateci.net:19092, servicex-kafka-2.slateci.net:19092" |                      
| --redis| Use Redis Backend for messages | False |
| --redis-host REDIS_HOST | Host for redis messaging backend | redis.slateci.net |
| --redis-port REDIS_PORT | Port for redis messaging backend | 6379 |


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
