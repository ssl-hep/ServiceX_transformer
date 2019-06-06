# ServiceX_transformer Repo for input-output transformation code for ServiceX

Allows user to access an xAOD-formatted TTree in a ROOT file via a standalone
ATLAS analysis release. This is based a Docker image found here:
    https://hub.docker.com/r/atlas/analysisbase

# How to Build
Build the docker image as:
```bash
docker build -t sslhep/servicex-transformer:latest .
```

# How to Run
The kubernetes deployment for this service can be found in 
kube/transformer.yaml

When the pod is created it creates two containers. One does something with the
X509 certificate. The other runs the transformer script which will wait for
file names to appear in elasticsearch, transform them and publish to kafka.

The script has hardcoded reference to the kafka cluster running in our GKE 
deployment as well as a topic name of servicex.

## Kafka Operations
We have a debugging pod deployed to the cluster that has some handy tools.
You can get a shell into this pod with the command:
```bash
% kubectl -n kafka exec -it testclient bash
```

From here you can:
1. Delete the topic to clear out old data 
```bash
% bin/kafka-topics.sh --delete --topic servicex --bootstrap-server servicex-kafka.kafka.svc.cluster.local:9092
```
2. Follow a topic to print the binary (opaque) data to the console:
```bash
% bin/kafka-console-consumer.sh --bootstrap-server servicex-kafka:9092 --topic servicex
```
3. See the size of the topic:
```bash
% bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list servicex-kafka:9092 --topic servicex --time -1 --offsets 1
```
