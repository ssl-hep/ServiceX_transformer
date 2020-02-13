# ServiceX_transformer Library

![CI/CD](https://github.com/ssl-hep/ServiceX_transformer/workflows/CI/CD/badge.svg)
[![codecov](https://codecov.io/gh/ssl-hep/ServiceX_transformer/branch/master/graph/badge.svg)](https://codecov.io/gh/ssl-hep/ServiceX_transformer)

Library of common classes for building serviceX transformers. 

## Minimum Requiremnts
Works with Python version 2.7 and above

## Download from PyPi
To use this library:
```bash
pip install servicex-transformer
```

## Standard Command Line Arguments
This library provides a subclass of ArgParse for standardizing commnand line 
arguments for all transformer implementations.

Available arguments are:

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
| --path PATH | Path to single Root file to transform. Any file path readable by xrootd | |
| --limit LIMIT | Max number of events to process | |
| --result-destination DEST| Where to send the results: kafka or object-store, output-dir | kafka
| --output-dir | Local directory where the result will be written. Use this to run standalone without other serviceX infrastructure | None 
| --result-format | Binary format for the results: arrow, parquet, or root-file | arrow
| --max-message-size | Maximum size for any message in Megabytes | 14.5 Mb |
| --rabbit-uri URI | RabbitMQ Connection URI | host.docker.internal |
| --request-id GUID| ID associated with this transformation request. Used as RabbitMQ Topic Name as well as object-store bucket | servicex


## Running Tests
Validation of the code logic is performed using 
[pytest](https://docs.pytest.org/en/latest/) and 
[pytest-mock](https://github.com/pytest-dev/pytest-mock). Unit test fixtures are
in `test` directories inside each package. 


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

