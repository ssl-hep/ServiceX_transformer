#!/bin/bash

# Set up the environment - this sets up python 2.7
source /home/atlas/release_setup.sh

pip install --user requests
pip install --user elasticsearch

python transform_starter.py