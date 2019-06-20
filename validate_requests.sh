#!/bin/bash

# Set up the environment
source /home/atlas/release_setup.sh
# echo '{gROOT->Macro("$ROOTCOREDIR/scripts/load_packages.C");}' > rootlogon.C

export PYTHONWARNINGS="ignore:Unverified HTTPS request"
python validate_requests.py

