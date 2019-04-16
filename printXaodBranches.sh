#!/bin/bash
# Filename: printXaodBranches.sh

# docker run -v $PWD:/home/xaodFiles -it atlas/analysisbase:21.2.14

source /home/atlas/release_setup.sh
echo '{gROOT->Macro("$ROOTCOREDIR/scripts/load_packages.C");}' > rootlogon.C
root -b -q ../xaodFiles/printXaodBranches.C
grep '=' temp.txt | awk '{print $1;}' > xaodBranches.txt
rm temp.txt
