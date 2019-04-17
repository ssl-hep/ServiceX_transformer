#!/bin/bash
# Filename: printXaodBranches.sh

# docker run -v $PWD:/xaodFiles -it printxaodbranches

source /home/atlas/release_setup.sh
echo '{gROOT->Macro("$ROOTCOREDIR/scripts/load_packages.C");}' > rootlogon.C
root -b -q /home/atlas/servicex/printXaodBranches.C
grep '=' temp.txt | awk '{print $1;}' > xaodBranches.txt
rm temp.txt
cat xaodBranches.txt
