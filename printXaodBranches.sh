#!/bin/bash
# Filename: printXaodBranches.sh

# Mount the current directory into the Docker image when it's run:
# docker run -v $PWD:/xaodFiles -it printxaodbranches

# Set up the environment
source /home/atlas/release_setup.sh
echo '{gROOT->Macro("$ROOTCOREDIR/scripts/load_packages.C");}' > rootlogon.C

# Get input ROOT file
while getopts f:b: option; do
    case "${option}" in
        f) file=${OPTARG};;
        b) branch=${OPTARG};;
    esac
done

# Run the ROOT script to print xAOD branches to a file
# root -b -q /home/atlas/servicex/printXaodBranches.C
if [[ -z $file ]]; then
    file="/xaodFiles/AOD.11182705._000001.pool.root.1"
fi
if [[ -z $branch ]]; then
    branch="Electrons"
fi
root -b -q "/xaodFiles/printXaodBranches.C(\"$file\", \"$branch\")"

# Search the file for the branch name and type
>| xaodBranches.txt
while read line; do
    name=""
    type=""
    size=""
    if [[ "$line" == *"Br"* ]]; then
        name=$(echo "$line" | awk '{print substr($3, 2)}')
        if [[ ! -z "$(echo \"$line\" | awk '{print $6}')" ]]; then
            type="$type$(echo \"$line\" | awk '{print $5}')"
        fi
      
        read nextLine
        if [[ "$nextLine" == *"|"* ]]; then
            type="$type$(echo \"$nextLine\" | awk '{print $3}')"
            read nextNextLine
            if [[ "$nextNextLine" ==  *"Total  Size="* ]]; then
                size=$(echo "$nextNextLine" | awk '{print $7}')
            fi
        elif [[ "$nextLine" == *"Total  Size="* ]]; then
            size=$(echo "$nextLine" | awk '{print $7}')
        fi
    fi

    if [[ $type == ":"* ]]; then
        type="DataVector<xAOD:$type"
    fi

    if [[ ! -z $name ]]; then
        echo "{" >> xaodBranches.txt
        echo "    \"branchName\": \"$name\"," >> xaodBranches.txt
        echo "    \"branchType\": \"$type\"," >> xaodBranches.txt
        echo "    \"branchSize\": $size" >> xaodBranches.txt
        echo "}" >> xaodBranches.txt
        # echo "$name $type $size" >> xaodBranches.txt
    fi
done < temp.txt

rm temp.txt
# cat xaodBranches.txt
