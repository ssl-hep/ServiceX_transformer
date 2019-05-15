#!/bin/bash
# Filename: xaod_branches.sh

# Set up the environment
source /home/atlas/release_setup.sh
# echo '{gROOT->Macro("$ROOTCOREDIR/scripts/load_packages.C");}' > rootlogon.C

# Get input ROOT file and branches
while getopts f:b:a option; do
    case "${option}" in
        f) file=${OPTARG};;
        b) branch=${OPTARG};;
        a) set -f   # Disable glob
           IFS=','   # Split on comma characters
           attr_array=($OPTARG);;   # Use the split+glob operator
    esac
done

if [[ -z $file ]]; then
    file="/xaodFiles/AOD.11182705._000001.pool.root.1"
fi
if [[ -z $branch ]]; then
    branch="Electrons"
fi
if [[ -z $attr_array ]]; then
    attr_array=("Electrons.pt()" "Electrons.eta()" "Electrons.phi()" "Electrons.e()" "Muons.pt()" "Muons.eta()" "Muons.phi()" "Muons.e()")
fi



print_branches () {
    # Print xAOD branches to temporary file temp.txt
    python -c "import xaod_branches; xaod_branches.print_branches(\"$file\", \"$branch\")"
    
    # Search the file for the branch name, type, and size
    >| xaod_branches.txt
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
            echo "{" >> xaod_branches.txt
            echo "    \"branchName\": \"$name\"," >> xaod_branches.txt
            echo "    \"branchType\": \"$type\"," >> xaod_branches.txt
            echo "    \"branchSize\": $size" >> xaod_branches.txt
            echo "}" >> xaod_branches.txt
            # echo "$name $type $size" >> xaod_branches.txt
        fi
    done < temp.txt

    rm temp.txt
    
    # Do whatever needs to be done with the output json
}



write_branches_to_ntuple () {
    attr_list="["
    for i in "${attr_array[@]}"; do
        attr_list="${attr_list}\"${i}\", "
    done
    attr_list="${attr_list}]"

    python -c "import xaod_branches; xaod_branches.write_branches_to_ntuple(\"$file\", $attr_list)"
    
    # Do whatever needs to be done with the output flat ntuple
}



print_branches
write_branches_to_ntuple
