#!/usr/bin/env python

# this code gets requests in state: Created, Validates request on one file
# if request valid (all branches exist) it sets request state to Defined
# if not it sets state to Failed, deletes all the paths belonging to that request.

import os
import ROOT
import requests
import time

ROOT.gROOT.Macro('$ROOTCOREDIR/scripts/load_packages.C')


def validate_branches(file_name, branch_names):
    print('validating file:', file_name, 'for branches:', branch_names)
    return True
    # file_in = ROOT.TFile.Open(file_name)
    # tree_in = ROOT.xAOD.MakeTransientTree(file_in)

    # ROOT.gSystem.RedirectOutput('temp.txt', 'w')
    # tree_in.Print()

    # # To print the attributes of a particle collection
    # ROOT.gSystem.RedirectOutput('xaod_branch_attributes.txt', 'w')
    # tree_in.GetEntry(0)
    # particles = getattr(tree_in, branch_name)
    # if particles.size() >= 1:
    #     for method_name in dir(particles.at(0)):
    #         print(method_name)

    # ROOT.gROOT.ProcessLine("gSystem->RedirectOutput(0);")


if __name__ == "__main__":
    while True:

        # gets request in Created
        req_resp = requests.get('https://servicex.slateci.net/drequest/status/LookedUp', verify=False)
        req = req_resp.json()
        if not req:
            time.sleep(10)
            continue
        print(req)

        req_id = req['_id']
        branches = req['_source']['columns']

        # gets one file belonging to this request
        path_res = requests.get('https://servicex.slateci.net/dpath/' + req_id + '/Created', verify=False)
        pat = path_res.json()
        if not pat:
            time.sleep(10)
            continue
        print(pat)

        # checks the file
        valid = validate_branches(pat['_source']['file_path'], branches)

        if valid:
            # sets all the files to "Validated"
            while True:
                path_res = requests.get('https://servicex.slateci.net/dpath/' + req_id + '/Created', verify=False)
                pat = path_res.json()
                if not pat:
                    break
                path_res = requests.put('https://servicex.slateci.net/dpath/status/' + pat['_id'] + '/Validated', verify=False)
                print('path:', pat['_id'], 'validation:', path_res.status_code)
            # sets request to "Validated"
            requests.put('https://servicex.slateci.net/drequest/status/' + req_id + '/Validated', verify=False)

        else:
            # deletes all files

            # sets request to "Failed"
            requests.put('https://servicex.slateci.net/drequest/status/' + req_id + '/Failed', verify=False)

        # if not rpath_output.text == 'false':
        #     _id = rpath_output.json()['_id']
        #     _file_path = rpath_output.json()['_source']['file_path']
        #     _request_id = rpath_output.json()['_source']['req_id']
        #     print("Received ID: " + _id + ", path: " + _file_path)

        #     request_output = requests.get('https://servicex.slateci.net/drequest/' + _request_id, verify=False)
        #     attr_name_list = request_output.json()['_source']['columns']
        #     print("Received request: " + _request_id + ", columns: " + str(attr_name_list))
