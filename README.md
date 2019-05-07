# ServiceX_transformer Repo for input-output transformation code for ServiceX

Allows user to access an xAOD-formatted TTree in a ROOT file via a standalone
ATLAS analysis release. This is based a Docker image found here:
    https://hub.docker.com/r/atlas/analysisbase

# How to Build
Build the docker image as:
```bash
docker build -t servicex_transform .
```

# How to Run
The script assumes that there is a mounted docker volume under `/data` - it will
read ROOT files from this directory and output the flattened n-tuple file as
well as the list of branches in this directory.

The script accepts a number of command line arguments:

| Argument | Description | Default |
| -------- | ----------- | ------- |
| f        | Path to ROOT File | None |
| b        | Branch name | Electrons |
| a        | List of attributes to read | _pt, eta, phi, e_0 |

You can invoke the script from the command line as:
```bash
docker run --rm -v <<path to my xaod files>>:/data servicex_transform <<args>>
```

When complete, your flattned Root file will be in the mounted directory with
the name `flat_file.root` and all of the branches will be listed in a file
called `xaodBranches.txt`
