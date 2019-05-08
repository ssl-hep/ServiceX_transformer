# ServiceX_transformer Repo for input-output transformation code for ServiceX

Allows user to access an xAOD-formatted TTree in a ROOT file via a standalone
ATLAS analysis release. This is based a Docker image found here:
    https://hub.docker.com/r/atlas/analysisbase

# How to Build
Build the docker image as:
```bash
docker build -t printxaodbranches .
```

# How to Run
The script in the image assumes that there is a file available in the
`/xaodFiles` directory called `AOD.11182705._000001.pool.root.1`. We will mount
this directory in the running container.

```bash
docker run -v <<path to my xaod files>>:/xaodFiles printxaodbranches
```

The CMD for this Dockerfile will run the script which will output the
branch names from the Root file.
 
