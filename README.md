# ServiceX_transformer
Repo for input-output transformation code for ServiceX

Allows user to access an xAOD-formatted TTree in a ROOT file via a standalone ATLAS analysis release. This requires a Docker image found here:

    https://hub.docker.com/r/atlas/analysisbase

First pull the Docker image:

    $ docker pull atlas/analysisbase

and mount this location into the image when you start it:

    $ docker run -v $PWD:/home/xaodFiles -it atlas/analysisbase:21.2.14

then run the script via:

    $ ../xaodFiles/printXaodBranches.sh
