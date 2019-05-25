FROM atlas/analysisbase:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

# analysisbase already sets user "atlas" so have to sudo everything

RUN sudo mkdir -p /etc/grid-security/certificates /etc/grid-security/vomsdir 

# needed to get x509 proxy to read the data
RUN sudo yum -y update

# this is for  centos7 but analysisbase is sl6

# RUN yum localinstall https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el7-release-latest.rpm -y
# RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm; \
#     curl -s -o /etc/pki/rpm-gpg/RPM-GPG-KEY-wlcg http://linuxsoft.cern.ch/wlcg/RPM-GPG-KEY-wlcg; \
#     curl -s -o /etc/yum.repos.d/wlcg-centos7.repo http://linuxsoft.cern.ch/wlcg/wlcg-centos7.repo; 

RUN sudo yum localinstall https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el6-release-latest.rpm -y

# epel comes preinstalled.
# RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm;

RUN sudo yum install -y voms fetch-crl 
ENV X509_USER_PROXY /etc/grid-security/x509up

# not needed. 
# RUN sudo yum install -y python34; \
#     sudo curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py; \
#     sudo python3 get-pip.py; \
#     sudo pip3 install elasticsearch;

# get back to atlas user
# RUN sudo su atlas

# Create app directory
WORKDIR /usr/src/app

COPY . .

USER atlas

# Install Python dependencies from pip
RUN source /home/atlas/release_setup.sh && \
     pip install -r requirements.txt --user

# COPY run_x509_updater.sh /.
# COPY transform_starter.py /.
# COPY xaod_branches.* /

# CMD xaod_branches.sh
