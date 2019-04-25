FROM atlas/analysisbase:latest


# analysisbase already sets user "atlas" so all these yum will have to do sudo.
# but analysisbase seems to be very old sl6. we better move away from it.

# # needed to get x509 proxy to read the data
# RUN yum -y update
# RUN yum localinstall https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el7-release-latest.rpm -y

# RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm; \
#     curl -s -o /etc/pki/rpm-gpg/RPM-GPG-KEY-wlcg http://linuxsoft.cern.ch/wlcg/RPM-GPG-KEY-wlcg; \
#     curl -s -o /etc/yum.repos.d/wlcg-centos7.repo http://linuxsoft.cern.ch/wlcg/wlcg-centos7.repo; 

# RUN yum install -y voms fetch-crl 
# ENV X509_USER_PROXY /etc/grid-security/x509up


COPY run_x509_updater.sh /.
COPY transform_starter.py /.

COPY printXaodBranches.* /
CMD /printXaodBranches.sh
