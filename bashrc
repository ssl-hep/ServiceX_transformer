# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
        . /etc/bashrc
fi

# Initialize the Atlas software release
source /home/atlas/release_setup.sh
export PATH=$PATH:/home/atlas/.local/bin
export X509_USER_PROXY=/etc/grid-security/x509up

