#!/bin/bash
# Make a distribution and send it to the Sourceforge project
# server via the perfecthash shell account.

source ./defineVariables.sh

set -e
set -v

cd ..
rm -rf web/doxygen/html
make dist
scp ${DIST_TARBALL} perfecthash@shell.sf.net:/home/groups/f/fe/fennel
