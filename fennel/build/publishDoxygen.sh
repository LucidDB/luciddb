#!/bin/bash
# Publish doxygen on Sourceforge web server.  Assumes pushDist has
# already been executed.

source ./defineVariables.sh

set -v
set -e

DOCROOT=/home/groups/f/fe/fennel/htdocs

ssh -T perfecthash@shell.sf.net <<EOF
set -e
set -v
cd ${DOCROOT}/..
rm -rf ${DIST_DIR}
tar -xzf ${DIST_TARBALL}
cd ${DIST_DIR}/fennel/web
doxygen fennel.doxycfg
rm -rf doxygen.old
touch ${DOCROOT}/doxygen
mv ${DOCROOT}/doxygen doxygen.old
mv doxygen ${DOCROOT}
rm -rf doxygen.old
EOF
