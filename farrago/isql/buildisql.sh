# /bin/bash
# $Id$

DEPLOY_DIR=../../../thirdparty/iSQLViewer

p4 edit $DEPLOY_DIR/isql-core.jar

export CVSROOT=:pserver:anonymous@cvs.sourceforge.net:/cvsroot/isql
cvs -z3 co .
cd isql-core
ant -Dlib.dir=$DEPLOY_DIR -Dapp.name=isql-core jar
cp isql-core.jar $DEPLOY_DIR
