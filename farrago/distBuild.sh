#!/bin/bash
# $Id$

# Script used to create an "installable" Farrago build.

usage() {
    echo "Usage:  distBuild.sh [--skip-init-build]"
}


if [ -z "$1" ]; then
    ./initBuild.sh --with-fennel
elif [ "$1" == "--skip-init-build" ]; then
    echo "Skipping Farrago initial build."
else
    usage
    exit -1
fi

set -e
set -v

# set up some useful vars
OPEN_DIR=$(cd ..; pwd)
SAFFRON_DIR=$OPEN_DIR/saffron
FARRAGO_DIR=$OPEN_DIR/farrago
FENNEL_DIR=$OPEN_DIR/fennel
THIRDPARTY_DIR=$OPEN_DIR/thirdparty

TMP_DIR=$FARRAGO_DIR/dist/tmp
TMP_FAR_DIR=$TMP_DIR/farrago
LIB_DIR=$TMP_FAR_DIR/lib
INSTALL_DIR=$TMP_FAR_DIR/install
CATALOG_DIR=$TMP_FAR_DIR/catalog
BIN_DIR=$TMP_FAR_DIR/bin

# create dist tree in a tmp directory
rm -rf $TMP_DIR
mkdir $TMP_DIR
mkdir $TMP_FAR_DIR
mkdir $LIB_DIR
mkdir $LIB_DIR/plugin
mkdir $LIB_DIR/mdrlibs
mkdir $LIB_DIR/fennel
mkdir $INSTALL_DIR
mkdir $CATALOG_DIR
mkdir $CATALOG_DIR/fennel
mkdir $BIN_DIR

cd $SAFFRON_DIR
ant jar

# copy saffron libs
cp dist/saffron.jar $LIB_DIR
cp $THIRDPARTY_DIR/janino/lib/janino.jar $LIB_DIR
cp $THIRDPARTY_DIR/janino/src/net/janino/doc-files/lgpl.txt $LIB_DIR/janino.lgpl.txt
cp $THIRDPARTY_DIR/mondrian-resource.jar $LIB_DIR
cp $THIRDPARTY_DIR/mondrian-xom.jar $LIB_DIR
cp $THIRDPARTY_DIR/LICENSE.mondrian $LIB_DIR
cp $THIRDPARTY_DIR/junit/junit.jar $LIB_DIR
cp $THIRDPARTY_DIR/junit/cpl-v10.html $LIB_DIR/junit.license.html

# copy fennel libs
cd $FENNEL_DIR
cp -d libfennel/.libs/lib*.so* $LIB_DIR/fennel
cp -d farrago/.libs/lib*.so* $LIB_DIR/fennel
cp -d $THIRDPARTY_DIR/stlport/lib/lib*.so* $LIB_DIR/fennel
cp -d $THIRDPARTY_DIR/boost/lib/lib*.so* $LIB_DIR/fennel
cp -d $THIRDPARTY_DIR/icu/lib/lib*.so* $LIB_DIR/fennel

# copy fennel resources
cp common/*.properties $CATALOG_DIR/fennel


# copy farrago libs
cd $FARRAGO_DIR
ant jar

cp dist/farrago.jar $LIB_DIR
cp dist/plugin/*.jar $LIB_DIR/plugin
cp $THIRDPARTY_DIR/mdrlibs/* $LIB_DIR/mdrlibs
cp $THIRDPARTY_DIR/RmiJdbc/dist/lib/*.jar $LIB_DIR
cp $THIRDPARTY_DIR/csvjdbc/csvjdbc.jar $LIB_DIR
cp $THIRDPARTY_DIR/csvjdbc/license.txt $LIB_DIR/csvjdbc.license.txt
cp $THIRDPARTY_DIR/sqlline.jar $LIB_DIR
cp $THIRDPARTY_DIR/sqlline/LICENSE $LIB_DIR/sqlline.license
cp $THIRDPARTY_DIR/jline.jar $LIB_DIR

cp dist/install/* $INSTALL_DIR
cp catalog/FarragoCatalog.bt? $CATALOG_DIR
cp catalog/*.dat $CATALOG_DIR

cp dist/bin/* $BIN_DIR

# tar the whole thing up
cd $TMP_DIR
tar cv * | bzip2 -c >../farrago.tar.bz2

rm -rf $TMP_DIR
