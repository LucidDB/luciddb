#!/bin/bash
# $Id$
# Farrago is an extensible data management system.
# Copyright (C) 2005-2005 The Eigenbase Project
# Copyright (C) 2005-2005 Disruptive Tech
# Copyright (C) 2005-2005 Red Square, Inc.
# Portions Copyright (C) 2003-2005 John V. Sichi
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later Eigenbase-approved version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA

# Script used to create an "installable" Farrago build.

# NOTE jvs 13-Mar-2005:  We can't use ant to do most of this, because
# it has poor support for symlinks and file permissions.

usage() {
    echo "Usage:  distBuild.sh [--skip-init-build]"
}

if [ ! -e dist/FarragoRelease.properties ]; then
    echo "Error:  You must create file dist/FarragoRelease.properties first."
    echo "See dist/ExampleRelease.properties for a template."
    exit -1
fi

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

# set up directories
OPEN_DIR=$(cd ..; pwd)
FARRAGO_DIR=$OPEN_DIR/farrago
DIST_DIR=$FARRAGO_DIR/dist
FENNEL_DIR=$OPEN_DIR/fennel
THIRDPARTY_DIR=$OPEN_DIR/thirdparty

# create staging directory
TMP_DIR=$DIST_DIR/tmp
rm -rf $TMP_DIR
mkdir $TMP_DIR

# get help from ant to figure out where to build release image
cd $DIST_DIR
ant createReleaseDir

# derive staging sub-directories
RELEASE_DIR=`echo $TMP_DIR/*`
LIB_DIR=$RELEASE_DIR/lib
INSTALL_DIR=$RELEASE_DIR/install
CATALOG_DIR=$RELEASE_DIR/catalog
BIN_DIR=$RELEASE_DIR/bin

# create staging sub-directories
mkdir $LIB_DIR
mkdir $LIB_DIR/plugin
mkdir $LIB_DIR/mdrlibs
mkdir $LIB_DIR/fennel
mkdir $INSTALL_DIR
mkdir $CATALOG_DIR
mkdir $CATALOG_DIR/fennel
mkdir $BIN_DIR

# copy thirdparty libs
cd $THIRDPARTY_DIR
cp janino/lib/janino.jar $LIB_DIR
cp janino/src/org/codehaus/janino/doc-files/new_bsd_license.txt $LIB_DIR/janino.license.txt
cp mondrian-resource.jar $LIB_DIR
cp mondrian-xom.jar $LIB_DIR
cp LICENSE.mondrian $LIB_DIR/mondrian.license.txt
cp mdrlibs/* $LIB_DIR/mdrlibs
cp OpenJava/openjava.jar $LIB_DIR
cp OpenJava/COPYRIGHT $LIB_DIR/openjava.license.txt
cp RmiJdbc/dist/lib/*.jar $LIB_DIR
cp csvjdbc/csvjdbc.jar $LIB_DIR
cp csvjdbc/license.txt $LIB_DIR/csvjdbc.license.txt
cp sqlline.jar $LIB_DIR
cp sqlline/LICENSE $LIB_DIR/sqlline.license
cp jline.jar $LIB_DIR
cp hsqldb/doc/hypersonic_lic.txt $LIB_DIR/hsqldb.license.txt
cp hsqldb/lib/hsqldb.jar $LIB_DIR
cp -d stlport/lib/lib*.so* $LIB_DIR/fennel
cp stlport/README $LIB_DIR/fennel/stlport.README.txt
cp -d boost/lib/lib*.so* $LIB_DIR/fennel
cp boost/LICENSE_1_0.txt $LIB_DIR/fennel/boost.license.txt

# TODO jvs 12-Mar-2005
# cp -d icu/lib/lib*.so* $LIB_DIR/fennel
# cp icu/license.html $LIB_DIR/fennel/icu.license.html

# copy fennel libs
cd $FENNEL_DIR
cp -d libfennel/.libs/lib*.so* $LIB_DIR/fennel
cp -d farrago/.libs/lib*.so* $LIB_DIR/fennel
cp -d disruptivetech/libfennel_dt/.libs/lib*.so* $LIB_DIR/fennel
cp -d disruptivetech/farrago/.libs/lib*.so* $LIB_DIR/fennel
cp -d redsquare/libfennel_rs/.libs/lib*.so* $LIB_DIR/fennel
cp -d redsquare/farrago/.libs/lib*.so* $LIB_DIR/fennel

# copy fennel resources
cp common/*.properties $CATALOG_DIR/fennel

# create farrago libs
cd $DIST_DIR
ant jar

# copy farrago libs
cd $FARRAGO_DIR
cp COPYING $RELEASE_DIR
if [ -e dist/VERSION ]; then
    cp dist/VERSION $RELEASE_DIR
fi
cp dist/farrago.jar $LIB_DIR
cp dist/plugin/*.jar $LIB_DIR/plugin

# copy other farrago artifacts
cp dist/install/* $INSTALL_DIR
cp catalog/FarragoCatalog.* $CATALOG_DIR
cp catalog/ReposStorage.properties $CATALOG_DIR
cp catalog/*.dat $CATALOG_DIR
cp dist/bin/* $BIN_DIR

# tar the whole thing up
cd $TMP_DIR
tar cv * | bzip2 -c >../farrago.tar.bz2

rm -rf $TMP_DIR
