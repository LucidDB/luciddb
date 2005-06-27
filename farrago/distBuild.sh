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

# Detect platform
cygwin=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
esac

if [ $cygwin = "true" ]; then
    SO_3P_PATTERN="lib*.dll*"
    SO_PATTERN="cyg*.dll*"
else
    SO_3P_PATTERN="lib*.so*"
    SO_PATTERN=$SO_3P_PATTERN
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
ISQL_DIR=$RELEASE_DIR/isql
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
mkdir $ISQL_DIR

# copy thirdparty libs
cd $THIRDPARTY_DIR
cp janino/lib/janino.jar $LIB_DIR
cp janino/src/org/codehaus/janino/doc-files/new_bsd_license.txt $LIB_DIR/janino.license.txt
cp resgen/lib/eigenbase-resgen.jar $LIB_DIR
cp resgen/lib/eigenbase-xom.jar $LIB_DIR
cp resgen/COPYING $LIB_DIR/resgen.license.txt
cp mdrlibs/* $LIB_DIR/mdrlibs
rm -f $LIB_DIR/mdrlibs/uml*.jar
rm -f $LIB_DIR/mdrlibs/mdrant.jar
cp OpenJava/openjava.jar $LIB_DIR
cp OpenJava/COPYRIGHT $LIB_DIR/openjava.license.txt
cp RmiJdbc/dist/lib/*.jar $LIB_DIR
cp csvjdbc/csvjdbc.jar $LIB_DIR
cp csvjdbc/license.txt $LIB_DIR/csvjdbc.license.txt
cp sqlline.jar $LIB_DIR
cp sqlline/LICENSE $LIB_DIR/sqlline.license
cp jline.jar $LIB_DIR
cp jgrapht/jgrapht-*.jar $LIB_DIR
cp jgrapht/license-LGPL.txt $LIB_DIR/jgrapht.license.txt
cp hsqldb/doc/hypersonic_lic.txt $LIB_DIR/hsqldb.license.txt
cp hsqldb/lib/hsqldb.jar $LIB_DIR
cp -d stlport/lib/$SO_3P_PATTERN $LIB_DIR/fennel
rm -f $LIB_DIR/fennel/*debug*
cp stlport/README $LIB_DIR/fennel/stlport.README.txt
cp -d boost/lib/$SO_3P_PATTERN $LIB_DIR/fennel
rm -f $LIB_DIR/fennel/*gdp*
cp boost/LICENSE_1_0.txt $LIB_DIR/fennel/boost.license.txt
cp iSQLViewer/* $ISQL_DIR

# TODO jvs 12-Mar-2005
# cp -d icu/lib/$SO_3P_PATTERN $LIB_DIR/fennel
# cp icu/license.html $LIB_DIR/fennel/icu.license.html

if [ $cygwin = "true" ]; then
    cp /usr/bin/mingwm10.dll $LIB_DIR/fennel
fi

# copy fennel libs
cd $FENNEL_DIR
cp -d libfennel/.libs/$SO_PATTERN $LIB_DIR/fennel
cp -d farrago/.libs/$SO_PATTERN $LIB_DIR/fennel
cp -d disruptivetech/libfennel_dt/.libs/$SO_PATTERN $LIB_DIR/fennel
cp -d disruptivetech/farrago/.libs/$SO_PATTERN $LIB_DIR/fennel
cp -d lucidera/libfennel_lu/.libs/$SO_PATTERN $LIB_DIR/fennel
cp -d lucidera/farrago/.libs/$SO_PATTERN $LIB_DIR/fennel

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
if [ -e dist/README ]; then
    cp dist/README $RELEASE_DIR
fi
cp dist/farrago.jar $LIB_DIR
cp dist/plugin/*.jar $LIB_DIR/plugin

# copy other farrago artifacts
if [ $cygwin = "true" ]; then
    cp dist/install/install.bat $INSTALL_DIR
else
    cp dist/install/install.sh $INSTALL_DIR
fi
cp catalog/FarragoCatalog.* $CATALOG_DIR
cp catalog/ReposStorage.properties $CATALOG_DIR
cp catalog/*.dat $CATALOG_DIR
if [ $cygwin = "true" ]; then
    cp dist/bin/*.bat $BIN_DIR
else
    cp dist/bin/* $BIN_DIR
    rm -f $BIN_DIR/*.bat
fi
cp isql/FarragoServer.service $ISQL_DIR

# archive the whole thing up
cd $TMP_DIR
if [ $cygwin = "true" ]; then
    zip -r -y ../farrago.zip .
else
    tar cv * | bzip2 -c >../farrago.tar.bz2
fi

rm -rf $TMP_DIR
