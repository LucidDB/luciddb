#!/bin/bash
# $Id$
# Farrago is an extensible data management system.
# Copyright (C) 2005 The Eigenbase Project
# Copyright (C) 2005 SQLstream, Inc.
# Copyright (C) 2005 Dynamo BI Corporation
# Portions Copyright (C) 2003 John V. Sichi
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
    echo "Usage:  distBuild.sh [--without-init-build][--with[out]-debug]"
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

CP_ARCHIVE_FLAG=-d
if [ $cygwin = "true" ]; then
    SO_PATTERN="*.dll"
else
    if [ `uname` = "Darwin" ]; then
        SO_PATTERN="lib*.dylib*"
        CP_ARCHIVE_FLAG=-PR
    else
        SO_PATTERN="lib*.so*"
    fi
fi

#default
init_build=true
dist_fennel=true
remove_debug=true

while [ -n "$1" ]; do
    case $1 in
        --skip-init-build|--without-init-build) 
            init_build=false;;
        --with-debug)
        	remove_debug=false;;
        --without-debug)
        	;;
        *) usage; exit -1;;
    esac
    shift
done

if $init_build; then
	debug_param=--with-debug
	if $remove_debug; then
		debug_param=--without-debug
	fi
    ./initBuild.sh --with-fennel ${debug_param}
else
    echo "Skip init build"

    # if fennel was not build, don't include it.
    if ( grep -q -i '^fennel.disabled=true' initBuild.properties ) ; then
        dist_fennel=false;
    else 
        dist_fennel=true;
    fi
fi

set -e
set -v

# set up directories
OPEN_DIR=$(cd ..; pwd)
FARRAGO_DIR=$OPEN_DIR/farrago
DIST_DIR=$FARRAGO_DIR/dist
TMP_DIR=$DIST_DIR/tmp
FENNEL_DIR=$OPEN_DIR/fennel
THIRDPARTY_DIR=$OPEN_DIR/thirdparty

# create staging directory
# FRG-402 Setup ant if not already present
cd $FARRAGO_DIR
. ./farragoenv.sh $THIRDPARTY_DIR

# get help from ant to figure out where to build release image
ant dist

# derive staging sub-directories
RELEASE_DIR=`echo $TMP_DIR/*`
LIB_DIR=$RELEASE_DIR/lib
PLUGIN_DIR=$RELEASE_DIR/plugin
INSTALL_DIR=$RELEASE_DIR/install
CATALOG_DIR=$RELEASE_DIR/catalog
BIN_DIR=$RELEASE_DIR/bin

# create staging sub-directories
mkdir $LIB_DIR
mkdir $PLUGIN_DIR
mkdir $LIB_DIR/mdrlibs
mkdir $LIB_DIR/enki
mkdir $LIB_DIR/fennel
mkdir $INSTALL_DIR
mkdir $CATALOG_DIR
mkdir $CATALOG_DIR/fennel
mkdir $BIN_DIR

# copy thirdparty libs
cd $THIRDPARTY_DIR
cp janino/janino.jar $LIB_DIR
cp janino/new_bsd_license.txt $LIB_DIR/janino.license.txt
cp resgen/lib/eigenbase-resgen.jar $LIB_DIR
cp resgen/lib/eigenbase-xom.jar $LIB_DIR
cp resgen/COPYING $LIB_DIR/resgen.license.txt
cp mdrlibs/* $LIB_DIR/mdrlibs
rm -f $LIB_DIR/mdrlibs/uml*.jar
rm -f $LIB_DIR/mdrlibs/mdrant.jar
cp enki/*.jar enki/*.txt enki/LICENSE $LIB_DIR/enki
rm -f $LIB_DIR/enki/eigenbase-enki-*-doc.jar
rm -f $LIB_DIR/enki/enki-src.jar
cp OpenJava/openjava.jar $LIB_DIR
cp OpenJava/COPYRIGHT $LIB_DIR/openjava.license.txt
cp RmiJdbc/dist/lib/*.jar $LIB_DIR
cp ant/LICENSE $LIB_DIR/commons.license.txt
cp csvjdbc/csvjdbc.jar $LIB_DIR
cp csvjdbc/license.txt $LIB_DIR/csvjdbc.license.txt
cp sqlline.jar $LIB_DIR
cp sqlline/LICENSE $LIB_DIR/sqlline.license
cp jline.jar $LIB_DIR
cp jgrapht/jgrapht-jdk1.5.jar $LIB_DIR
cp jgrapht/license-LGPL.txt $LIB_DIR/jgrapht.license.txt
cp jgrapht/license-LGPL.txt $LIB_DIR/vjdbc.license.txt
cp jgrapht/license-LGPL.txt $LIB_DIR/RmiJdbc.license.txt
cp hsqldb/doc/hypersonic_lic.txt $LIB_DIR/hsqldb.license.txt
cp hsqldb/lib/hsqldb.jar $LIB_DIR
if [ -e postgresql-8.4-701.jdbc3.jar ]; then
    cp postgresql-8.4-701.jdbc3.jar $LIB_DIR
fi
cp commons-transaction-1.1.jar $LIB_DIR
cp vjdbc/lib/vjdbc.jar $LIB_DIR
cp vjdbc/lib/vjdbc_server.jar $LIB_DIR
cp vjdbc/lib/commons-logging-1.1.jar $LIB_DIR
cp vjdbc/lib/commons-pool-1.3.jar $LIB_DIR
cp vjdbc/lib/commons-dbcp-1.4.jar $LIB_DIR
cp vjdbc/lib/commons-digester-1.7.jar $LIB_DIR
cp stlport/README $LIB_DIR/fennel/stlport.README.txt
# get rid of this dangling symlink; it causes trouble for cp
rm -f stlport/lib/libstlport_gcc_debug.so
if $dist_fennel; then
    cp ${CP_ARCHIVE_FLAG} stlport/lib/$SO_PATTERN $LIB_DIR/fennel
    cp ${CP_ARCHIVE_FLAG} boost/lib/$SO_PATTERN $LIB_DIR/fennel
fi
cp jetty/lib/*.jar $LIB_DIR
cp jetty/LICENSE-APACHE-2.0.txt $LIB_DIR/jetty.license.txt

if $remove_debug; then
    rm -f $LIB_DIR/fennel/*debug*
    rm -f $LIB_DIR/fennel/*gdp*
fi
cp boost/LICENSE_1_0.txt $LIB_DIR/fennel/boost.license.txt

# TODO jvs 12-Mar-2005
# if dist_fennel; then
#   cp ${CP_ARCHIVE_FLAG} icu/lib/$SO_PATTERN $LIB_DIR/fennel
# fi
# cp icu/license.html $LIB_DIR/fennel/icu.license.html

# copy fennel libs
if $dist_fennel; then
    cd $FENNEL_DIR
    cp ${CP_ARCHIVE_FLAG} libfennel/$SO_PATTERN $LIB_DIR/fennel
    cp ${CP_ARCHIVE_FLAG} farrago/$SO_PATTERN $LIB_DIR/fennel

    # if possible, strip rpath info
    if [ $cygwin = "false" ]; then
        if [ -e /usr/bin/chrpath ]; then
            /usr/bin/chrpath -d $LIB_DIR/fennel/*.so*
        fi
    fi
    
    # copy fennel resources
    cp common/*.properties $CATALOG_DIR/fennel
fi

# create farrago libs
cd $DIST_DIR
ant allJars 

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
cp dist/vjdbc_servlet.war $LIB_DIR
cp dist/plugin/*.jar $PLUGIN_DIR

# copy other farrago artifacts
if [ $cygwin = "true" ]; then
    cp dist/install/install.bat $INSTALL_DIR
else
    cp dist/install/install.sh $INSTALL_DIR
fi

# Make a backup to get a mysql dump in the event that HSQLDB isn't being used
ant backupCatalog
cp catalog/backup/FarragoCatalog.* $CATALOG_DIR
cp catalog/ReposStorage.properties $CATALOG_DIR
 
if $dist_fennel; then
    cp catalog/backup/*.dat $CATALOG_DIR
fi

if [ $cygwin = "true" ]; then
    cp dist/bin/*.bat $BIN_DIR
else
    cp dist/bin/* $BIN_DIR
    rm -f $BIN_DIR/*.bat
fi

cd $DIST_DIR
# archive the whole thing up
if [ $cygwin = "true" ]; then
    cd $TMP_DIR
    zip -r -y ../farrago.zip .
    cd $DIST_DIR
else
    ant package
fi
ant removeReleaseDir

cd $FARRAGO_DIR
