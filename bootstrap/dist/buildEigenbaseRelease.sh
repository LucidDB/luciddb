#!/bin/bash
# $Id$
# Eigenbase master build script for creating release images
# Copyright (C) 2005 The Eigenbase Project
# Copyright (C) 2005 SQLstream, Inc.
# Copyright (C) 2005 Dynamo BI Corporation
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

if test "`p4 opened`" != ""; then
    echo "You have Perforce files open for edit in this client; aborting."
    exit -1
fi
  
usage() {
    echo "Usage:  buildEigenbaseRelease.sh <label> <major> <minor> <point>"
}

# Get parameters
if [ "$#" != 4 ]; then
    usage
    exit -1
fi

set -e
set -v

LABEL="$1"
MAJOR="$2"
MINOR="$3"
POINT="$4"

# Construct release names
RELEASE_NUMBER="$MAJOR.$MINOR.$POINT"
BINARY_RELEASE="eigenbase-$RELEASE_NUMBER"
LUCIDDB_BINARY_RELEASE="luciddb-$RELEASE_NUMBER"
SRC_RELEASE="eigenbase-src-$RELEASE_NUMBER"
FENNEL_RELEASE="fennel-$RELEASE_NUMBER"
FARRAGO_RELEASE="farrago-$RELEASE_NUMBER"

DIST_DIR=$(cd `dirname $0`; pwd)
OPEN_DIR=$DIST_DIR/../..

# Detect platform
cygwin=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
esac

if [ $cygwin = "true" ]; then
    ARCHIVE_SUFFIX=zip
else
    ARCHIVE_SUFFIX=tar.bz2

    if [ `uname` != "Darwin" ]; then
        # Verify that chrpath is available
        if [ ! -e /usr/bin/chrpath ]; then
            echo "Error:  /usr/bin/chrpath is not installed"
            exit -1
        fi
    fi
fi

# Generate version info
# This will fail if requested label doesn't exist
echo "$BINARY_RELEASE" > $DIST_DIR/VERSION
echo "Perforce change @`p4 counter change`" >> $DIST_DIR/VERSION
p4 label -o $LABEL >> $DIST_DIR/VERSION

# Start from a clean sync to requested label
cd $OPEN_DIR
rm -rf thirdparty fennel farrago luciddb extensions
p4 sync -f thirdparty/...@$LABEL
p4 sync -f fennel/...@$LABEL
p4 sync -f farrago/...@$LABEL
p4 sync -f luciddb/...@$LABEL
p4 sync -f extensions/...@$LABEL

# Verify that client was mapped correctly
if [ ! -e thirdparty ]; then
    echo "Error:  thirdparty is not where it should be"
    exit -1
fi
if [ ! -e fennel ]; then
    echo "Error:  fennel is not where it should be"
    exit -1
fi
if [ ! -e farrago ]; then
    echo "Error:  farrago is not where it should be"
    exit -1
fi
if [ ! -e luciddb ]; then
    echo "Error:  luciddb is not where it should be"
    exit -1
fi
if [ ! -e extensions ]; then
    echo "Error:  extensions is not where it should be"
    exit -1
fi

# Create farrago/customBuild.properties to set GPL release flag
echo 'build.mode=release' > farrago/customBuild.properties

# Append setting to pick up custom LucidDB release properties
echo 'release.properties.source=${luciddb.dir}/src/FarragoRelease.properties' \
    >> farrago/customBuild.properties

# Kludge for forcing 32-bit runtime on MacOS
if [ `uname` = "Darwin" ]; then
    echo 'assertions.jvmarg=-ea -esa -d32' >> farrago/customBuild.properties
fi

if [ $cygwin = "false" ]; then

# Build full source release first before projects get polluted by builds
cd $DIST_DIR
rm -f $SRC_RELEASE.$ARCHIVE_SUFFIX
rm -rf $SRC_RELEASE
mkdir $SRC_RELEASE
cp -R $OPEN_DIR/thirdparty $SRC_RELEASE
# Delete and stub out irrelevant thirdparty archives
rm -f $SRC_RELEASE/thirdparty/icu-2.8.patch.tgz
rm -f $SRC_RELEASE/thirdparty/tpch.tar.gz
rm -f $SRC_RELEASE/thirdparty/postgresql-*
rm -f $SRC_RELEASE/thirdparty/logging-log4j-*
rm -f $SRC_RELEASE/thirdparty/jfreechart-*
rm -f $SRC_RELEASE/thirdparty/jdbcappender.zip
rm -f $SRC_RELEASE/thirdparty/jcommon-*
rm -rf $SRC_RELEASE/thirdparty/GroboUtils
touch $SRC_RELEASE/thirdparty/logging-log4j-1.3alpha-8.tar.gz
touch $SRC_RELEASE/thirdparty/log4j
touch $SRC_RELEASE/thirdparty/jdbcappender.zip
touch $SRC_RELEASE/thirdparty/jdbcappender
touch $SRC_RELEASE/thirdparty/jtds-1.2-dist.zip
touch $SRC_RELEASE/thirdparty/jtds
touch $SRC_RELEASE/thirdparty/tpch.tar.gz
touch $SRC_RELEASE/thirdparty/tpch
cp -R $OPEN_DIR/fennel $SRC_RELEASE
cp -R $OPEN_DIR/farrago $SRC_RELEASE
cp -R $OPEN_DIR/luciddb $SRC_RELEASE
cp -R $OPEN_DIR/extensions $SRC_RELEASE
cp $DIST_DIR/VERSION $SRC_RELEASE
cp $DIST_DIR/README.src $SRC_RELEASE/README
cp $OPEN_DIR/farrago/COPYING $SRC_RELEASE
tar cjvf $SRC_RELEASE.$ARCHIVE_SUFFIX $SRC_RELEASE
rm -rf $SRC_RELEASE

# Build Farrago-only source release
rm -f $DIST_DIR/$FARRAGO_RELEASE.$ARCHIVE_SUFFIX
rm -rf $DIST_DIR/$FARRAGO_RELEASE
rm -rf $DIST_DIR/farrago
cp -R $OPEN_DIR/farrago $DIST_DIR
cd $DIST_DIR
mv farrago $FARRAGO_RELEASE
cp $DIST_DIR/VERSION $FARRAGO_RELEASE
tar cjvf $FARRAGO_RELEASE.$ARCHIVE_SUFFIX $FARRAGO_RELEASE
rm -rf $DIST_DIR/$FARRAGO_RELEASE


# Build Fennel-only source release
rm -rf $DIST_DIR/$FENNEL_RELEASE
rm -f $DIST_DIR/$FENNEL_RELEASE.$ARCHIVE_SUFFIX
cd $DIST_DIR
mkdir $FENNEL_RELEASE
cp -R $OPEN_DIR/fennel $FENNEL_RELEASE
cp $DIST_DIR/VERSION $FENNEL_RELEASE
tar cjvf $FENNEL_RELEASE.$ARCHIVE_SUFFIX $FENNEL_RELEASE
rm -rf $FENNEL_RELEASE

fi

# Build full binary release
rm -f $DIST_DIR/$BINARY_RELEASE.$ARCHIVE_SUFFIX
cp -f $DIST_DIR/VERSION $OPEN_DIR/farrago/dist
cp -f $DIST_DIR/README.bin $OPEN_DIR/farrago/dist/README
cat > $OPEN_DIR/farrago/dist/FarragoRelease.properties <<EOF
package.name=eigenbase
product.name=Eigenbase Data Management System
product.version.major=$MAJOR
product.version.minor=$MINOR
product.version.point=$POINT
jdbc.driver.name=FarragoJdbcDriver
jdbc.driver.version.major=$MAJOR
jdbc.driver.version.minor=$MINOR
jdbc.url.base=jdbc:farrago:
jdbc.url.port.default=5433
jdbc.url.http.port.default=8033
EOF
cat > $OPEN_DIR/luciddb/src/FarragoRelease.properties <<EOF
package.name=luciddb
product.name=LucidDB
product.version.major=$MAJOR
product.version.minor=$MINOR
product.version.point=$POINT
jdbc.driver.name=LucidDbJdbcDriver
jdbc.driver.version.major=$MAJOR
jdbc.driver.version.minor=$MINOR
jdbc.url.base=jdbc:luciddb:
jdbc.url.port.default=5434
jdbc.url.http.port.default=8034
EOF

cd $OPEN_DIR/farrago
./initBuild.sh --with-fennel --with-optimization --without-debug
./distBuild.sh --skip-init-build
mv ../farrago/dist/farrago.$ARCHIVE_SUFFIX \
    $DIST_DIR/$BINARY_RELEASE.$ARCHIVE_SUFFIX

cd $OPEN_DIR/luciddb
./initBuild.sh --without-farrago-build --with-optimization --without-debug
mv dist/luciddb.$ARCHIVE_SUFFIX \
    $DIST_DIR/$LUCIDDB_BINARY_RELEASE.$ARCHIVE_SUFFIX
