#!/bin/bash
# $Id$
# Eigenbase master build script for creating release images
# Copyright (C) 2005-2005 The Eigenbase Project
# Copyright (C) 2005-2005 Disruptive Tech
# Copyright (C) 2005-2005 Red Square, Inc.
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

set -e
set -v

usage() {
    echo "Usage:  buildEigenbaseRelease.sh <label> <major> <minor>"
}

# Get parameters
if [ "$#" != 3 ]; then
    usage
    exit -1
fi
LABEL="$1"
MAJOR="$2"
MINOR="$3"

# Construct release names
BINARY_RELEASE="eigenbase-$MAJOR.$MINOR"
SRC_RELEASE="eigenbase-src-$MAJOR.$MINOR"
FENNEL_RELEASE="fennel-$MAJOR.$MINOR"
FARRAGO_RELEASE="farrago-$MAJOR.$MINOR"

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
fi

# Generate version info
# This will fail if requested label doesn't exist
echo "$BINARY_RELEASE" > $DIST_DIR/VERSION
echo "Perforce change @`p4 counter change`" >> $DIST_DIR/VERSION
p4 label -o $LABEL >> $DIST_DIR/VERSION

# Start from a clean sync to requested label
cd $OPEN_DIR
rm -rf thirdparty fennel farrago
p4 sync -f thirdparty/...@$LABEL
p4 sync -f fennel/...@$LABEL
p4 sync -f farrago/...@$LABEL

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

if [ $cygwin = "false" ]; then

# Build full source release first before projects get polluted by builds
cd $DIST_DIR
rm -f $SRC_RELEASE.$ARCHIVE_SUFFIX
rm -rf $SRC_RELEASE
mkdir $SRC_RELEASE
cp -R $OPEN_DIR/thirdparty $SRC_RELEASE
cp -R $OPEN_DIR/fennel $SRC_RELEASE
cp -R $OPEN_DIR/farrago $SRC_RELEASE
cp $DIST_DIR/VERSION $SRC_RELEASE
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

fi

# Build full binary release
rm -f $DIST_DIR/$BINARY_RELEASE.$ARCHIVE_SUFFIX
cp -f $DIST_DIR/VERSION $OPEN_DIR/farrago/dist
cat > $OPEN_DIR/farrago/dist/FarragoRelease.properties <<EOF
package.name=eigenbase
product.name=Eigenbase Data Management System
product.version.major=$MAJOR
product.version.minor=$MINOR
jdbc.driver.name=FarragoJdbcDriver
jdbc.driver.version.major=$MAJOR
jdbc.driver.version.minor=$MINOR
jdbc.url.base=jdbc:farrago:
jdbc.url.port.default=5433
EOF
cd $OPEN_DIR/farrago
./initBuild.sh --with-fennel --with-optimization
./distBuild.sh --skip-init-build
mv dist/farrago.$ARCHIVE_SUFFIX $DIST_DIR/$BINARY_RELEASE.$ARCHIVE_SUFFIX

if [ $cygwin = "false" ]; then

# Build Fennel-only source release
# This has to happen after binary build so that Makefiles are generated.
# Note that if someone forgot to update Fennel's version in configure.in,
# the tar xjvf below will fail.
rm -rf $DIST_DIR/$FENNEL_RELEASE
rm -f $DIST_DIR/$FENNEL_RELEASE.$ARCHIVE_SUFFIX
cd $OPEN_DIR/fennel
make dist
cd $DIST_DIR
tar xzvf $OPEN_DIR/fennel/$FENNEL_RELEASE.tar.gz
cp $DIST_DIR/VERSION $FENNEL_RELEASE
tar cjvf $FENNEL_RELEASE.$ARCHIVE_SUFFIX $FENNEL_RELEASE
rm -rf $FENNEL_RELEASE
rm -f $OPEN_DIR/fennel/$FENNEL_RELEASE.tar.gz

fi
