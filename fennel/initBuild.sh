#!/bin/bash
# $Id$

if [ "$1" == "--with-farrago" -o "$1" == "--without-farrago" ] ; then
    FARRAGO_FLAG=$1
else
    echo "Usage:  initBuild.sh [--with-farrago|--without-farrago]"
    exit -1;
fi

set -e
set -v

SAVE_PWD="$PWD"

# Unpack thirdparty components
cd ../thirdparty
make fennel


# Detect Cygwin
cygwin=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
esac


if $cygwin ; then
    export CC="gcc -mno-cygwin"
    export CXX="gcc -mno-cygwin"
    MINGW32_TARGET="--target=mingw32"
fi

# Configure Fennel
cd "$SAVE_PWD"
autoreconf --force --install
./configure --with-boost=`pwd`/../thirdparty/boost \
    --with-stlport=`pwd`/../thirdparty/stlport \
    $FARRAGO_FLAG $MINGW32_TARGET

if $cygwin ; then
    unset CC
    unset CXX
fi

# Build thirdparty libaries required by Fennel
cd build
./buildStlportLibs.sh
./buildBoostLibs.sh

# Build Fennel itself
cd ..
make
