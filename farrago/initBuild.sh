#!/bin/bash
# $Id$

# Script to set up a new Farrago build environment

usage() {
    echo "Usage:  initBuild.sh --with[out]-fennel [--append-init-properties] [--with[out]-optimization] [--skip-thirdparty]"
}

if [ "$1" == "--with-fennel" ] ; then
    fennel_disabled=false
    shift
elif [ "$1" == "--without-fennel" ] ; then
    fennel_disabled=true
    shift
else
    usage
    exit -1
fi

if [ "$1" == "--append-init-properties" ] ; then
    touch initBuild.properties
    shift
else
    # default is remove this file
    rm -f initBuild.properties
fi

if [ "$1" == "--with-optimization" -o "$1" == "--without-optimization" ] ; then
    OPT_FLAG="$1"
    shift
else
    # default to unoptimized build
    OPT_FLAG="--without-optimization"
fi

if [ "$1" == "--skip-thirdparty" ] ; then
    THIRDPARTY_FLAG="$1"
    shift
else
    THIRDPARTY_FLAG=""
fi

if [ -n "$1" ] ; then
    usage
    exit -1;
fi

# Set up Farrago custom build properties file
cat >> initBuild.properties <<EOF
# initBuild.properties should only be used to store the fennel.disabled
# property: initBuild.sh will destroy other information stored here.  Create
# customBuild.properties to override other build parameters if necessary.
fennel.disabled=$fennel_disabled
EOF

set -e
set -v

# Blow away obsolete Farrago build properties file
rm -f farrago_build.properties

. farragoenv.sh `pwd`/../thirdparty


# Unpack thirdparty components
cd ../thirdparty
make farrago optional

if $fennel_disabled ; then
    echo Skipping Fennel build
else
    cd ../fennel
    ./initBuild.sh --with-farrago $OPT_FLAG $THIRDPARTY_FLAG

    # Set up Fennel runtime environment
    . fennelenv.sh `pwd`
fi

# Build Saffron
cd ../saffron
ant clean
ant

# Build Farrago catalog and everything else, then run tests
cd ../farrago
ant clean
ant test
