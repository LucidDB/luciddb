#!/bin/bash
# $Id$

# Script to set up a new Farrago build environment

if [ "$1" == "--with-fennel" ] ; then
    fennel_disabled=false
elif [ "$1" == "--without-fennel" ] ; then
    fennel_disabled=true
else
    echo "Usage:  initBuild.sh [--with-fennel|--without-fennel]"
    exit -1
fi
    
# Set up Farrago custom build properties file
rm -f initBuild.properties
cat > initBuild.properties <<EOF
# initBuild.properties should only be used to store the fennel.disabled
# property: initBuild.sh will destroy other information stored here.  Create
# customBuild.properties to override other build parameters if necessary.
fennel.disabled=$fennel_disabled
EOF

# Blow away obsolete Farrago build properties file
rm -f farrago_build.properties

. farragoenv.sh `pwd`/../thirdparty

set -e
set -v

# Unpack thirdparty components
cd ../thirdparty
make farrago optional

if $fennel_disabled ; then
    echo Skipping Fennel build
else
    cd ../fennel
    ./initBuild.sh --with-farrago

    # Set up Fennel runtime environment
    . fennelenv.sh `pwd`
fi

# Build Saffron
cd ../saffron
ant

# Build Farrago catalog and everything else, then run tests
cd ../farrago
ant test
