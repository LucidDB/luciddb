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

# Build Farrago catalog and everything else, then run tests
# (but don't run tests when Fennel is disabled, since most fail without it)
cd ../farrago
ant clean

if $fennel_disabled ; then
    ant createCatalog
else
    ant test
fi
