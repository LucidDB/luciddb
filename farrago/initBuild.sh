#!/bin/bash
# $Id$
# Farrago is an extensible data management system.
# Copyright (C) 2005-2007 The Eigenbase Project
# Copyright (C) 2005-2007 Disruptive Tech
# Copyright (C) 2005-2007 LucidEra, Inc.
# Portions Copyright (C) 2003-2007 John V. Sichi
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

# Script to set up a new Farrago build environment, or to reinitialize
# an existing one after syncing changes from source control.

usage() {
    echo "Usage:  initBuild.sh"
    echo "           --with[out]-fennel (required)"
    echo "           [--with[out]-optimization] (default without)"
    echo "           [--with[out]-debug] (default with)"
    echo "           [--without-fennel[-thirdparty]-build] (default w/both)"
    echo "           [--with[out]-aio-required] (default without)"
    echo ""
    echo "           [--with[out]-tests] (default without)"
    echo "           [--with-nightly-tests] (default without)"
    echo "           [--with-repos-type=(default|mysql/hibernate|psql/netbeans)]"
}

fennel_flag_missing=true
fennel_disabled=missing
fennel_skip_build=false
skip_tests=true
with_nightly_tests=false
repos_type=missing

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
        --with-fennel) fennel_disabled=false;;
        --without-fennel) fennel_disabled=true;;
        --with?(out)-optimization) OPT_FLAG="$1";;
        --with?(out)-debug) DEBUG_FLAG="$1";;
        --with?(out)-aio-required) AIO_FLAG="$1";;
        --skip-fennel-build|--without-fennel-build) 
            fennel_skip_build=true;;
        --skip-fennel-thirdparty-build|--without-fennel-thirdparty-build) 
            FENNEL_BUILD_FLAG="$1";;
        --with-tests)
            skip_tests=false;
            TEST_FLAG="$1";;
        --with-nightly-tests)
            with_nightly_tests=true;;
        --without-tests)
            skip_tests=true;
            TEST_FLAG="$1";;

        --with-repos-type=default)
            repos_type="switchToDefaultReposStorage";;
        --with-repos-type=mysql/hibernate)
            repos_type="switchToMysqlHibernateReposStorage";;
        --with-repos-type=psql/netbeans) 
            repos_type="switchToPsqlReposStorage";;
            
        *) echo "Unknown option: $1"; usage; exit -1;;
    esac
    shift
done

shopt -uq extglob

# Check required options
if [ $fennel_disabled == "missing" ] ; then
    echo "You must specify --with-fennel or --without-fennel"
    usage
    exit -1;
fi

rm -f initBuild.properties

# Set up Farrago custom build properties file
cat >> initBuild.properties <<EOF
# initBuild.properties should only be used to store the fennel.disabled
# property: initBuild.sh will destroy other information stored here.  Create
# customBuild.properties to override other build parameters if necessary.
fennel.disabled=$fennel_disabled
EOF

set -e
set -v

if $with_nightly_tests ; then
    # make the build/test processes go further
    set +e
    run_ant="ant -keep-going"
else
    run_ant="ant"
fi

# Blow away obsolete Farrago build properties file
rm -f farrago_build.properties

. farragoenv.sh `pwd`/../thirdparty

# Unpack thirdparty components
cd ../thirdparty
make farrago optional

if $fennel_disabled ; then
    echo Fennel disabled.
else
    cd ../fennel
    if $fennel_skip_build; then
        echo Fennel enabled. Skipping Fennel build.
    else
        ./initBuild.sh --with-farrago $OPT_FLAG $DEBUG_FLAG $AIO_FLAG \
            $FENNEL_BUILD_FLAG $TEST_FLAG
    fi

    echo "Sourcing Fennel Runtime Environment" 
    # Set up Fennel runtime environment
    . fennelenv.sh `pwd`
fi

# Build Farrago catalog and everything else, then run tests
# (but don't run tests when Fennel is disabled, since most fail without it)
cd ../farrago
${run_ant} clean

# if missing, assume repos is configured as desired (or that the default is ok)
if [ $repos_type != "missing" ]; then
    ${run_ant} $repos_type
fi

if $fennel_disabled || $skip_tests ; then
    ${run_ant} createCatalog
else
    ${run_ant} test
fi
