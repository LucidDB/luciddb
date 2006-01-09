#!/bin/bash
# $Id$
# LucidDB is a DBMS optimized for business intelligence.
# Copyright (C) 2006-2006 LucidEra, Inc.
# Copyright (C) 2006-2006 The Eigenbase Project
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version approved by The Eigenbase Project.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#  
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

usage() {
    echo "Usage:  initBuild.sh --with[out]-farrago [--with[out]-fennel] [--with[out]-optimization] [--with[out]-debug] [--skip-fennel[-thirdparty]-build] [--with[out]-tests]"
}


farrago_disabled=missing
skip_tests=true
FENNEL_FLAG=--with-fennel

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
        --with-farrago) farrago_disabled=false;;
        --without-farrago) farrago_disabled=true;;
        --with?(out)-fennel) FENNEL_FLAG="$1";;
        --with?(out)-optimization) OPT_FLAG="$1";;
        --with?(out)-debug) DEBUG_FLAG="$1";;
        --skip-fennel?(-thirdparty)-build) FENNEL_BUILD_FLAG="$1";;
        --with-tests)
            skip_tests=false;
            TEST_FLAG="$1";;
        --without-tests)
            skip_tests=true;
            TEST_FLAG="$1";;

        *) usage; exit -1;;
    esac
    shift
done

shopt -uq extglob

# Check required options
if [ $farrago_disabled == "missing" ] ; then
    usage
    exit -1;
fi

if $farrago_disabled ; then
    echo Farrago disabled.
else
    cd ../farrago
    ./initBuild.sh $FENNEL_FLAG $OPT_FLAG $DEBUG_FLAG \
        $FENNEL_BUILD_FLAG $TEST_FLAG
fi

# Build catalog then run tests
cd ../luciddb
ant clean

if $skip_tests ; then
    ant createCatalog
else
    ant test
fi
