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
set -e

usage() {
    echo "Usage:  initBuild.sh [--with[out]-fennel] [--with[out]-optimization] [--with[out]-debug] [--skip-farrago-build] [--skip-fennel[-thirdparty]-build] [--with[out]-tests]"
}

farrago_skip_build=false
skip_tests=true
FARRAGO_FLAGS=""
luciddb_dir=$(cd $(dirname $0); pwd)

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
        --skip-farrago-build) farrago_skip_build=true;;
        --with-tests)
            skip_tests=false;
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        --without-tests)
            skip_tests=true;
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        --*) FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        
        *) usage; exit -1;;
    esac

    shift
done

shopt -uq extglob
set -v

if $farrago_skip_build ; then
    echo Skipping Farrago build.
else
    cd ${luciddb_dir}/../farrago
    ./initBuild.sh ${FARRAGO_FLAGS}
fi

# Build catalog then run tests
cd ${luciddb_dir}/../farrago
. farragoenv.sh `pwd`/../thirdparty

# set up Fennel's LD_LIBRARY_PATH
export LD_LIBRARY_PATH=
. ${luciddb_dir}/../fennel/fennelenv.sh ${luciddb_dir}/../fennel

cd ${luciddb_dir}
ant clean

if $skip_tests ; then
    ant createCatalog
else
    cd ${luciddb_dir}/../blackhawk
    ant

    cd ${luciddb_dir}
    ant test
fi
