#!/bin/bash
# $Id: //open/lu/dev/luciddb/distBuild.sh#1 $
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
set -v

usage() {
    echo "Usage:  distBuild.sh --with[out]-farrago [--skip-init-build]"
}


farrago_disabled=missing
skip_init_build=false
INIT_BUILD_FLAGS=""
luciddb_dir=$(cd $(dirname $0); pwd)

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
        --with-farrago) 
	    farrago_disabled=false;
            INIT_BUILD_FLAGS="${INIT_BUILD_FLAGS} $1";;
        --without-farrago) 
	    farrago_disabled=true;
            INIT_BUILD_FLAGS="${INIT_BUILD_FLAGS} $1";;

        --skip-init-build)
            skip_init_build=true;
            INIT_BUILD_FLAGS="${INIT_BUILD_FLAGS} $1";;

        --*) INIT_BUILD_FLAGS="${INIT_BUILD_FLAGS} $1";;

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

set -x

if ! ${skip_init_build}; then
    if ${farrago_disabled}; then
        ./initBuild.sh ${INIT_BUILD_FLAGS}
    else
        ./initBuild.sh ${INIT_BUILD_FLAGS} --with-fennel
    fi
fi

if $farrago_disabled ; then
    echo "Skip Farrago Packaging ..."
else
    cd ${luciddb_dir}/../farrago
    ./distBuild.sh --skip-init-build
fi

# get the thirdparty ant
cd ${luciddb_dir}/../farrago
. farragoenv.sh `pwd`/../thirdparty

cd ${luciddb_dir}
ant dist
