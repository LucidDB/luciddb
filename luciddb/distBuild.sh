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

usage() {
    echo "Usage:  distBuild.sh [--without-farrago-build] [--without-init-build]"
}

without_farrago_build=false
without_init_build=false
INIT_BUILD_FLAGS=""
luciddb_dir=$(cd $(dirname $0); pwd)
FARRAGO_DIST_FLAGS=""

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
         --without-farrago-build|--skip-farrago-build) 
	    without_farrago_build=true;
            INIT_BUILD_FLAGS="${INIT_BUILD_FLAGS} $1";;
        --without-init-build|--skip-init-build)
            without_init_build=true;;
        --with?(out)-debug) 
            FARRAGO_DIST_FLAGS="${FARRAGO_DIST_FLAGS} $1";;
        --*) INIT_BUILD_FLAGS="${INIT_BUILD_FLAGS} $1";;

        *) usage; exit -1;;
    esac

    shift
done

shopt -uq extglob

if ! ${without_init_build}; then
    if ${without_farrago_build}; then
        ./initBuild.sh ${INIT_BUILD_FLAGS}
    else
        ./initBuild.sh ${INIT_BUILD_FLAGS} --with-fennel
    fi
fi

set -x
set -v

# get the thirdparty ant
cd ${luciddb_dir}/../farrago
. ./farragoenv.sh `pwd`/../thirdparty

if $without_farrago_build ; then
    echo "Skip Farrago Packaging ..."
else
    cd ${luciddb_dir}/../farrago
    /bin/bash -x ./distBuild.sh --without-init-build ${FARRAGO_DIST_FLAGS}
fi


cd ${luciddb_dir}
ant dist
