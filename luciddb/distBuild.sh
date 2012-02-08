#!/bin/bash
# Licensed to DynamoBI Corporation (DynamoBI) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  DynamoBI licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http:www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
