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
    echo "Usage:  initBuild.sh "
    echo "            [--without-fennel[-thirdparty]-build] (default w/both) "
    echo "            [--with[out]-fennel] (default with) "
    echo "            [--with[out]-optimization] (default without) "
    echo "            [--with[out]-debug] (default with) "
    echo "            [--with[out]-aio-required] (default with) "
    echo ""
    echo "            [--with[out]-tests] (default without) "
    echo "            [--with-nightly-tests] (default without) "
    echo ""
    echo "            [--without-farrago-build] (default with) "
    echo "            [--without-dist-build] (default with) "
    echo "            [--with-repos-type=REPOS_TYPE]"
    echo "              where REPOS_TYPE may be:"
    echo "                 default            (Enki/Hibernate + HSQLDB)"
    echo "                 mysql/hibernate    (Enki/Hibernate + MySQL)"
    echo "                 hsqldb/hibernate   (Enki/Hibernate + HSQLDB)"
    echo "                 hsqldb/netbeans    (Enki/Netbeans + HSQLDB)"
    echo "                 psql/netbeans      (Enki/Netbeans + psql)"
}

with_fennel=true
with_aio=true
without_farrago_build=false
without_dist_build=false
without_tests=true
with_nightly_tests=false
FARRAGO_FLAGS=""
FARRAGO_DIST_FLAGS=""
luciddb_dir=$(cd $(dirname $0); pwd)
with_repos_type=false

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
        --without-farrago-build|--skip-farrago-build) 
            without_farrago_build=true;;
        --without-dist-build|--skip-dist-build) 
            without_dist_build=true;;

        --with-tests)
            without_tests=false;
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        --without-tests)
            without_tests=true;
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        --with-nightly-tests)
            with_nightly_tests=true;
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;

        --with-fennel)
            with_fennel=true;;
        --without-fennel)
            with_fennel=false;;

        --with-aio-required)
            with_aio=true;;
        --without-aio-required)
            with_aio=false;;
            
        --with?(out)-optimization) FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        --with?(out)-debug) 
            FARRAGO_DIST_FLAGS="${FARRAGO_DIST_FLAGS} $1";
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;

        # We match all the possibilities here so that we don't get a 
        # confusing usage message from Farrago's initBuild.sh
        --with-repos-type=@(default|mysql/hibernate|hsqldb/hibernate|psql/netbeans|hsqldb/netbeans))
            with_repos_type=true
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;

        --skip-fennel-thirdparty-build|--without-fennel-thirdparty-build) 
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;
        --skip-fennel-build|--without-fennel-build) 
            FARRAGO_FLAGS="${FARRAGO_FLAGS} $1";;

        *) echo "Unknown option: $1"; usage; exit -1;;
    esac

    shift
done

shopt -uq extglob

if $with_fennel ; then
    FARRAGO_FLAGS="${FARRAGO_FLAGS} --with-fennel"
else
    FARRAGO_FLAGS="${FARRAGO_FLAGS} --without-fennel"
fi

if $with_aio ; then
    FARRAGO_FLAGS="${FARRAGO_FLAGS} --with-aio-required"
else
    FARRAGO_FLAGS="${FARRAGO_FLAGS} --without-aio-required"
fi

if $with_nightly_tests ; then
    # make the build/test processes go further
    set +e
    run_ant="ant -keep-going"
else
    run_ant="ant"
fi

if $without_farrago_build ; then
    echo Skipping Farrago build.
    if $with_repos_type; then
        echo "** Ignoring --with-repos-type"
    fi
else
    cd ${luciddb_dir}/../farrago
    ./initBuild.sh ${FARRAGO_FLAGS}
fi

set -v

# set up Farrago build environment
cd ${luciddb_dir}/../farrago
. farragoenv.sh `pwd`/../thirdparty

# build applib
cd ${luciddb_dir}/../extensions/applib
ant clean jar

# set up Fennel's LD_LIBRARY_PATH
export LD_LIBRARY_PATH=
. ${luciddb_dir}/../fennel/fennelenv.sh ${luciddb_dir}/../fennel

# Build catalog then run tests
cd ${luciddb_dir}
${run_ant} clean
if $without_tests ; then
    ${run_ant} createCatalog
else
    cd ${luciddb_dir}
    ${run_ant} test
fi

if $without_dist_build ; then
    echo Skipping distribution build.
else
    cd ${luciddb_dir}/../farrago
    if [ ! -e ./dist/FarragoRelease.properties ]; then
        cp -f ./dist/ExampleRelease.properties ./dist/FarragoRelease.properties
    fi
    cd ${luciddb_dir}
    ./distBuild.sh --without-init-build ${FARRAGO_DIST_FLAGS}
fi

nightlylog_dir=${luciddb_dir}/nightlylog
nightly_test_list="\
test-nondb \
test-nondb-concurrency \
test-nondb-backupRestore \
test-nondb-concurrency-backupRestore \
test-flatfile \
test-oracle \
test-sqlserver \
test-csvjdbc\
"

cd ${luciddb_dir}
if $with_nightly_tests ; then
    /bin/mkdir -p ${nightlylog_dir}
    ${run_ant} -keep-going test-nightly-all-init

    for test in ${nightly_test_list}; do
        2>&1 ${run_ant} -keep-going "${test}" | tee "${nightlylog_dir}/${test}.log"
    done
fi
