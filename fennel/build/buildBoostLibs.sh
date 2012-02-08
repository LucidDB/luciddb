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

source ./defineVariables.sh

set -e
set -v

cd ${BOOST_DIR}
if test "${TARGET_OS}" = "windows"
then
    ./bootstrap.bat --prefix=${BOOST_DIR} \
        --with-libraries=date_time,filesystem,regex,system,thread,test \
        --without-icu
else
    ./bootstrap.sh --prefix=${BOOST_DIR} \
        --with-libraries=date_time,filesystem,regex,system,thread,test \
        --without-icu
fi
echo "using stlport : 5.2.1 : ${STLPORT_LOCATION}/stlport : ${STLPORT_LOCATION}/lib ;" >> ${BOOST_DIR}/tools/build/v2/user-config.jam

if test "${TARGET_OS}" = "win32"
then
    export INCLUDE="$INCLUDE;."
    ./bjam toolset=${BOOST_TOOLSET} stdlib=stlport target-os=windows threadapi=win32 variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged  --stagedir=${BOOST_DIR}
else
    if test "${TARGET_OS}" = "darwin"
    then
	./bjam toolset=darwin-4.0 stdlib=stlport architecture=x86 target-os=darwin variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged --stagedir=${BOOST_DIR} cflags="-D_REENTRANT -Wno-long-long"
    else
        ./bjam toolset=${BOOST_TOOLSET} stdlib=stlport variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged --stagedir=${BOOST_DIR} cflags=-Wno-long-long
    fi
fi
