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

GCC_VER=`g++ --version | head -n 1 | cut -f 3 -d ' '`
STLPORT_DIR=${STLPORT_LOCATION}

TARGET_LIST="clean install-stldbg-shared install-release-shared"
cd ${STLPORT_DIR}/build/lib
if test "${TARGET_OS}" = "win32"
then
    echo COMPILER_NAME=vc9 > ${STLPORT_DIR}/build/Makefiles/nmake/config.mak
    echo SELECTED_COMPILER=msvc >> ${STLPORT_DIR}/build/Makefiles/nmake/config.mak
    echo SELECTED_COMPILER_VERSION=90 >> ${STLPORT_DIR}/build/Makefiles/nmake/config.mak
    echo TARGET_OS=x86 >> ${STLPORT_DIR}/build/Makefiles/nmake/config.mak
    echo WINVER=0x0600 >> ${STLPORT_DIR}/build/Makefiles/nmake/config.mak
    echo "#define _STLP_STATIC_CONST_INIT_BUG 1" > ${STLPORT_DIR}/stlport/stl/config/user_config.h
    nmake SHELL=/bin/bash TARGET_OS=x86 /fmsvc.mak ${TARGET_LIST}
else
    if test "${TARGET_OS}" = "darwin"
    then
       # echo "DEFS=-arch x86_64" > ${STLPORT_DIR}/build/Makefiles/config.mak
       echo "#define _STLP_NATIVE_INCLUDE_PATH /usr/include/c++/${GCC_VER}" > ${STLPORT_DIR}/stlport/stl/config/user_config.h
    fi
    rm -rf ${STLPORT_DIR}/lib ${STLPORT_DIR}/include
    make INSTALL_D='mkdir -p' INSTALL_F='cp' BASE_INSTALL_DIR=${STLPORT_DIR} SHELL=/bin/bash -f gcc.mak ${TARGET_LIST}
fi

# hack for Boost/STLport disagreements
cd ${STLPORT_DIR}/lib
if test "${TARGET_OS}" = "win32"
then
    # MSVC toolchain does not like symlinks!
    cp stlport.5.2.lib stlport.lib
    cp stlportstld.5.2.lib stlportstlg.lib
    cp ../bin/*.dll .
    cp ../bin/*.pdb .
else
    if test "${TARGET_OS}" = "linux-gnu"
    then
        ln -s -f libstlport.so libstlport.5.2.so
        ln -s -f libstlportstlg.so libstlportstlg.5.2.so
    fi
fi
