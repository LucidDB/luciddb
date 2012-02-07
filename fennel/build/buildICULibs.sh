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

ICU_DIR=${ICU_LOCATION}

# NOTE: ICU does not have, as of 2.8 a mingw port. Therefore use
# NOTE: the pre-compiled MSVC libraries. Cygwin port works fine
# NOTE: as long as the patched version of 2.8 is used. -JKalucki 4/2004

if test "${TARGET_OS}" = "cygwin"
then
    cd ${ICU_DIR}/source
    # prefix causes libs and includes to be installed in same dir
    ./runConfigureICU CygWin --prefix=${ICU_DIR}
    make 
    make install
elif test "${TARGET_OS}" = "mingw32"
then
    # nothing to do
    cd ${ICU_DIR}
else
    cd ${ICU_DIR}/source
    # prefix causes libs and includes to be installed in same dir
    ./runConfigureICU LinuxRedHat --prefix=${ICU_DIR}
    make 
    make install
fi
