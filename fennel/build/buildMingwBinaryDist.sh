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

# REVIEW jvs 29-June-2004:  I doubt this script works any more.

source ./defineVariables.sh

set -e
set -v

rm -rf farragoMingw
rm -f farragoMingw.zip
mkdir farragoMingw
cp ${STLPORT_LOCATION}/lib/lib${STLPORT_LIB}.dll.4.5 farragoMingw
cp ${BOOST_DIR}/lib/lib${BOOST_THREADLIB}.dll farragoMingw
cp ../libfennel/.libs/*.dll farragoMingw
cp ../farrago/.libs/cygfarrago-0.dll farragoMingw
cp /usr/bin/mingwm10.dll farragoMingw
cd farragoMingw
strip -g *
cd ..
zip -r farragoMingw.zip farragoMingw
rm -rf farragoMingw
