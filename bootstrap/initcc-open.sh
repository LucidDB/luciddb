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


if [ "!" -d farrago ]; then
    mkdir farrago
fi

if [ "!" -d fennel ]; then
    mkdir fennel
fi

if [ "!" -d saffron ]; then
    mkdir saffron
fi

if [ "!" -d logs ]; then
    mkdir logs
    mkdir logs/farrago
    mkdir logs/fennel
    mkdir logs/saffron
fi

if [ "!" -d artifacts ]; then
    mkdir artifacts
    mkdir artifacts/farrago${configSuffix}
    mkdir artifacts/fennel${configSuffix}
    mkdir artifacts/saffron${configSuffix}
fi

if [ "!" -d build ]; then
    mkdir build
    mkdir build/farrago
    mkdir build/fennel
    mkdir build/saffron
    mkdir build/thirdparty
fi

if [ "!" -d ant ]; then
    echo "Cruise Control requires an installed copy of ant in its working directory."
    echo "The best version to use is the one stored in //open/dev/thirdparty"
    exit 1;
fi

