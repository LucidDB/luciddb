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

if [ -z "$1" ]
then
    echo "Usage:  . farragoenv.sh /path/to/open/thirdparty"
    return
fi

if [ ! -e "$1/build.properties" ]
then
    echo "Usage:  . farragoenv.sh /path/to/open/thirdparty"
    echo "build.properties not found in $1"
fi

if [ -z "$JAVA_HOME" ]
then
    echo 'You must define environment variable JAVA_HOME'
    return
fi

THIRDPARTY_HOME=$1

if [ -z "$ANT_HOME" ]; then
    # if ANT_HOME is unset, use the one under thirdparty
    export ANT_HOME=$THIRDPARTY_HOME/ant
    export PATH=$ANT_HOME/bin:$PATH
else
    export PATH=$ANT_HOME/bin:$PATH
fi
