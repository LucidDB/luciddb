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

if [ "!" -z "$ANT_HOME" ]; then
    ant -f buildResources.xml execute.resgen;
else
    echo
    echo "In order to regenerate FennelResource code, Apache Ant must be"
    echo "installed, the ANT_HOME environment variable set, and the Eigenbase"
    echo "Resource Generator (ResGen) JAR files (eigenbase-resgen.jar and"
    echo "eigenbase-xom.jar) must be in your CLASSPATH environment variable."
    echo
    exit 1
fi
