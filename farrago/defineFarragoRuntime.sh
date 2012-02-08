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

BASE_JAVA_ARGS="-ea -esa -cp `cat classpath.gen` \
  -Dnet.sf.farrago.home=. \
  -Djava.util.logging.config.file=trace/FarragoTrace.properties"

SERVER_JAVA_ARGS="-Xss768K ${BASE_JAVA_ARGS}"

# TODO:  trim this
CLIENT_JAVA_ARGS=${BASE_JAVA_ARGS}

SQLLINE_JAVA_ARGS="sqlline.SqlLine"
