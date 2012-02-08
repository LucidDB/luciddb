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

SESSION_FACTORY="class:org.luciddb.session.LucidDbSessionFactory"

SERVER_JAVA_ARGS="-Xms512m -Xmx512m -ea -esa \
  -cp classes:`cat ../farrago/classpath.gen` \
  -Dnet.sf.farrago.home=. \
  -Dnet.sf.farrago.catalog=./catalog \
  -Djava.util.logging.config.file=trace/LucidDbTrace.properties \
  -Dnet.sf.farrago.defaultSessionFactoryLibraryName=${SESSION_FACTORY}"

CLIENT_JAVA_ARGS="-ea -esa -cp plugin/LucidDbClient.jar:../thirdparty/sqlline.jar:../thirdparty/jline.jar:../thirdparty/hsqldb/lib/hsqldb.jar -Djava.util.logging.config.file=trace/LucidDbTrace.properties"

SQLLINE_JAVA_ARGS="sqlline.SqlLine"
