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

MAIN_DIR=$(cd `dirname $0`/..; pwd)

if [ ! -e "$MAIN_DIR/bin/classpath.gen" ]; then
    echo "Error:  $MAIN_DIR/install/install.sh has not been run yet."
    exit -1
fi

# If you are trying to give additional memory usable by queries
# see this doc: http://pub.eigenbase.org/wiki/LucidDbBufferPoolSizing
# Upping Java Heap will unlikely help queries on "large" datasets
JAVA_MEM="-Xms256m -Xmx256m -XX:MaxPermSize=128m"
JAVA_ARGS_CLIENT="-cp `cat $MAIN_DIR/bin/classpath.gen` \
  -Dnet.sf.farrago.home=$MAIN_DIR \
  -Dorg.eigenbase.util.AWT_WORKAROUND=off \
  -Djava.util.logging.config.file=$MAIN_DIR/trace/Trace.properties"
JAVA_ARGS="$JAVA_MEM $JAVA_ARGS_CLIENT"

SQLLINE_JAVA_ARGS="sqlline.SqlLine"

JAVA_EXEC=${JAVA_HOME}/bin/java

if [ `uname` = "Darwin" ]; then
    export DYLD_LIBRARY_PATH=$MAIN_DIR/plugin:$MAIN_DIR/lib/fennel
    JAVA_ARGS="$JAVA_ARGS -d32"
else
    export LD_LIBRARY_PATH=$MAIN_DIR/plugin:$MAIN_DIR/lib/fennel
fi
