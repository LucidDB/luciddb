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

INSTALL_DIR=$(cd `dirname $0`; pwd)
BASE_DIR=$(cd $INSTALL_DIR/..; pwd)
LIB_DIR=$BASE_DIR/lib
BIN_DIR=$BASE_DIR/bin
TRACE_DIR=$BASE_DIR/trace

cd $INSTALL_DIR

if [ -z "$JAVA_HOME" ]; then
    echo "The JAVA_HOME environment variable must be set to the location"
    echo "of a version 1.6 or higher JVM."
    exit 1;
fi

set -e
set -v

export LD_LIBRARY_PATH=$LIB_DIR/fennel

# configure tracing
mkdir $TRACE_DIR
cat >$TRACE_DIR/Trace.properties <<EOF
# Tracing configuration

handlers=java.util.logging.FileHandler
java.util.logging.FileHandler.append=true
java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter

java.util.logging.FileHandler.pattern=$TRACE_DIR/Trace.log

.level=CONFIG
EOF

LOCALCLASSPATH=$JAVA_HOME/lib/tools.jar
for lib in `find $LIB_DIR -path $LIB_DIR/plugin -not -prune -o -name "*.jar"`; do
  LOCALCLASSPATH=$LOCALCLASSPATH:$lib
done

cygwin=false
case "`uname`" in
    CYWGIN*) cygwin=true ;;
esac

if $cygwin; then
    LOCALCLASSPATH=`cygpath --path --windows "$LOCALCLASSPATH"`
fi

echo $LOCALCLASSPATH >$BIN_DIR/classpath.gen
