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

cd ..

echo "*** Unique file extensions"
for file in `find . -name '*\\.*' -a ! -type d -print`;
do
  echo ${file##*.}
done | sort | uniq

echo "*** Line counts for green zone"
wc -l `find . \( -name '*.cpp' -o -name '*.h' -o -name '*.ypp' -o -name '*.lpp' \) -a ! -path '*disruptivetech*' -a ! -path '*lucidera*'`

echo "*** Line counts for SQLstream yellow zone"
wc -l `find disruptivetech -name '*.cpp' -o -name '*.h'`

echo "*** Line counts for LucidEra yellow zone"
wc -l `find lucidera -name '*.cpp' -o -name '*.h'`
