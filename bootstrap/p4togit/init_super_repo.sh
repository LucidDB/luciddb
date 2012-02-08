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

if [ -d eigenbase ]; then
 exit
fi

set -x
mkdir eigenbase
cd eigenbase
git init
for mod in bootstrap extensions farrago fennel firewater luciddb thirdparty;
do
  git submodule add git://github.com/eigenbase/$mod.git $mod
done
git status
echo "Commit? [Ctrl+C to stop]"
read
git commit -m "Adding all the submodules"
git remote add origin git@github.com:eigenbase/eigenbase.git
git push origin master
git submodule init
