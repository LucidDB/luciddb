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

. setenv.sh
for mod in mondrian bootstrap extensions farrago fennel firewater luciddb thirdparty;
do
  if [ $# -gt 0 ] && [ "$mod" != "$1" ]; then
    continue
  fi

  cd $mod
  git-p4 rebase
  git repack -ad
  git push origin master
  git push --tags
  if [ "$mod" != "mondrian" ]; then
    cd ../eigenbase/
    git submodule update $mod
    cd $mod
    git remote update
    git rebase origin/master
    cd ..
    git add $mod # warning: don't add a /
    git status
    git commit -m "Updated submodule $mod."
    git push origin master
    git push --tags
  fi
  cd ..
done
