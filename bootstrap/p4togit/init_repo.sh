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

if [ $# -lt 2 ]; then
  echo "Usage: init_repo.sh git_repo_name path_to_p4_repo"
  echo "e.g. init_repo.sh luciddb //open/dev/luciddb"
  exit
fi

. setenv.sh
set -x
git_repo=$1
p4_repo=$2
mkdir $git_repo
cd $git_repo
git init
git-p4 sync $p4_repo@all --detect-labels
git repack -a -d -f
git remote add origin git@github.com:eigenbase/$git_repo.git
git pull . remotes/p4/master
set +x
echo "Ready to push? [Ctrl+C to stop]"
read
git push origin master
git push --tags
