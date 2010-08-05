#!/bin/sh

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
