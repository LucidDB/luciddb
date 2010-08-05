#!/bin/sh

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
