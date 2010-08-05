#!/bin/sh

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
