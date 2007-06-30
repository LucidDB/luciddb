#!/bin/bash
# Gather source code metrics such as lines of code
# Run this against a pristine source tree (not after a build has polluted it)

cd ${FENNEL_HOME}

echo "*** Unique file extensions"
for file in `find . -name '*\\.*' -a ! -type d -print`;
do
  echo ${file##*.}
done | sort | uniq

echo "*** Line counts for green zone"
wc -l `find . \( -name '*.cpp' -o -name '*.h' -o -name '*.ypp' -o -name '*.lpp' \) -a ! -path '*disruptivetech*' -a ! -path '*lucidera*'`

echo "*** Line counts for Disruptive Tech yellow zone"
wc -l `find disruptivetech -name '*.cpp' -o -name '*.h'`

echo "*** Line counts for LucidEra yellow zone"
wc -l `find lucidera -name '*.cpp' -o -name '*.h'`
