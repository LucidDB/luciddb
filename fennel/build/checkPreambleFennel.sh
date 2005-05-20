#!/bin/bash
# $Id$
# Check the preamble of fennel files.

set -e

fennelDir=$(dirname $0)/..

# Check preambles of Eigenbase ('green zone') files.
# These are all files under fennel except those under
# fennel/disruptivetech or fennel/lucidera.
# They must have Eigenbase, Disruptive Tech and Red
# Square copyright notices.

/usr/bin/find $fennelDir \( -name \*.cpp -o -name \*.h \) |
grep -v -F \
'disruptivetech
lucidera
build/wshack.cpp
common/FemGeneratedEnums.h
common/FennelResource.cpp
common/FennelResource.h
config.h
farrago/FemGeneratedClasses.h
farrago/FemGeneratedMethods.h
farrago/JniPseudoUuid.h
farrago/NativeMethods.h' |
    xargs $fennelDir/build/checkPreamble.sh -fennel -eigenbase

# Check preambles of Red Square files.
# These are all files under fennel/lucidera.
# They must have Eigenbase and Red Square copyright notices.
/usr/bin/find $fennelDir/lucidera \( -name \*.cpp -o -name \*.h \) |
    xargs $fennelDir/build/checkPreamble.sh -fennel -redsquare

# Check preambles of Disruptive Tech files.
# These are all files under fennel/disruptivetech.
# They must have Eigenbase and Disruptive Tech copyright notices.
/usr/bin/find $fennelDir/disruptivetech \( -name \*.cpp -o -name \*.h \) |
grep -v -F 'disruptivetech/calc/CalcGrammar.cpp
disruptivetech/calc/CalcGrammar.h
disruptivetech/calc/CalcLexer.cpp' |
    xargs $fennelDir/build/checkPreamble.sh -fennel -disruptivetech

# End checkPreambleFennel.sh
