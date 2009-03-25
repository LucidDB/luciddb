#!/bin/bash
# $Id$
# Check the preamble of fennel files.

set -e

fennelDir=$(dirname $0)/..

# Check preambles of Eigenbase ('green zone') files.
# These are all files under fennel except those under
# fennel/lucidera.
# They must have Eigenbase, SQLstream and LucidEra
# copyright notices.

/usr/bin/find $fennelDir \( -name \*.cpp -o -name \*.h \) |
grep -v -F \
'lucidera
build/wshack.cpp
common/FemGeneratedEnums.h
common/FennelResource.cpp
common/FennelResource.h
config.h
calculator/CalcGrammar.h
calculator/CalcGrammar.cpp
calculator/CalcLexer.cpp
farrago/FemGeneratedClasses.h
farrago/FemGeneratedMethods.h
farrago/JniPseudoUuid.h
farrago/NativeMethods.h' |
    xargs $fennelDir/build/checkPreamble.sh -fennel -eigenbase

# Check preambles of LucidEra files.
# These are all files under fennel/lucidera.
# They must have Eigenbase and LucidEra copyright notices.
/usr/bin/find $fennelDir/lucidera \( -name \*.cpp -o -name \*.h \) |
    xargs $fennelDir/build/checkPreamble.sh -fennel -lucidera

# End checkPreambleFennel.sh
