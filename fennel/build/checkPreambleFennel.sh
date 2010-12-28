#!/bin/bash
# $Id$
# Check the preamble of fennel files.

set -e

fennelDir=$(dirname $0)/..

# Check preambles of Eigenbase ('green zone') files.
# These are all files under fennel.
# They must have Eigenbase, SQLstream and Dynamo BI
# copyright notices.

/usr/bin/find $fennelDir \( -name \*.cpp -o -name \*.h \) |
grep -v -F \
'common/FemGeneratedEnums.h
common/FennelResource.cpp
common/FennelResource.h
config.h
dummy.cpp
CMakeFiles/CompilerIdCXX/CMakeCXXCompilerId.cpp
calculator/CalcGrammar.h
calculator/CalcGrammar.tab.cpp
calculator/CalcGrammar.cpp
calculator/CalcLexer.cpp
farrago/FemGeneratedClasses.h
farrago/FemGeneratedMethods.h
farrago/JniPseudoUuid.h
farrago/NativeMethods.h' |
    xargs $fennelDir/build/checkPreamble.sh -fennel -eigenbase

# End checkPreambleFennel.sh
