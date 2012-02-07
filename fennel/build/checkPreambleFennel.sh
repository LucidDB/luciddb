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
