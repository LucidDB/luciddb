#!/bin/bash
# Build STLport libraries required for fennel

source ./defineVariables.sh

set -e
set -v

ICU_DIR=${ICU_LOCATION}

# NOTE: ICU does not have, as of 2.8 a mingw port. Therefore use
# NOTE: the pre-compiled MSVC libraries. Cygwin port works fine
# NOTE: as long as the patched version of 2.8 is used. -JKalucki 4/2004

if test "${TARGET_OS}" = "cygwin"
then
    cd ${ICU_DIR}/source
    # prefix causes libs and includes to be installed in same dir
    ./runConfigureICU CygWin --prefix=${ICU_DIR}
    make 
    make install
elif test "${TARGET_OS}" = "mingw32"
then
    # nothing to do
    cd ${ICU_DIR}
else
    cd ${ICU_DIR}/source
    # prefix causes libs and includes to be installed in same dir
    ./runConfigureICU LinuxRedHat --prefix=${ICU_DIR}
    make 
    make install
fi
