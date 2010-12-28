#!/bin/bash
# Build STLport libraries required for fennel: modified for stlport 5

source ./defineVariables.sh

set -e
set -v

GCC_VER=`g++ --version | head -n 1 | cut -f 3 -d ' '`
STLPORT_DIR=${STLPORT_LOCATION}

TARGET_LIST="clean install-stldbg-shared install-release-shared"
cd ${STLPORT_DIR}/build/lib
if test "${TARGET_OS}" = "win32"
then
    echo COMPILER_NAME=vc9 > ${STLPORT_DIR}/build/Makefiles/config.mak
    echo SELECTED_COMPILER=msvc >> ${STLPORT_DIR}/build/Makefiles/config.mak
    echo SELECTED_COMPILER_VERSION=90 >> ${STLPORT_DIR}/build/Makefiles/config.mak
    echo TARGET_OS=x86 >> ${STLPORT_DIR}/build/Makefiles/config.mak
    echo "#define _STLP_STATIC_CONST_INIT_BUG 1" > ${STLPORT_DIR}/stlport/stl/config/user_config.h
    nmake SHELL=/bin/bash TARGET_OS=x86 /fmsvc.mak ${TARGET_LIST}
else
    if test "${TARGET_OS}" = "darwin"
    then
       # echo "DEFS=-arch x86_64" > ${STLPORT_DIR}/build/Makefiles/config.mak
       echo "#define _STLP_NATIVE_INCLUDE_PATH /usr/include/c++/${GCC_VER}" > ${STLPORT_DIR}/stlport/stl/config/user_config.h
    fi
    make SHELL=/bin/bash -f gcc.mak ${TARGET_LIST}
fi

# hack for Boost/STLport disagreements
cd ${STLPORT_DIR}/lib
if test "${TARGET_OS}" = "win32"
then
    # MSVC toolchain does not like symlinks!
    cp stlport.5.1.lib stlport.lib
    cp stlportstld.5.1.lib stlportstlg.lib
    cp ../bin/*.dll .
    cp ../bin/*.pdb .
else
    if test "${TARGET_OS}" = "linux-gnu"
    then
        ln -s -f libstlport.so libstlport.5.1.so
        ln -s -f libstlportstlg.so libstlportstlg.5.1.so
    fi
fi
