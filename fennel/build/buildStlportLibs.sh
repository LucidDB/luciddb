#!/bin/bash
# Build STLport libraries required for fennel

source ./defineVariables.sh

set -e
set -v

# This is a workaround for STLport, which makes some unwarranted
# assumptions about g++ 3.x installation locations.
# If it doesn't work for you, you'll have to
# fix it manually by symlinking the correct location for the ctime header.
rm -f ${STLPORT_LOCATION}/g++-v3
rm -f ${STLPORT_LOCATION}/3.3.2
ln -s /usr/include/c++/3.* ${STLPORT_LOCATION}/g++-v3
ln -s /usr/include/c++/3.* ${STLPORT_LOCATION}/3.3.2

STLPORT_DIR=${STLPORT_LOCATION}
MINGW_CFLAGS="-mno-cygwin -D_STLP_NO_STATIC_TEMPLATE_DATA -D_STLP_THREADS"

# NOTE:  this script implements the procedure from
# http://www.boost.org/tools/build/gcc-nocygwin-tools.html

if test "${TARGET_OS}" = "cygwin"
then
    cd ${STLPORT_DIR}/src
    make -f gcc-cygwin.mak clean all_dynamic symbolic_links \
        DYN_LINK="g++ -shared -o"
elif test "${TARGET_OS}" = "mingw32"
then
    cd ${STLPORT_DIR}/src
    make -f gcc-cygwin.mak clean all_dynamic symbolic_links \
        DYN_LINK="g++ -shared -mno-cygwin -o" CC="gcc ${MINGW_CFLAGS}" \
        CXX="g++ ${MINGW_CFLAGS}" LIB_BASENAME="libstlport_gcc"
else
    cd ${STLPORT_DIR}/src
    make -f gcc.mak clean all_dynamic symbolic_links
fi
