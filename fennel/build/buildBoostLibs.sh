#!/bin/bash
# Build boost libraries required for fennel

source ./defineVariables.sh

set -e
set -v

# First, build bjam
BOOST_PLATFORM=bin.${BOOST_JAM_PLATFORM}
BJAM=${BOOST_DIR}/tools/build/jam_src/${BOOST_PLATFORM}/bjam
BUILD_OPTS="debug release <runtime-link>dynamic <threading>multi"
BUILD_OPTS="${BUILD_OPTS} <stlport-anachronisms>off"
cd ${BOOST_DIR}/tools/build/jam_src
./build.sh

if test "${TARGET_OS}" = "mingw32"
then
    export NOCYGWIN_STLPORT_LIB_ID=1
fi

# Next, generate (but do not exec) a script to build Boost
cd ${BOOST_DIR}
${BJAM} -n -obuildBoost2.sh "-sTOOLS=${BOOST_TOOLSET}" "-sBUILD=${BUILD_OPTS}" \
    "-sSTLPORT_ROOT=${STLPORT_LOCATION}" \
    boost_thread

# Transform the script to keep path lengths under control for Cygwin
sed -e 's:stlport-anachronisms-off/stlport-cstd-namespace-std/stlport-debug-alloc-off/stlport-iostream-on/stlport-version-[0-9\.]*:stlport-opts:g' \
    buildBoost2.sh > buildBoost3.sh

# And apply some platform-specific tweaks

if test "${TARGET_OS}" = "cygwin"
then
    rm buildBoost2.sh
    mv buildBoost3.sh buildBoost2.sh
    sed -e 's:-lstlport_gcc:-lstlport_cygwin:g' \
        buildBoost2.sh > buildBoost3.sh
fi

# Finally, execute the script
set +e
. buildBoost3.sh

# TODO:  figure out why release build has unresolved externals on mingw;
# debug build seems fine; that's why error checking is disabled above
