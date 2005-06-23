#!/bin/bash
# Build boost libraries required for fennel

source ./defineVariables.sh

set -e
set -v

# First, build bjam
cd ${BOOST_DIR}/tools/build/jam_src
./build.sh

# Locate bjam executable
BOOST_PLATFORM=bin.${BOOST_JAM_PLATFORM}
BJAM=${BOOST_DIR}/tools/build/jam_src/${BOOST_PLATFORM}/bjam

# Choose Boost build options
BUILD_OPTS="debug release <runtime-link>dynamic <threading>multi"
BUILD_OPTS="${BUILD_OPTS} <stlport-anachronisms>off"

BJAM_OPTS="--stagedir=${BOOST_DIR}"

# exclude the Boost libraries we don't want
#BJAM_OPTS="${BJAM_OPTS} --without-filesystem"
BJAM_OPTS="${BJAM_OPTS} --without-test"
BJAM_OPTS="${BJAM_OPTS} --without-signals"
BJAM_OPTS="${BJAM_OPTS} --without-python"
BJAM_OPTS="${BJAM_OPTS} --without-program_options"
BJAM_OPTS="${BJAM_OPTS} --without-serialization"

# export variables to control bjam
export BUILD="${BUILD_OPTS}"
export TOOLS="${BOOST_TOOLSET}"
export STLPORT_ROOT="${STLPORT_LOCATION}"

if test "${TARGET_OS}" = "mingw32"
then
    export NOCYGWIN_STLPORT_LIB_ID=stlport_gcc
    # NOTE:  horrible hack
    sed -i -e 's:IRIX:CYGWIN*:g' \
        ${BOOST_DIR}/tools/build/v1/gcc-stlport-tools.jam
    set -e
fi

# Run the Boost build, staging the libraries into ${BOOST_DIR}/lib
cd ${BOOST_DIR}
${BJAM} ${BJAM_OPTS} stage
