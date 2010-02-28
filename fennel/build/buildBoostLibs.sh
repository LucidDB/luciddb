#!/bin/bash
# Build boost libraries required for fennel

source ./defineVariables.sh

set -e
set -v

cd ${BOOST_DIR}
if test "${TARGET_OS}" = "windows"
then
    ./bootstrap.bat --prefix=${BOOST_DIR} \
        --with-libraries=date_time,filesystem,regex,system,thread,test \
        --without-icu
else
    ./bootstrap.sh --prefix=${BOOST_DIR} \
        --with-libraries=date_time,filesystem,regex,system,thread,test \
        --without-icu
fi
echo "using stlport : 5.1.6 : ${STLPORT_LOCATION}/stlport : ${STLPORT_LOCATION}/lib ;" >> ${BOOST_DIR}/tools/build/v2/user-config.jam

if test "${TARGET_OS}" = "win32"
then
    ./bjam toolset=${BOOST_TOOLSET} stdlib=stlport target-os=windows threadapi=win32 variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged  --stagedir=${BOOST_DIR}
else
    if test "${TARGET_OS}" = "darwin"
    then
	./bjam toolset=darwin-4.0 stdlib=stlport architecture=x86 target-os=darwin variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged --stagedir=${BOOST_DIR} cflags="-D_REENTRANT -Wno-long-long"
    else
        ./bjam toolset=${BOOST_TOOLSET} stdlib=stlport variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged --stagedir=${BOOST_DIR} cflags=-Wno-long-long
    fi
fi
