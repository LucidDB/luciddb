#!/bin/bash
# Build boost libraries required for fennel

source ./defineVariables.sh

set -e
set -v

cd ${BOOST_DIR}
if test "${TARGET_OS}" = "windows"
then
    ./bootstrap.bat --prefix=${BOOST_DIR} \
        --with-libraries=date_time,filesystem,regex,system,thread,task,test \
        --without-icu
else
    ./bootstrap.sh --prefix=${BOOST_DIR} \
        --with-libraries=date_time,filesystem,regex,system,thread,task,test \
        --without-icu
fi
echo "using stlport : 5.1.6 : ${STLPORT_LOCATION}/stlport : ${STLPORT_LOCATION}/lib ;" >> ${BOOST_DIR}/tools/build/v2/user-config.jam
#echo "stage: .dummy" >> ${BOOST_DIR}/Makefile
#echo '	@$(BJAM) $(BJAM_CONFIG) --user-config=user-config.jam --prefix=$(prefix) --exec-prefix=$(exec_prefix) --stagedir=$(prefix) $(LIBS) stage || echo "Not all Boost libraries built properly."' >> ${BOOST_DIR}/Makefile

if test "${TARGET_OS}" = "linux-gnu"
then
    ./bjam toolset=${BOOST_TOOLSET} stdlib=stlport variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged --stagedir=${BOOST_DIR} cflags=-Wno-long-long
else
    ./bjam toolset=${BOOST_TOOLSET} stdlib=stlport target-os=windows threadapi=win32 variant=debug,release link=shared runtime-link=shared threading=multi address-model=${CPU_BITS} --layout=tagged  --stagedir=${BOOST_DIR}
fi
