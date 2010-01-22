#!/bin/bash
# Build boost libraries required for fennel

source ./defineVariables.sh

set -e
set -v

cd ${BOOST_DIR}
./configure --prefix=${BOOST_DIR} \
    --with-libraries=date_time,filesystem,regex,system,thread,task,test \
    --without-icu
echo "using stlport : 5.1.6 : ${STLPORT_LOCATION}/stlport ${STLPORT_LOCATION}/lib ;" >> ${BOOST_DIR}/user-config.jam
echo "stage: .dummy" >> ${BOOST_DIR}/Makefile
echo '	@$(BJAM) $(BJAM_CONFIG) --user-config=user-config.jam --prefix=$(prefix) --exec-prefix=$(exec_prefix) --stagedir=$(prefix) $(LIBS) stage || echo "Not all Boost libraries built properly."' >> ${BOOST_DIR}/Makefile

if test "${TARGET_OS}" = "win32"
then
    make BJAM_CONFIG="toolset=${BOOST_TOOLSET} target-os=windows threadapi=win32 variant=debug,release link=shared runtime-link=shared stdlib=stlport --layout=system" stage
else
    if test "${TARGET_OS}" = "darwin"
    then
        make BJAM_CONFIG="toolset=darwin variant=debug,release link=shared runtime-link=shared stdlib=stlport define=_REENTRANT --layout=system" stage
    else
        make BJAM_CONFIG="toolset=${BOOST_TOOLSET} variant=debug,release link=shared runtime-link=shared stdlib=stlport --layout=system" stage
    fi
fi
