#!/bin/bash
# Build a binary Mingw distribution

# REVIEW jvs 29-June-2004:  I doubt this script works any more.

source ./defineVariables.sh

set -e
set -v

rm -rf farragoMingw
rm -f farragoMingw.zip
mkdir farragoMingw
cp ${STLPORT_LOCATION}/lib/lib${STLPORT_LIB}.dll.4.5 farragoMingw
cp ${BOOST_DIR}/lib/lib${BOOST_THREADLIB}.dll farragoMingw
cp ../libfennel/.libs/*.dll farragoMingw
cp ../farrago/.libs/cygfarrago-0.dll farragoMingw
cp /usr/bin/mingwm10.dll farragoMingw
cd farragoMingw
strip -g *
cd ..
zip -r farragoMingw.zip farragoMingw
rm -rf farragoMingw
