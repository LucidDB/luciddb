#!/bin/bash
# Build STLport libraries required for fennel: modified for stlport 5

source ./defineVariables.sh

set -e
set -v

GCC_VER=`g++ --version | head -n 1 | cut -f 3 -d ' '`
STLPORT_DIR=${STLPORT_LOCATION}
MINGW_CFLAGS="-mno-cygwin -D_STLP_THREADS"

if [ -e ${STLPORT_DIR}/build ]
then                                    # stlport version 5
  TARGET_LIST="clean install-stldbg-shared install-release-shared"
  cd ${STLPORT_DIR}/build/lib
  make SHELL=/bin/bash -f gcc.mak ${TARGET_LIST}

  # link like version 4
  # TODO: instead, configure fennel to link appropriately
  cd ${STLPORT_DIR}/lib
  ln -s -f libstlport.so      libstlport_gcc.so
  ln -s -f libstlportstlg.so  libstlport_gcc_stldebug.so

else                                    # stlport version 4
# This is a workaround for STLport, which makes some unwarranted
# assumptions about g++ 3.x installation locations.
# If it doesn't work for you, you'll have to
# fix it manually by symlinking the correct location for the ctime header.
  rm -f ${STLPORT_LOCATION}/g++-v3
  rm -f ${STLPORT_LOCATION}/${GCC_VER}
  ln -s /usr/include/c++/3.* ${STLPORT_LOCATION}/g++-v3
  ln -s /usr/include/c++/3.* ${STLPORT_LOCATION}/${GCC_VER}

  TARGET_LIST="clean stldebug_dynamic release_dynamic symbolic_links"  

  # NOTE:  this script implements the procedure from
  # http://www.boost.org/tools/build/gcc-nocygwin-tools.html

  if test "${TARGET_OS}" = "cygwin"
  then
      cd ${STLPORT_DIR}/src
      make -f gcc-cygwin.mak \
          ${TARGET_LIST} \
          DYN_LINK="g++ -shared -o"
  elif test "${TARGET_OS}" = "mingw32"
  then
      # NOTE jvs 30-June-2004:  read wshack.cpp for why this is necessary
      if test ! -e ${STLPORT_DIR}/src/dll_main.orig
      then
          cp ${STLPORT_DIR}/src/dll_main.cpp ${STLPORT_DIR}/src/dll_main.orig
          cat wshack.cpp >> ${STLPORT_DIR}/src/dll_main.cpp
      fi
      cd ${STLPORT_DIR}/src
      make -f gcc-mingw32.mak \
          ${TARGET_LIST} \
          CC="gcc ${MINGW_CFLAGS}" CXX="g++ ${MINGW_CFLAGS}" \
          DYN_LINK="g++ -shared -mno-cygwin -o" LIB_BASENAME="libstlport_gcc"
  else
      cd ${STLPORT_DIR}/src
      make -f gcc.mak ${TARGET_LIST}
  fi
fi

