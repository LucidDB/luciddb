#!/bin/bash
# $Id$

usage() {
    echo "Usage:  initBuild.sh [--with[out]-farrago] [--with[out]-icu] [--with[out]-optimization] [--with[out]-debug] [--without[-fennel]-thirdparty-build] [--with[out]-tests] [--with[out]-aio-required]"
}

build_thirdparty=true
skip_tests=true
ICU_FLAG=
OPT_FLAG=--without-optimization
AIO_FLAG=

# extended globbing for case statement
shopt -sq extglob

while [ -n "$1" ]; do
    case $1 in
        --with?(out)-farrago) FARRAGO_FLAG="$1";;
        --with?(out)-icu) ICU_FLAG="$1";;
        --with?(out)-optimization) OPT_FLAG="$1";;
        --with?(out)-debug) DEBUG_FLAG="$1";;
        --skip?(-fennel)-thirdparty-build|--without?(-fennel)-thirdparty-build) 
            build_thirdparty=false;;
        --with-tests) skip_tests=false;;
        --without-tests) skip_tests=true;;
        --with-aio-required) AIO_FLAG="$1";;
        --without-aio-required) ;;
        *) usage; exit -1;;
    esac
    shift
done

# By default, debug is opposite of optimization
if [ -z "$DEBUG_FLAG" ]; then
    if [ "$OPT_FLAG" == "--with-optimization" ]; then
        DEBUG_FLAG="--without-debug"
    else
        DEBUG_FLAG="--with-debug"
    fi
fi

shopt -uq extglob

set -e
set -v

SAVE_PWD="$PWD"

# Unpack thirdparty components
cd ../thirdparty
make fennel

# Detect Cygwin
cygwin=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
esac

thirdparty_dir=`pwd`
if $cygwin ; then
    thirdparty_dir=$(cygpath -a -m ${thirdparty_dir})
    export JAVA_HOME=`cygpath -u $JAVA_HOME`
fi

# the default  is to disable ICU
if [ "$ICU_FLAG" == "" ] ; then
   ICU_FLAG="--without-icu";
fi

if [ "$ICU_FLAG" == "--with-icu" ] ; then
    ICU_CONF="--with-icu=${thirdparty_dir}/../thirdparty/icu"
fi

CMAKE_FLAGS="-Dboost_location=${thirdparty_dir}/boost \
    -Dstlport_location=${thirdparty_dir}/stlport \
    -DOPT_FLAG=$OPT_FLAG -DDEBUG_FLAG=$DEBUG_FLAG -DAIO_FLAG=$AIO_FLAG"

if [ "$FARRAGO_FLAG" == "--with-farrago" ] ; then
    CMAKE_FLAGS="$CMAKE_FLAGS -Dwith_farrago=TRUE"
fi

# Configure Fennel
cd "$SAVE_PWD"
rm -rf CMakeFiles cmake_install.cmake CMakeCache.txt
if $cygwin ; then
    cmake $CMAKE_FLAGS -G "NMake Makefiles" .
    # NOTE jvs 6-Apr-2009:  the cmake kludge for Windows touches
    # 0-sized .cpp.obj files in order to satisfy bogus dependencies,
    # so we need to delete them now before attempting the real build
    rm `find . -name '*.cpp.obj'`
    rm -f FlexLexer.h
    cp -f /usr/include/FlexLexer.h ./FlexLexer.h
    touch ./unistd.h
else
    cmake $CMAKE_FLAGS .
fi

# Build thirdparty libraries required by Fennel
if $build_thirdparty ; then
    cd build
    ./buildStlportLibs.sh
    ./buildBoostLibs.sh
    if [ $ICU_FLAG == "--with-icu" ] ; then
        ./buildICULibs.sh
    fi
    cd ..
fi

# Build Fennel itself
if $cygwin ; then
    nmake clean
    nmake
else
    make clean
    make
fi

if ! $skip_tests ; then
    # Set up Fennel runtime environment
    . fennelenv.sh `pwd`

if $cygwin ; then
    nmake check
else    
    make check
fi

fi
