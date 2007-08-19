#!/bin/bash
# $Id$

usage() {
    echo "Usage:  initBuild.sh [--with[out]-farrago] [--with[out]-icu] [--with[out]-optimization] [--with[out]-debug] [--without[-fennel]-thirdparty-build] [--with[out]-tests]"
}

build_thirdparty=true
skip_tests=true
ICU_FLAG=
OPT_FLAG=--without-optimization

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

# Check automake/libtool/autoconf versions
AUTOMAKE_VERSION=$(automake --version | awk '{print $4; exit}')
case $AUTOMAKE_VERSION in
1.7.6*) ;;
1.7.8*) ;;
1.7.9*) ;;
1.8*) ;;
1.9*) ;;
1.10*) ;;
*)
    echo "Invalid automake version '$AUTOMAKE_VERSION'."
    echo "To fix, please run 'make automake' under thirdparty,"
    echo "then as root, 'make install' under thirdparty/automake."
    exit -1
    ;;
esac

LIBTOOL_VERSION=$(libtool --version | awk '{print $4; exit}')
case $LIBTOOL_VERSION in
1.5*) ;;
*)
    echo "Invalid libtool version '$LIBTOOL_VERSION'."
    echo "To fix, please run 'make libtool' under thirdparty,"
    echo "then as root, 'make install' under thirdparty/libtool."
    exit -1
    ;;
esac

AUTOCONF_VERSION=$(autoconf --version | awk '{print $4; exit}')
VALID_AUTOCONF=`echo "$AUTOCONF_VERSION >= 2.57" | bc`
if [ $VALID_AUTOCONF -ne 1 ]; then
    echo "Invalid autoconf version '$AUTOCONF_VERSION'."
    echo "Autoconf version must be 2.57 or later."
    echo "To fix, please run 'make autoconf' under thirdparty,"
    echo "then as root, 'make install' under thirdparty/autoconf."
    exit -1
fi

# Unpack thirdparty components
cd ../thirdparty
make fennel


# Detect Cygwin
cygwin=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
esac


if $cygwin ; then
    export CC="gcc -mno-cygwin"
    export CXX="gcc -mno-cygwin"
    MINGW32_TARGET="--target=mingw32"
    if [ "$ICU_FLAG" == "--with-icu" ] ; then
        # an explicit call for ICU
        echo "Error: ICU library not supported on Cygwin / Mingw"
        exit -1;
    fi
    export JAVA_HOME=`cygpath -u $JAVA_HOME`
fi

# the default  is to disable ICU
if [ "$ICU_FLAG" == "" ] ; then
   ICU_FLAG="--without-icu";
fi

if [ "$ICU_FLAG" == "--with-icu" ] ; then
    ICU_CONF="--with-icu=`pwd`/../thirdparty/icu"
fi

# Configure Fennel
cd "$SAVE_PWD"
rm -rf autom4te.cache
autoreconf --force --install
./configure --with-boost=`pwd`/../thirdparty/boost \
    --with-stlport=`pwd`/../thirdparty/stlport \
    $FARRAGO_FLAG $ICU_CONF $MINGW32_TARGET $OPT_FLAG $DEBUG_FLAG

if $cygwin ; then
    unset CC
    unset CXX
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
make clean
make

if ! $skip_tests ; then
    # Set up Fennel runtime environment
    . fennelenv.sh `pwd`

    make check
fi


