#!/bin/bash
# $Id$

usagemsg="Usage:  initBuild.sh [--with[out]-farrago] [--with[out]-icu] [--with[out]-optimization] [--skip-thirdparty]";

if [ "$1" == "--with-farrago" -o "$1" == "--without-farrago" ] ; then
    FARRAGO_FLAG=$1
    shift
fi

if [ "$1" == "--with-icu" -o "$1" == "--without-icu" ] ; then
    # override default
    ICU_FLAG=$1
    shift
else
    # use default
    ICU_FLAG=
fi

if [ "$1" == "--with-optimization" -o "$1" == "--without-optimization" ] ; then
    OPT_FLAG=$1
    shift
else
    # default to unoptimized build
    OPT_FLAG="--without-optimization"
fi

if [ "$1" == "--skip-thirdparty" ] ; then
    build_thirdparty=false
    shift
else
    build_thirdparty=true
    shift
fi

if [ -n "$1" ] ; then
    echo $usagemsg;
    exit -1;
fi

set -e
set -v

SAVE_PWD="$PWD"

# Check automake/libtool/autoconf versions
AUTOMAKE_VERSION=$(automake --version | awk '{print $4; exit}')
case $AUTOMAKE_VERSION in
1.8*) ;;
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
case $AUTOCONF_VERSION in
2.57*) ;;
2.58*) ;;
2.59*) ;;
*)
    echo "Invalid autoconf version '$AUTOCONF_VERSION'."
    echo "To fix, please run 'make autoconf' under thirdparty,"
    echo "then as root, 'make install' under thirdparty/autoconf."
    exit -1
    ;;
esac

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
    #default to no ICU for Cygwin/Mingw
    ICU_FLAG="--without-icu";
fi

# the default for non-Cygwin/Mingw is to enable ICU
if [ "$ICU_FLAG" == "" ] ; then
   ICU_FLAG="--with-icu";
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
    $FARRAGO_FLAG $ICU_CONF $MINGW32_TARGET $OPT_FLAG

if $cygwin ; then
    unset CC
    unset CXX
fi

# Build thirdparty libaries required by Fennel
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
