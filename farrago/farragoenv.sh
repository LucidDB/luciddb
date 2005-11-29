# $Id$
# Script to set properties for locations of third-party
# components needed by Farrago.  Environment variables which are already set 
# are not modified.

if [ -z "$1" ]
then
    echo "Usage:  . farragoenv.sh /path/to/open/thirdparty"
    return
fi

if [ ! -e "$1/build.properties" ]
then
    echo "Usage:  . farragoenv.sh /path/to/open/thirdparty"
    echo "build.properties not found in $1"
fi

if [ -z "$JAVA_HOME" ]
then
    echo 'You must define environment variable JAVA_HOME'
    return
fi

THIRDPARTY_HOME=$1

if [ -z "$ANT_HOME" ]; then
    # if ANT_HOME is unset, use the one under thirdparty
    export ANT_HOME=$THIRDPARTY_HOME/ant
    export PATH=$ANT_HOME/bin:$PATH
else
    # REVIEW jvs 19-Nov-2005: disabling this to see if it's
    # breaking LucidEra CruiseControl
    
    # otherwise, prepend ANT if not already present on PATH
    # ANT_BIN=$(cd $ANT_HOME/bin; pwd)
    # CURR_ANT=$(dirname `/usr/bin/which ant 2>&1 | cut -d " " -f 1`)
    # if [ "$CURR_ANT" != "$ANT_BIN" ]; then
    #    export PATH=$ANT_BIN:$PATH
    # fi
    # unset -v ANT_BIN CURR_ANT
    export PATH=$ANT_HOME/bin:$PATH
fi
