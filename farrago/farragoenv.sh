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
    export PATH=$ANT_HOME/bin:$PATH
fi
