# $Id$
# Script to set properties for locations of third-party
# components needed by Farrago.  Environment variables which are already set 
# are not modified.

if [ -z "$1" ]
then
    echo "Usage:  . farragoenv.sh /path/to/open/thirdparty"
    return
fi

if [ ! -d "$1/ant" ]
then
    echo "Usage:  . farragoenv.sh /path/to/open/thirdparty"
    echo "ant not found in $1"
fi

if [ -z "$JAVA_HOME" ]
then
    echo 'You must define  environment variable JAVA_HOME'
    return
fi

THIRDPARTY_HOME=$1
[ -z "$ANT_HOME" ] && export ANT_HOME=$THIRDPARTY_HOME/ant

export PATH=$ANT_HOME/bin:$PATH
