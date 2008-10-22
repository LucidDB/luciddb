# $Id$
# Define variables needed by runtime scripts such as farragoServer.
# This script is meant to be sourced from other scripts, not
# executed directly.

MAIN_DIR=$(cd `dirname $0`/..; pwd)

if [ ! -e "$MAIN_DIR/bin/classpath.gen" ]; then
    echo "Error:  $MAIN_DIR/install/install.sh has not been run yet."
    exit -1
fi

JAVA_ARGS="-Xms256m -Xmx256m -cp `cat $MAIN_DIR/bin/classpath.gen` \
  -Dnet.sf.farrago.home=$MAIN_DIR \
  -Dorg.eigenbase.util.AWT_WORKAROUND=off \
  -Djava.util.logging.config.file=$MAIN_DIR/trace/Trace.properties"

SQLLINE_JAVA_ARGS="sqlline.SqlLine"

JAVA_EXEC=${JAVA_HOME}/bin/java

export LD_LIBRARY_PATH=$MAIN_DIR/plugin:$MAIN_DIR/lib/fennel
