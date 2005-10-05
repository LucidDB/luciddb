# Define variables needed by runtime scripts such as farragoServer.
# This script is meant to be sourced from other scripts, not
# executed directly.

SERVER_JAVA_ARGS="-ea -esa -cp `cat classpath.gen` \
  -Dnet.sf.farrago.home=. \
  -Djava.util.logging.config.file=trace/FarragoTrace.properties"

# TODO:  trim this
CLIENT_JAVA_ARGS=${SERVER_JAVA_ARGS}

SQLLINE_JAVA_ARGS="sqlline.SqlLine"
