# Define variables needed by runtime scripts such as farragoServer.
# This script is meant to be sourced from other scripts, not
# executed directly.

BASE_JAVA_ARGS="-ea -esa -cp `cat classpath.gen` \
  -Dnet.sf.farrago.home=. \
  -Djava.util.logging.config.file=trace/FarragoTrace.properties"

SERVER_JAVA_ARGS="-server -Xss768K ${BASE_JAVA_ARGS}"

# TODO:  trim this
CLIENT_JAVA_ARGS=${BASE_JAVA_ARGS}

SQLLINE_JAVA_ARGS="sqlline.SqlLine"
