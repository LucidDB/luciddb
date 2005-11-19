# Define variables needed by runtime scripts such as farragoServer.
# This script is meant to be sourced from other scripts, not
# executed directly.

SESSION_FACTORY="class:com.lucidera.farrago.LucidDbSessionFactory"

SERVER_JAVA_ARGS="-ea -esa -cp classes:`cat ../farrago/classpath.gen` \
  -Dnet.sf.farrago.home=../farrago \
  -Dnet.sf.farrago.catalog=./catalog \
  -Djava.util.logging.config.file=trace/LucidDbTrace.properties \
  -Dnet.sf.farrago.defaultSessionFactoryLibraryName=${SESSION_FACTORY}"

# TODO:  trim this
CLIENT_JAVA_ARGS=${SERVER_JAVA_ARGS}

SQLLINE_JAVA_ARGS="sqlline.SqlLine"
