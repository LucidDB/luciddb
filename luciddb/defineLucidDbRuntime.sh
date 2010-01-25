# Define variables needed by runtime scripts such as farragoServer.
# This script is meant to be sourced from other scripts, not
# executed directly.

SESSION_FACTORY="class:org.luciddb.session.LucidDbSessionFactory"

SERVER_JAVA_ARGS="-Xms512m -Xmx512m -ea -esa \
  -cp classes:`cat ../farrago/classpath.gen` \
  -Dnet.sf.farrago.home=. \
  -Dnet.sf.farrago.catalog=./catalog \
  -Djava.util.logging.config.file=trace/LucidDbTrace.properties \
  -Dnet.sf.farrago.defaultSessionFactoryLibraryName=${SESSION_FACTORY}"

CLIENT_JAVA_ARGS="-ea -esa -cp plugin/LucidDbClient.jar:../thirdparty/sqlline.jar:../thirdparty/jline.jar:../thirdparty/hsqldb/lib/hsqldb.jar -Djava.util.logging.config.file=trace/LucidDbTrace.properties"

SQLLINE_JAVA_ARGS="sqlline.SqlLine"
