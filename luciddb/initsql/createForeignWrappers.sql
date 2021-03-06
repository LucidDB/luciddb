-- create sql server jdbc wrapper with browse_connect
create or replace foreign data wrapper "SQL SERVER"
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java
options(
  browse_connect_description 'SQL Server Database Connection',
  driver_class 'net.sourceforge.jtds.jdbc.Driver',
  url 'jdbc:jtds:sqlserver://server:port',
  login_timeout '10',
  validation_query 'select 1',
  lenient 'true'
);

-- create flat file wrapper with browse_connect
create or replace foreign data wrapper "FLAT FILE"
library 'class net.sf.farrago.namespace.flatfile.FlatFileDataWrapper'
language java
options(
  browse_connect_description 'Flat File Connection'
);

-- create luciddb local wrapper with browse connect
create or replace foreign data wrapper "LUCIDDB LOCAL"
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java
options(
  browse_connect_description 'LucidDb Loopback Connection',
  driver_class 'org.luciddb.jdbc.LucidDbLocalDriver',
  url 'jdbc:luciddb:'
);

-- create luciddb remote wrapper with browse connect
create or replace foreign data wrapper "LUCIDDB REMOTE"
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java
options(
  browse_connect_description 'LucidDb Connection',
  driver_class 'org.luciddb.jdbc.LucidDbRmiDriver',
  url 'jdbc:luciddb:rmi:'
);
