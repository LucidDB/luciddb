-- create oracle jdbc wrapper with browse connect
create or replace foreign data wrapper ORACLE 
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java
options(
  browse_connect_description 'Oracle Database Connection',
  driver_class 'oracle.jdbc.driver.OracleDriver',
  url 'jdbc:oracle:thin:@machineName:port:SID',
  type_mapping 'DATE:TIMESTAMP;DECIMAL(22,0):DOUBLE;BINARY:CHAR;VARBINARY:VARCHAR',
  login_timeout '10'
);

-- create sql server jdbc wrapper with browse_connect
create or replace foreign data wrapper "SQL SERVER"
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java
options(
  browse_connect_description 'SQL Server Database Connection',
  driver_class 'net.sourceforge.jtds.jdbc.Driver',
  url 'jdbc:jtds:sqlserver://server:port',
  login_timeout '10'
);

-- create flat file wrapper with browse_connect
create or replace foreign data wrapper "FLAT FILE"
library 'class com.lucidera.farrago.namespace.flatfile.FlatFileDataWrapper'
language java
options(
  browse_connect_description 'Flat File Connection'
);

-- create luciddb local wrapper with browse connect
create or replace foreign data wrapper "LUCIDDB LOCAL"
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java
options(
  browse_connect_description 'LucidDb Loopback Connection',
  driver_class 'com.lucidera.jdbc.LucidDbLocalDriver',
  url 'jdbc:luciddb:'
);
