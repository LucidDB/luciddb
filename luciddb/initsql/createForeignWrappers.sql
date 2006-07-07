-- create oracle jdbc wrapper with browse connect
create foreign data wrapper ORACLE 
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java
options(
  browse_connect_description 'Oracle Database Connection',
  driver_class 'oracle.jdbc.driver.OracleDriver',
  url 'jdbc:oracle:thin:@machineName:port:SID'
);

-- create sql server jdbc wrapper with browse_connect
create foreign data wrapper "SQL SERVER"
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java
options(
  browse_connect_description 'SQL Server Database Connection',
  driver_class 'net.sourceforge.jtds.jdbc.Driver',
  url 'jdbc:jtds:sqlserver://server:port'
);

-- create flat file wrapper with browse_connect
create foreign data wrapper "FLAT FILE"
library 'class com.lucidera.farrago.namespace.flatfile.FlatFileDataWrapper'
language java
options(
  browse_connect_description 'Flat File Connection'
);
