-- NOTE jvs 11-Oct-2009:  This is currently disabled because
-- the Oracle JDBC driver is not open source; we need to make its
-- execution optional as part of test setup.

-- create oracle jdbc wrapper with browse connect
create or replace foreign data wrapper ORACLE 
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java
options(
  browse_connect_description 'Oracle Database Connection',
  driver_class 'oracle.jdbc.driver.OracleDriver',
  url 'jdbc:oracle:thin:@machineName:port:SID',
  type_mapping 'DATE:TIMESTAMP;DECIMAL(22,0):DOUBLE;BINARY:CHAR;VARBINARY:VARCHAR',
  login_timeout '10',
  validation_query 'select 1 from dual',
  lenient 'true'
);
