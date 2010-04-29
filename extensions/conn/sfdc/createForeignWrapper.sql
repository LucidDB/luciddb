--
-- This script creates the built-in foreign data wrappers
--

!set verbose true

create or replace schema sys_boot.farrago;
set schema 'sys_boot.farrago';
set path 'sys_boot.farrago';

--------------
-- SFDC Jar --
--------------

create or replace jar sys_boot.farrago.sfdcJar
--library 'file:${FARRAGO_HOME}/plugin/MedSfdc.jar'
library 'file:plugin/MedSfdc.jar'
options(0);

---------------------------------------
-- UDXes needed by SFDC_DATA_WRAPPER --
---------------------------------------

create or replace function sys_boot.farrago.sfdc_query(
  query varchar(10000), types varchar(10000))
returns table(
  objects varchar(128))
language java
parameter style system defined java
dynamic_function
no sql
external name 'sys_boot.farrago.sfdcJar:net.sf.farrago.namespace.sfdc.SfdcUdx.query';

create or replace function sys_boot.farrago.sfdc_deleted(
  objectName varchar(1024), startTime varchar(256), endTime varchar(256))
returns table(
  objects varchar(128))
language java
parameter style system defined java
dynamic_function
no sql
external name 'sys_boot.farrago.sfdcJar:net.sf.farrago.namespace.sfdc.SfdcUdx.getDeleted';

create or replace function sys_boot.farrago.sfdc_lov(
  objectName varchar(1024))
returns table(
  objects varchar(128))
language java
parameter style system defined java
dynamic_function
no sql
external name 'sys_boot.farrago.sfdcJar:net.sf.farrago.namespace.sfdc.SfdcUdx.getLov';

-----------------------
-- SFDC DATA WRAPPER --
-----------------------

create or replace foreign data wrapper SALESFORCE
--library '${FARRAGO_HOME}/plugin/MedSfdc.jar'
library 'plugin/MedSfdc.jar'
language java
options(
  browse_connect_description 'Salesforce.com Web Service Connection'
);
