-- $Id$
-- Test DDL on jars

create schema jartest;

set schema 'jartest';

set path 'jartest';

-- first, test standard SQLJ builtin procedures

call sqlj.install_jar('file:plugin/FarragoMedJdbc.jar','medjar',0);

create function get_driver_for_url(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'medjar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.getDriverForUrl';

create procedure test_connection(
    in driver_class_name varchar(2000),
    in url varchar(2000),
    in user_name varchar(2000),
    in password varchar(2000))
language java
no sql
external name 
'medjar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.testConnection';

values get_driver_for_url('jdbc:default:connection');

call test_connection(
'org.hsqldb.jdbcDriver','jdbc:hsqldb:testcases/hsqldb/scott','SA',
cast(null as varchar(128)));

-- should fail:  invalid external name
create function bad1(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'net.sf.farrago.namespace.jdbc.MedJdbcUDR.getDriverForUrl';

-- should fail:  invalid jar name
create function bad2(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'jar jar binks:net.sf.farrago.namespace.jdbc.MedJdbcUDR.testConnection';

-- should fail:  unknown jar
create function bad3(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'nojar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.testConnection';

-- should fail:  no such method
create function bad4(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'medjar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.connectToODBC';

-- should fail due to dependencies and implicit RESTRICT
call sqlj.remove_jar('medjar',0);

drop function get_driver_for_url;

drop procedure test_connection;

-- should succeed
call sqlj.remove_jar('medjar',0);

-- should fail since jar is gone now
create function get_driver_for_url(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'medjar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.getDriverForUrl';


-- next, test non-standard DDL, which is what we use to implement
-- the above sqlj routines internally

create jar medjar library 'file:plugin/FarragoMedJdbc.jar' options(0);

create function get_driver_for_url(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'medjar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.getDriverForUrl';

values get_driver_for_url('jdbc:default:connection');

-- should fail
drop jar medjar options(0) restrict;

-- should succeed
drop jar medjar options(0) cascade;

-- should fail since jar is gone now
create function get_driver_for_url(url varchar(2000))
returns varchar(2000)
language java
no sql
external name 
'medjar:net.sf.farrago.namespace.jdbc.MedJdbcUDR.getDriverForUrl';
