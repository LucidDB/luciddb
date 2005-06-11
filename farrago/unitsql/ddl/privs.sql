-- $Id$
-- Test DDL on privileges

-- Create a security manager and login as this user to perform all grant 
-- tests. 
-- TODO: grant all appropriate system privilege for this user once
-- these privs are available.

create user SECMAN authorization 'Unknown';
create user SECMAN_2 authorization 'Unknown';

!connect jdbc:farrago: SECMAN net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver

-- Test 1:
-- 
create schema privstest;

set schema 'privstest';

create table pt1 (c1 int not null primary key, c2 int);

grant SELECT on pt1 to PRIV_USER1 with grant option;

grant SELECT on pt_notexist to PRIV_USER1 with grant option;

---- TODO: read back on whether a user has been created. Looks like there is a bug
---- i.e. I can't select specific columns, only seems to work with select *
---- set catalog 'sys_boot';
---- select name from sys_fem."Security"."AuthorizationIdentifier";

-- Test 2: multiple privs
-- 
set catalog 'localdb';
create table pt2 (c1 int not null primary key, c2 int);

GRANT select, insert, delete, update ON PT2 TO PUBLIC;

GRANT SELECT, UPDATE ON PT2 TO PRIV_USER1 GRANTED BY CURRENT_USER;

---- specify fully qualify name 
GRANT SELECT, INSERT, DELETE, UPDATE ON PRIVSTEST.PT2 to PRIV_USER1;

-- Test 3: negative test

---- setup 
create table pt3 (c1 int not null primary key, c2 int);

---- Invalid privs
grant sel on privstest.pt3 to PUBLIC;

---- invalid object
GRANT SELECT ON PRIVSTEST.PT_NOTEXIST TO PUBLIC WITH GRANT OPTION;

---- todo: incompatible priv vs. object type. EXECUTE vs TABLE
grant execute on pt3 to public;

---- todo: incompatible priv vs. object type. EXECUTE vs SEQUENCE

---- todo: incompatible priv vs. object type. INSERT vs SEQUENCE


-- Test 4: Test different object types VIEW, FUNCTION, PROCEDURE and ROUTINE

---- Create function, procedure and routine etc.
create function add_integers_2(i int,j int)
returns int
contains sql
return i + j;

create procedure set_java_property(in name varchar(128),val varchar(128))
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.setSystemProperty';


---- Grant them away
GRANT EXECUTE on SPECIFIC FUNCTION add_integers_2 to PUBLIC;

GRANT EXECUTE on SPECIFIC PROCEDURE set_java_property to PUBLIC;

---- Negative, specific a function with the PROCEDURE qualifier. Should fail
GRANT EXECUTE on SPECIFIC PROCEDURE add_integers_2 to PUBLIC;

---- Negative, specific a function with the PROCEDURE qualifier. Should fail
GRANT EXECUTE on SPECIFIC FUNCTION set_java_property to PUBLIC;

---- Check the catalog that they the privileges are actually created accordingly

-- Test 5: Test with grantor as CURRENT_ROLE and CURRENT_USER

---- setup 
create table pt5 (c1 int not null primary key, c2 int);

grant select on pt5 to PUBLIC granted by CURRENT_USER;

-- TODO: grant SELECT on pt5 to PUBLIC granted by CURRENT_ROLE
