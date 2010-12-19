-- $Id$
-- Test DDL on privileges

-- Create a security manager and login as this user to perform all grant 
-- tests. 
-- TODO: grant all appropriate system privilege for this user once
-- these privs are available.

create user SECMAN authorization 'Unknown';
create user SECMAN_2 authorization 'Unknown';
create user SECMAN_3 authorization 'Unknown';
create user PRIV_USER1 authorization 'Unknown';

create role r1;
create role r2;
create role r3;
create role r4;
grant role r1 to secman_2;
grant role r1 to r2;
grant role r2 to secman_3;
grant role r3 to secman with admin option;
grant role r4 to r3 with admin option;

!closeall
!connect jdbc:farrago: SECMAN tiger

-- Test 1:
-- 
create schema privstest;

set schema 'privstest';

create table pt1 (c1 int not null primary key, c2 int);

grant SELECT on pt1 to PRIV_USER1 with grant option;
grant SELECT on pt1 to r1;

grant SELECT on pt_notexist to PRIV_USER1 with grant option;

select "name" from sys_fem."Security"."AuthId" order by 1;

-- Test 2: multiple privs
-- 
create table pt2 (c1 int not null primary key, c2 int);

create view pv1 as select * from pt1;

GRANT select, insert, delete, update ON PT2 TO PUBLIC;

GRANT select ON pv1 TO PUBLIC;

GRANT SELECT, UPDATE ON PT2 TO PRIV_USER1 GRANTED BY CURRENT_USER;

---- specify fully qualified name 
GRANT SELECT, INSERT, DELETE, UPDATE ON PRIVSTEST.PT2 to PRIV_USER1;

-- Test 3: negative test

---- setup 
create table pt3 (c1 int not null primary key, c2 int);

---- Invalid privs
grant sel on privstest.pt3 to PUBLIC;

---- invalid object
GRANT SELECT ON PRIVSTEST.PT_NOTEXIST TO PUBLIC WITH GRANT OPTION;

---- incompatible priv vs. object type. EXECUTE vs TABLE
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

---- Check the catalog that the privileges are actually created accordingly

-- Test 5: Test with grantor as CURRENT_ROLE and CURRENT_USER

---- setup 
create table pt5 (c1 int not null primary key, c2 int);

grant select on pt5 to PUBLIC granted by CURRENT_USER;

-- should fail:  no current role set
grant SELECT on pt5 to PUBLIC granted by CURRENT_ROLE;

-- should fail:  r1 does not have rights
set role 'r3';
grant SELECT on pt5 to PUBLIC granted by CURRENT_ROLE;

grant SELECT ON pt5 to r3 with grant option granted by current_user;

-- now it should work
grant SELECT on pt5 to PUBLIC granted by CURRENT_ROLE;

create role rx;

-- should fail:  current user does not have role r1
grant role r1 to rx granted by current_user;

-- should fail:  nor does current role
grant role r1 to rx granted by current_role;

-- should work
grant role r3 to rx granted by current_user;

-- should fail:  r3 does not have admin option
grant role r3 to rx granted by current_role;

-- but this should work since r3 has r4 with admin option
grant role r4 to rx granted by current_role;

-- Test 6: Tests for insufficient privileges

!closeall
!connect jdbc:farrago: SECMAN_2 tiger

set schema 'privstest';

-- should fail:  no grant
select * from pt1;

-- should succeed:  via PUBLIC
select * from pt2;

-- should succeed:  via PUBLIC on view, even though underlying table pt1
-- is inaccessible
select * from pv1;

-- should succeed:  via role r1
set role 'r1';
values(current_role);

select * from pt1;

set role none;
values(current_role);

-- should fail:  no role any more
select * from pt1;

!closeall
!connect jdbc:farrago: SECMAN_3 tiger

set schema 'privstest';

-- should fail:  no grant
select * from pt1;

-- should succeed:  indirect via role r2
set role 'r2';
select * from pt1;
