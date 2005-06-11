-- $Id$
-- Test DDL on Authorization: 
-- User, Role, Inheritance structure
-- ???
-- ???

-------------------------------------------------------------------------
-- Basic setup
-- set catalog 'local_db'; TODO: Why does this not work?
create schema authtest;
set schema 'authtest';

-- Create a security manager and login as this user to perform all grant 
-- tests. 
-- TODO: grant all appropriate system privilege for this user once
-- these privs are available.

create user SECMAN authorization 'Unknown';
create user SECMAN_2 authorization 'Unknown';

!connect jdbc:farrago: SECMAN net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver

-------------------------------------------------------------------------
-- Test 1: Create User U1
-- 

Create user U1 AUTHORIZATION 'Unknown';

-- Create Role 1 at  level 1
Create Role R1_L1;

-- Grant the role R1_L1 directly to U1
Grant Role R1_L1 to U1;

-- Alter user to make Role R1_L1 the default
-- Alter user default role R1_L1;

-- Check system catalog for the objects created
set catalog 'sys_boot';
select "name" from sys_fem."Security"."AuthId";
select "name" from sys_fem."Security"."Role";
select "name" from sys_fem."Security"."User";

-- TODO: John to debug this exception
-- select "action" from sys_fem."Security"."CreationGrant";

-------------------------------------------------------------------------
-- Test 2: user, roles at two different levels hierarchies
-- 
-- U2 -> R2_L1 -> R1_L2 -> R1_L3
-- 
-- U4 -> ( R1_L1, R2_L1, R3_L1, R4_L1)
-- 

Create user U2 AUTHORIZATION 'Unknown';
Create user U3 AUTHORIZATION 'Unknown';
Create user U4 AUTHORIZATION 'Unknown';
Create user U5 AUTHORIZATION 'Unknown';
Create user U6 AUTHORIZATION 'Unknown';
Create user U7 AUTHORIZATION 'Unknown';

-- Create Role 2, 3 and 4 at level 1
Create Role R2_L1 WITH ADMIN SECMAN_2;
Create Role R3_L1 WITH ADMIN SECMAN_2;
Create Role R4_L1 WITH ADMIN SECMAN_2;

-- Grant the role R2_L1 directly to U2
Grant Role R2_L1 to U2;

-- Grant  R2_L1 -> R1_L2 -> R1_L3
Create Role R1_L2;
Create Role R1_L3;
Grant Role R1_L2 to R2_L1 WITH GRANT OPTION;
Grant Role R1_L3 to R1_L2;

-- Grant all level 1 roles to U3 i.e. U3 -> ( R1_L1, R2_L1, R3_L1) 
grant role R1_L1, R2_L1, R3_L1 to U3;

-- Grant to user U5, U6, U7 the roles (R3_L1, R4_L1, R1_L3)
Grant role R3_L1, R4_L1, R1_L3 to U5, U6, U7;

-- Alter user to make Role R1_L1 the default
-- Alter user default role R1_L1;R1_L1, R2_L1, R3_L1

set catalog 'sys_boot';
select "name" from sys_fem."Security"."AuthId";
select "name" from sys_fem."Security"."Role";
select "name" from sys_fem."Security"."User";

-- TODO: John to debug this exception
-- select "action" from sys_fem."Security"."CreationGrant";

-------------------------------------------------------------------------
-- Test 3:
-- 

