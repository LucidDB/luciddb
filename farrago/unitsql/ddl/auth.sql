-- $Id$
-- Test DDL on Authorization: 
-- User, Role, Role Inheritance structure

-- REVIEW jvs 12-June-2005:  I put in this connect as _SYSTEM so that
-- the creation grant for user SECMAN has a non-null grantee.  In
-- "real life" this shouldn't be allowed.  And there needs to be a check
-- for valid user creator.
!closeall
!connect jdbc:farrago: _SYSTEM net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver

-------------------------------------------------------------------------
-- Basic setup

-- Create a security manager and login as this user to perform all grant 
-- tests. 
-- TODO: grant all appropriate system privileges for this user once
-- these privs are available.

create user SECMAN authorization 'Unknown' DEFAULT CATALOG localdb;
create user SECMAN_2 authorization 'Unknown' DEFAULT CATALOG localdb;

!closeall
!connect jdbc:farrago: SECMAN net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver

set catalog 'localdb'; 
create schema authtest;
set schema 'authtest';

create view grant_view as 
select  me."name" granted_element,  gte."name" grantee,  gto."name" grantor, g."action", 
g."withGrantOption"
from 
        sys_fem."Security"."Grant" g
inner join
        sys_fem."Security"."AuthId" gto
on      g."Grantor" = gto."mofId"
inner join 
        sys_fem."Security"."AuthId" gte
on      g."Grantee" = gte."mofId"
inner join 
        sys_cwm."Core"."ModelElement" me
on      g."Element" = me."mofId";

-------------------------------------------------------------------------
-- Test 1: 
-- Create User U1, 
-- o grant a role to the user, 
-- o check the system catalog to ensure that the appropriate records has 
--   been written.

Create user U1 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;

-- Create Role 1 at  level 1
Create Role R1_L1;

-- Grant the role R1_L1 directly to U1
Grant Role R1_L1 to U1;

-- Alter user to make Role R1_L1 the default
-- Alter user default role R1_L1;

-- Check system catalog for the objects and system generated grants

-- Check out the role created
select "name" from sys_fem."Security"."Role" where "name" like 'R%_L%'
order by "name";
select "name" from sys_fem."Security"."User" where "name" like 'U%'
order by "name";

-- check out the grants created
select  granted_element,  grantee,  grantor, "action", "withGrantOption"
from grant_view
where grantee = 'U1' or grantee= 'R1_L1'
order by granted_element;

-------------------------------------------------------------------------
-- Test 2: user, roles at two different levels hierarchies
-- 
-- o U2 -> R2_L1 -> R1_L2 -> R1_L3
-- o U4 -> ( R1_L1, R2_L1, R3_L1, R4_L1)
-- o U5, U6, U7 -> (R3_L1, R4_L1, R1_L3)

Create user U2 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;
Create user U3 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;
Create user U4 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;
Create user U5 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;
Create user U6 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;
Create user U7 AUTHORIZATION 'Unknown' DEFAULT CATALOG localdb;

select "name" from sys_fem."Security"."User" where "name" like 'U%';

-- Create Role 2, 3 and 4 at level 1
Create Role R2_L1 WITH ADMIN SECMAN_2;
Create Role R3_L1 WITH ADMIN SECMAN_2;
Create Role R4_L1 WITH ADMIN SECMAN_2;

select "name" from sys_fem."Security"."Role" where "name" like 'R%_L%'
order by "name";

-- Grant the role R2_L1 directly to U2
Grant Role R2_L1 to U2;


-- Grant  R2_L1 -> R1_L2 -> R1_L3
Create Role R1_L2;
Create Role R1_L3;

select "name" from sys_fem."Security"."Role" where "name" like 'R%_L%'
order by "name";

Grant Role R1_L2 to R2_L1 WITH GRANT OPTION;
Grant Role R1_L3 to R1_L2;

select  granted_element,  grantee,  grantor, "action", "withGrantOption"
from grant_view
where grantee = 'U2' or grantee = 'R2_L1' or grantee = 'R1_L3'
order by granted_element;


-- Grant all level 1 roles to U3 i.e. U3 -> ( R1_L1, R2_L1, R3_L1) 
grant role R1_L1, R2_L1, R3_L1 to U3;

select  granted_element,  grantee,  grantor, "action", "withGrantOption"
from grant_view
where grantee = 'U3'
order by granted_element;

-- Grant to user U5, U6, U7 the roles (R3_L1, R4_L1, R1_L3)
Grant role R3_L1, R4_L1, R1_L3 to U5, U6, U7;

select  granted_element,  grantee,  grantor, "action", "withGrantOption"
from grant_view
where grantee = 'U5' or grantee = 'U6' or grantee = 'U7'
order by grantee;


-- Alter user to make Role R1_L1 the default
-- Alter user default role R1_L1;R1_L1, R2_L1, R3_L1


-------------------------------------------------------------------------
-- Test 3:
-- 
