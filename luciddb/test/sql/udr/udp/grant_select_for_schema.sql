-- Test the grant_select_for_schema UDP
create schema MASSGRANTTEST;
create user MONDRIAN_USER authorization NOPE default schema MASSGRANTTEST;
set schema 'MASSGRANTTEST';
create table T1(col1 integer, col2 integer);
create table T2(col3 varchar(255), col4 integer);
create view V1 as select * from T2;
create view V2 as select col2 from T1;
-- connect as new user and try to do select on tables. should fail.
!connect jdbc:luciddb: MONDRIAN_USER NOPE
select * from T1;
select * from V2;
!closeall
-- connect as sysadmin and give new user select privs for everything in schema
!connect jdbc:luciddb: sa sa
call applib.grant_select_for_schema('MASSGRANTTEST', 'MONDRIAN_USER');
-- try again. should succeed.
!connect jdbc:luciddb: MONDRIAN_USER NOPE
select * from T1;
select * from V1;
!closeall
!connect jdbc:luciddb: sa sa
drop table MASSGRANTTEST.T1 cascade;
drop table MASSGRANTTEST.T2 cascade;
drop user MONDRIAN_USER;
drop schema MASSGRANTTEST;
