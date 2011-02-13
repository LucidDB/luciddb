-- $Id$
-- Test DDL for user-defined functions with impersonation support

create schema udfimp;
set schema 'udfimp';
set path 'udfimp';

create function execute_sql_as_definer(stmt varchar(65535))
returns varchar(65535)
language java
modifies sql data
external name 
'class net.sf.farrago.test.FarragoTestUDR.executeSql'
external security definer;

create function execute_sql_as_invoker(stmt varchar(65535))
returns varchar(65535)
language java
modifies sql data
external name 
'class net.sf.farrago.test.FarragoTestUDR.executeSql'
external security invoker;

create function execute_sql_as_unspecified(stmt varchar(65535))
returns varchar(65535)
language java
modifies sql data
external name 
'class net.sf.farrago.test.FarragoTestUDR.executeSql';

create procedure udp_execute_sql_as_definer(stmt varchar(65535))
language java
modifies sql data
external name 'class net.sf.farrago.test.FarragoTestUDR.executeSqlVoid'
external security definer;

create procedure udp_execute_sql_as_invoker(stmt varchar(65535))
language java
modifies sql data
external name 'class net.sf.farrago.test.FarragoTestUDR.executeSqlVoid'
external security invoker;

values (execute_sql_as_definer('values current_user'));
values (execute_sql_as_invoker('values current_user'));
values (execute_sql_as_unspecified('values current_user'));
values (execute_sql_as_definer('values session_user'));
values (execute_sql_as_invoker('values session_user'));
values (execute_sql_as_unspecified('values session_user'));

create user noob;
create role r;
grant role r to noob;

grant execute on specific function execute_sql_as_definer to noob;
grant execute on specific function execute_sql_as_invoker to noob;
grant execute on specific function execute_sql_as_unspecified to noob;
grant execute on specific procedure udp_execute_sql_as_definer to noob;
grant execute on specific procedure udp_execute_sql_as_invoker to noob;

!closeall
!connect jdbc:farrago: NOOB tiger

set schema 'udfimp';
set path 'udfimp';

create table tn(i int primary key);

grant select on tn to r;

set role 'r';

values (execute_sql_as_definer('values current_user'));
values (execute_sql_as_invoker('values current_user'));
values (execute_sql_as_unspecified('values current_user'));
values (execute_sql_as_definer('values session_user'));
values (execute_sql_as_invoker('values session_user'));
values (execute_sql_as_unspecified('values session_user'));

-- should succeed since sa has privs
values (execute_sql_as_definer('select count(*) from sales.emps'));

-- should fail since sa has no privs (and role r is inactive during call)
values (execute_sql_as_definer('select count(*) from tn'));

-- should fail since noob has no privs
values (execute_sql_as_invoker('select count(*) from sales.emps'));

-- should succeed since noob has privs
values (execute_sql_as_invoker('select count(*) from tn'));

-- should be created by sa
values (execute_sql_as_definer(
'create table udfimp.td1(i int primary key)'));
call udp_execute_sql_as_definer(
'create table udfimp.td2(i int primary key)');

-- should be created by noob
values (execute_sql_as_invoker(
'create table udfimp.ti1(i int primary key)'));
call udp_execute_sql_as_invoker(
'create table udfimp.ti2(i int primary key)');

-- should fail since sa is creator and noob has no privs
select * from td1;
select * from td2;

-- should succeed since noob is creator
select * from ti1;
select * from ti2;
