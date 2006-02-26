-- $Id$
-- Throws unsupported types at the system, to make sure the errors are
-- civilized.

create schema types_test;
set schema 'types_test';

-- Test supported type (DECIMAL) 

create table td(n integer not null primary key, d decimal);

create table td5(n integer not null primary key, d decimal(5));

create table td52(n integer not null primary key, d decimal(5, 2));

values (cast(null as decimal));

values (cast(null as decimal(5)));

values (cast(null as decimal(5, 2)));

values (cast(1.2 as decimal(5,2)));


-- Test assignment rules

-- should fail
insert into sales.depts values(10,20);

-- should fail
update sales.depts set deptno='Infinitum';


-- TODO: Test unsupported types

-- End types.sql
