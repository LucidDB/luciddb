-- $Id$
-- Throws unsupported types at the system, to make sure the errors are
-- civilized.

create schema decimal_ddl_test;
set schema 'decimal_ddl_test';

-- Test table creation with decimal type
-- (default precision=19, scale=0)
create table td(n integer not null primary key, d decimal);
create table td5(n integer not null primary key, d decimal(5));
create table td52(n integer not null primary key, d decimal(5, 2));

-- Test table creation with numeric type (should be same as decimal)
create table tn(n integer not null primary key, d numeric);
create table tn9(n integer not null primary key, d numeric(9));
create table tn95(n integer not null primary key, d numeric(9, 5));

-- should give error 'precision exceeds maximum'
create table tde1(n integer not null primary key, d decimal(40, 2));

-- should give error 'scale exceeds maximum'
create table tde2(n integer not null primary key, d decimal(5, 20));

-- should give error, negative precision not allowed
create table tde3(n integer not null primary key, d decimal(-1));

-- should give error, negative scale not allowed
create table tde4(n integer not null primary key, d decimal(5, -2));

-- should give error, precision cannot be 0
create table tde5(n integer not null primary key, d decimal(0));

-- Test insert into table (decimal)
insert into td values(1, 2);
insert into td5 values(1, 3);
insert into td52 values(1, 2.69);

-- rounding
insert into td values(2, 2.7);
insert into td values(3, 2.5);
insert into td values(4, 2.1);
insert into td5 values(2, -2.7);
insert into td5 values(3, -2.5);
insert into td5 values(4, -2.1);
insert into td52 values(2, 2.699);
insert into td52 values(3, 2.695);
insert into td52 values(4, 2.691);
insert into td52 values(5, -2.699);
insert into td52 values(6, -2.695);
insert into td52 values(7, -2.691);

-- in range rounding
-- TODO: Fix error: Numeric literal is out of range
-- insert into td values(10, 9223372036854775807.13213);
-- insert into td values(11, -9223372036854775808.12321);
insert into td5 values(10, 99999.1411312);
insert into td5 values(11, -99999.123123); 
insert into td52 values(10, 999.99452324);
insert into td52 values(11, -999.99452142); 

-- out of range
insert into td values(20, 9223372036854775808);
insert into td values(21, 9223372036854775807.5);
insert into td values(22, -9223372036854775809);
insert into td values(23, -9223372036854775808.5);

-- TODO: Should give error
-- insert into td5 values(20, 100000);
-- insert into td5 values(21, 99999.5); 
-- insert into td5 values(22, -100000);
-- insert into td5 values(23, -99999.5); 
-- insert into td52 values(20, 1000.00);
-- insert into td52 values(21, 999.995);
-- insert into td52 values(22, -1000.00);
-- insert into td52 values(23, -999.995); 

-- null
insert into td values(30, null);
insert into td5 values(30, null);
insert into td52 values(30, null);

select * from td;
select * from td5;
select * from td52;

-- Test insert into table (numerics)

insert into tn values(1, 2.5);
insert into tn9 values(1, 1234.456);
insert into tn95 values(1, 9999.99999123);

insert into tn values(2, null);
insert into tn9 values(2, null);
insert into tn95 values(2, null);

select * from tn;
select * from tn9;
select * from tn95;

-- Test views (decimals)

create view vd as select * from td;
create view vd5 as select * from td5;
create view vd52 as select * from td52;

select * from vd;
select * from vd5;
select * from vd52;

-- Test views (numerics)

create view vn as select * from td;
create view vn9 as select * from td5;
create view vn95 as select * from td52;

select * from vn;
select * from vn9;
select * from vn95;

-- End decimal.sql
