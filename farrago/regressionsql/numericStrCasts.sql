-- $Id$
-- Test casts between string types

--
-- test basic integer conversions
--
create schema s;
set schema 's';
create table ints (
    x integer not null primary key,
    si smallint not null,
    i integer not null,
    bi bigint not null);
insert into ints values (1, 1, 1, 1);
insert into ints values (2, -1, -1, -1);
insert into ints values (3, 0, 0, 0);
insert into ints values (4, 32767, 2147483647, 9223372036854775807);
-- REVIEW: SZ: 8/5/2004: Unary minus is treated separately from the
-- number itself.  Currently we use Java longs for literal validation
-- and since 9223372036854775808 is Long.MAX_LONG + 1, the literal
-- -9223372036854775808 appears invalid.  Once we have support for
-- arbitrary precision numerics, this limitation will likely go away.
insert into ints values (5, -32768, -2147483648, -9223372036854775807);

create table exacts (
    x integer not null primary key,
    i decimal(5,0) not null,
    r decimal(4,2) not null);
insert into exacts values (1, 1, 1.00);
insert into exacts values (2, -1, -1.00);
insert into exacts values (3, 0, 0.00);
insert into exacts values (4, 99999, 99.99);
insert into exacts values (5, -99999, -99.99);

create table approx (
    x integer not null primary key,
    r real not null,
    d double not null);
insert into approx values (1, 1.0, 1.0);
insert into approx values (2, -1.0, -1.0);
insert into approx values (3, 0.0, 0.0);
insert into approx values (4, 3.1415927, 3.141592653589793);
insert into approx values (5, 31.415927, 31.41592653589793);

select * from ints;
select * from exacts;
select * from approx;

--
-- cast numbers to char
--
select cast(si as char(6))  from ints;
select cast(i as char(11)) from ints;
select cast(bi as char(20)) from ints;
select cast(i as char(6)) from exacts;
select cast(r as char(6)) from exacts;

-- REVIEW: Results are likely to be non-portable, as float representations
-- REVIEW: will differ on various processors. Should round or truncate
-- REVIEW: results lexically. -JK 2004/08/11
select cast(r as char(10)) from approx;
select cast(d as char(23)) from approx;

--
-- cast numbers to varchar
--
select cast(si as varchar(6)) from ints;
select cast(i as varchar(11)) from ints;
select cast(bi as varchar(20)) from ints;
select cast(i as varchar(6)) from exacts;
select cast(r as varchar(6)) from exacts;


--
-- cast numbers to char that's too small
--
select cast(si as char(3)) from ints;
select cast(i as char(5)) from ints;
select cast(bi as char(10)) from ints;
select cast(i as char(3)) from exacts;
select cast(r as char(3)) from exacts;
select cast(r as char(3)) from approx;
select cast(d as char(3)) from approx;


--
-- cast numbers to varchar that's too small
--
select cast(si as varchar(3)) from ints;
select cast(i as varchar(5)) from ints;
select cast(bi as varchar(10)) from ints;
select cast(i as varchar(3)) from exacts;
select cast(r as varchar(3)) from exacts;
select cast(r as varchar(3)) from approx;
select cast(d as varchar(3)) from approx;

-- REVIEW: Results are likely to be non-portable, as float representations
-- REVIEW: will differ on various processors. Should round or truncate
-- REVIEW: results lexically. -JK 2004/08/11
select cast(d as varchar(23)) from approx;

-- TODO: fennel displays more than 8 digits. (plus signg ...), that caused diff.
alter system set "calcVirtualMachine"='CALCVM_JAVA';
select cast(r as varchar(23)) from approx;

drop table ints;
drop table exacts;
drop table approx;
drop schema s;
