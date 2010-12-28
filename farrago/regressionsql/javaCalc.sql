-- $Id$
-- Tests to cover Java calc code paths
-- (But preserve the existing calcVirtualMachine settings since that 
-- setting persists from test to test)

alter system set "calcVirtualMachine"='CALCVM_JAVA';

create schema s;
set schema 's';

-- DATA SOURCES
-- (use tables for nullable values and to avoid constant optimization)

-- tinyint data sources
create table tinyint_nonnullable(
    val tinyint not null primary key);
insert into tinyint_nonnullable values (-128), (127);

create table tinyint_nullable_src (
    i tinyint generated always as identity primary key,
    val tinyint);
insert into tinyint_nullable_src (val) values (-128), (127);

create view tinyint_nullable as
select val from tinyint_nullable_src;

-- smallint data sources
create table smallint_nonnullable(
    val smallint not null primary key);
insert into smallint_nonnullable 
    values (-128), (127), (-32768), (32767);

create table smallint_nullable_src (
    i smallint generated always as identity primary key,
    val smallint);
insert into smallint_nullable_src (val) 
    values (-128), (127), (-32768), (32767);

create view smallint_nullable as
select val from smallint_nullable_src;

-- float data sources
create table float_nonnullable(
    val float not null primary key);
insert into float_nonnullable 
    values (-9.9), (9.9);

create table float_nullable_src (
    i int generated always as identity primary key,
    val float);
insert into float_nullable_src (val) 
    values (-19.9), (19.9);

create view float_nullable as
select val from float_nullable_src;

-- TEST CASES

-- test tinyint
create table a (
    i tinyint generated always as identity primary key,
    j tinyint not null,
    k tinyint);

insert into a (j) select * from tinyint_nonnullable;
insert into a (j) select * from tinyint_nullable;

insert into a (j, k) select 0, val from tinyint_nonnullable;
insert into a (j, k) select 0, val from tinyint_nullable;

-- test overflow
insert into a (j) select * from smallint_nonnullable 
where val > 127;
-- used to cause an infinite loop due to FRG-215
insert into a (j) select * from smallint_nullable 
where val > 127;
insert into a (j, k) select 0, val from smallint_nonnullable 
where val > 127;
insert into a (j, k) select 0, val from smallint_nullable 
where val > 127;

-- test rounding
insert into a (j) select * from float_nonnullable;
insert into a (j) select * from float_nullable;

insert into a (j, k) select 0, val from float_nonnullable;
insert into a (j, k) select 0, val from float_nullable;

-- TODO: test rounding which causes overflow (especially int)
-- the (-) operation used in rounding algorithm can also cause overflow

select * from a;

-- bug LER-2179
create table adouble(i int primary key, d double);
insert into adouble values (1, null), (2, null), (3, null);
select cast(d as int) from adouble;

alter system set "calcVirtualMachine"='CALCVM_AUTO';
