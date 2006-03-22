--
-- basic decimal tests
--

create schema s;
set schema 's';

-- 19 digit literals

create table d1 (d decimal (19,0));
insert into d1 values (9223372036854775807.);
-- FRG-46
insert into d1 values (-92.23372036854775808);
insert into d1 values (-922337203685477580.9);

select * from d1;

-- arithmetic

create table dt(d decimal(10,5), d2 decimal(2,2));

insert into dt values(-2.123456, 0.12);

select d+d2 from dt;
select d-d2 from dt;
select d/d2 from dt;
select d*d2 from dt;
select abs(d) from dt;
select mod ((cast (d as decimal (10,0))), cast (d2 as decimal(1,0))) from dt;
select mod ((cast (d as decimal (10,0))), cast ('9.09' as decimal(1,0))) from dt;

alter system set "calcVirtualMachine"='CALCVM_JAVA';
-- casting
create table dectable (d decimal(2,0));
create table inttable (i integer);
create table doubletable (d double);
-- spec defines
create table floattable (f float(10));
create table floattable (f float);

-- decimal source
insert into dectable values(10);
insert into inttable select d from dectable;
insert into doubletable select d from dectable;
insert into floattable select d from dectable;

select * from inttable order by 1;
select * from doubletable order by 1;
select * from floattable order by 1;
select * from dectable order by 1;

-- integer source
insert into inttable values(15.5);
insert into dectable select i from inttable;
insert into floattable select i from inttable;
insert into doubletable select i from inttable;

-- double source
insert into doubletable values(1.999);
insert into inttable select d from doubletable;
insert into dectable select d from doubletable;
insert into floattable select d from doubletable;

-- FRG-54
select * from inttable order by 1;
select * from doubletable order by 1;
select * from floattable order by 1;
select * from dectable order by 1;

alter system set "calcVirtualMachine"='CALCVM_FENNEL';

drop table dectable;
drop table inttable;
drop table doubletable;
drop table floattable;
-- casting
create table dectable (d decimal(2,0));
create table inttable (i integer);
create table doubletable (d double);
-- spec defines
create table floattable (f float(10));
create table floattable (f float);

-- decimal source
insert into dectable values(10);
insert into inttable select d from dectable;
insert into doubletable select d from dectable;
insert into floattable select d from dectable;

select * from inttable order by 1;
select * from doubletable order by 1;
select * from floattable order by 1;
select * from dectable order by 1;

-- integer source
insert into inttable values(15.5);
insert into dectable select i from inttable;
insert into floattable select i from inttable;
insert into doubletable select i from inttable;

-- double source
insert into doubletable values(1.999);
insert into inttable select d from doubletable;
insert into dectable select d from doubletable;
insert into floattable select d from doubletable;

-- FRG-54
select * from inttable order by 1;
select * from doubletable order by 1;
select * from floattable order by 1;
select * from dectable order by 1;

alter system set "calcVirtualMachine"='CALCVM_AUTO';

values cast (123.45 as decimal(8,4));
values cast (1234.5678 as decimal(8,4));
values cast (1234.567890 as decimal(8,4));
values cast (12345.567890 as decimal(8,4));

-- from literals
values cast (9223372036854775807 as decimal(19,0));
values cast (9223372036854775808 as decimal(19,0));
values cast ('-9223372036854775808' as decimal(19,0));
values cast (-9223372036854775809 as decimal(19,0));

-- from char/varchar
values cast (cast ('.9176543210987654321' as char(20)) as decimal(19,19));
values cast (cast ('-918765432109876543.2' as varchar(100)) as decimal(19,1));

values cast (cast ('-918765432109876543.5' as varchar(100)) as decimal(19,0));
values cast (cast ('-918765432109876543.5' as varchar(100)) as decimal(19,3));

-- to char/varchar
values cast (cast (-918765432109876543.2 as decimal(19,1)) as char(21));
values cast (cast (.9176543210987654321 as decimal(19,19)) as varchar(20));

create table strtable (s varchar(100));
-- FRG-56: remove primary key once FRG is fixed.
create table dTable2 (d decimal (10,2) primary key);

insert into strtable values('002');
insert into strtable values(' -002');
insert into strtable values(' 00020 ');

select * from strtable order by 1;

insert into dTable2 select cast(s as integer) from strtable;

insert into dTable2 values(cast ('002.5' as decimal(10,2)));
insert into dTable2 values(cast (' -002.3' as decimal(10,2)));
insert into dTable2 values(cast (' +0023.0000 ' as decimal(10,2)));
insert into dTable2 values(cast (' -0012345678.9099 ' as decimal(10,2)));
insert into dTable2 values(cast (' -0012345678.9999 ' as decimal(10,2)));
insert into dTable2 values(cast (' -1012345678.9999 ' as decimal(10,2)));

select * from dTable2 order by 1;

select floor(d) from dTable2 order by 1;
select ceil(d) from dTable2 order by 1;
select exp(d) from dTable2 order by 1;
select ln(abs(d)) from dTable2 order by 1;
select mod(cast (d as decimal(10,0)), 99999999999999) from dTable2 order by 1;
-- round
-- log
-- sqrt

-- type derivation:
values cast(12.3 as decimal(4,2)) + cast(12.3 as decimal(4,1));

-- nullability
values cast(null as decimal(1,1));
values cast (cast (null as varchar(256)) as decimal(6,3));
values cast(null as decimal(6,2)) + 3.25;

-- overflow
values cast(100000 as char(4));
-- FRG-46
values cast (1.2 as decimal(19,18)) + 10;

-- negative (P,S)
values cast (0 as decimal(1,-1));
values cast (null as decimal(0,0));
values cast (null as decimal(-1,1));

-- Validation checks:
values ('1.2' < cast(2.0 as decimal (2,1)));
values (cast ('1.2' as decimal (2,1)) < cast(2.0 as decimal (2,1)));

create table strT (s char(4));
insert into strT values (cast(1 as integer));
insert into strT values (cast(-1.9 as decimal(2,1)));
select * from strT;

