-- $Id$
-- Test expressions on nullable data

-- comparison
select name from sales.emps where age > 30;

-- use outputformat xmlattr so we can see nulls
!set outputformat xmlattr

-- computation
select age+1 from sales.emps order by 1;

-- test boolean expressions with one nullable, one nonnullable
select slacker, manager, 
       slacker and manager, not slacker and manager from sales.emps;

select slacker, manager, 
       slacker or manager, not slacker or manager from sales.emps;

-- test boolean expressions with both nullable
select slacker, age < 60,
       slacker and (age < 60), not slacker and (age < 60) from sales.emps;

select slacker, age < 60,
       slacker or (age < 60), not slacker or (age < 60) from sales.emps;

-- casts with nulls; use xmlattr to distinguish null from empty string
!set outputformat xmlattr

values cast(cast(null as tinyint) as varchar(30));
values cast(cast(null as boolean) as varchar(30));
values cast(cast(null as int) as varchar(30));
values cast(cast(null as varchar(10)) as varchar(30));
values cast(cast(null as date) as varchar(30));
values cast(cast(null as time) as varchar(30));
values cast(cast(null as timestamp) as varchar(30));
values cast(cast(null as double) as varchar(30));

-- make sure empty string doesn't get converted back to null (FRG-275)
values ('');

create schema x;
create table x.t(i int not null primary key);

-- this should work since the table is empty (FRG-365)
insert into x.t select null from x.t;
