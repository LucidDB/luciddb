-- $Id$
-- Test sqlline incremental result handling

create schema d;

create table d.t(
    i int not null primary key,
    long_label varchar(3),
    short_label varchar(40));

insert into d.t values (1,'a','a');
insert into d.t values (20,'bcd','bcdefghijklmnopqrstuvwxyz');

-- without incremental
select * from d.t order by 1;

-- with incremental
!set incremental on
select * from d.t order by 1;

-- verify boolean display size
select slacker as s from sales.emps order by 1;
