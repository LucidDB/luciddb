--
-- bug1504 - aggregation on reals
--

set schema 's';

create table sample1504(cola double);
insert into sample1504 values(5.25);
insert into sample1504 values(19.50);
insert into sample1504 values(55.75);
select * from sample1504 order by 1;
-- FRG-49
select min(cola), max(cola), avg(cola), sum(cola) from sample1504 order by 1;
select min(cola), max(cola), sum(cola) from sample1504 order by 1;

drop table sample1504;
create table sample1504(cola float);
insert into sample1504 values(5.25);
insert into sample1504 values(19.50);
insert into sample1504 values(55.75);
select * from sample1504 order by 1;
-- FRG-49
select min(cola), max(cola), avg(cola), sum(cola) from sample1504 order by 1;
select min(cola), max(cola), sum(cola) from sample1504 order by 1;

drop table sample1504;
create table sample1504(cola real);
insert into sample1504 values(5.25);
insert into sample1504 values(19.50);
insert into sample1504 values(55.75);
select * from sample1504 order by 1;
-- FRG-49
select min(cola), max(cola), avg(cola), sum(cola) from sample1504 order by 1;
select min(cola), max(cola), sum(cola) from sample1504 order by 1;

