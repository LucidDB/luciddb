-- tests which have different results depending on VM.  Jrockit results don't
-- seem logical
create schema fp;
set schema 'fp';

values (45.3, cast(45.3 as float));
values (45.3, cast(45.3 as double));
values (4.004, cast(4.004 as float));
values (4.004, cast(4.004 as double));

create table fp(f float, d double);
insert into fp values
(1.001, 1.001),
(1.003, 1.003),
(555.55, 555.55);

select * from fp order by f,d;

select * from fp where f = 1.001;
select * from fp where d = 1.001;
select * from fp where f = 555.55;
