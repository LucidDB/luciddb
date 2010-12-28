-- test TopNUdx

-- setup
create schema TOPNTEST;
set schema 'TOPNTEST';
create table T1 (COL1 integer, COL2 varchar(255));
insert into T1 (COL1, COL2) values (null, 'aa');
insert into T1 (COL1, COL2) values (20, null);
insert into T1 (COL1, COL2) values (30, 'cc');
insert into T1 (COL1, COL2) values (40, 'dd');
insert into T1 (COL1, COL2) values (50, 'ee');

-- tests
select * from table(
  applib.topn( cursor(select * from T1 order by 1), -1 )
)
order by 1;
select * from table(
  applib.topn( cursor(select * from T1 order by 1), 0 )
)
order by 1;
select * from table(
  applib.topn( cursor(select * from T1 order by 1), 4 )
)
order by 1;
select * from table(
  applib.topn( cursor(select * from T1 order by 1), 5 )
)
order by 1;
select * from table(
  applib.topn( cursor(select * from T1 order by 1), 6 )
)
order by 1;

-- tear down
drop schema TOPNTEST cascade;

