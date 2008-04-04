-- test TopNUdx

-- setup
create schema TOPNTEST;
set schema 'TOPNTEST';
create table T1 (COL1 integer, COL2 varchar(255));
insert into T1 (COL1, COL2) values (10, 'aa');
insert into T1 (COL1, COL2) values (20, null);
insert into T1 (COL1, COL2) values (30, 'cc');
insert into T1 (COL1, COL2) values (40, 'dd');
insert into T1 (COL1, COL2) values (null, 'ee');

-- tests
select * from table(
  applib.topn( cursor(select * from T1), -1 )
);
select * from table(
  applib.topn( cursor(select * from T1), 0 )
);
select * from table(
  applib.topn( cursor(select * from T1), 4 )
);
select * from table(
  applib.topn( cursor(select * from T1), 5 )
);
select * from table(
  applib.topn( cursor(select * from T1), 6 )
);

-- tear down
drop schema TOPNTEST cascade;

