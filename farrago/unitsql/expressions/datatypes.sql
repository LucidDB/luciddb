-- $Id$
-- Test expressions with different datatypes

-- VARCHAR comparison
select name from sales.emps where city='San Francisco';

-- test CHAR pad/truncation and VARCHAR truncation
create schema s;
set schema 's';
create table t(i int not null primary key,c char(10),v varchar(10));
insert into t values (1,'goober','goober');
insert into t values (2,'endoplasmic reticulum','endoplasmic reticulum');
!set outputformat csv
select * from t;
!set outputformat table

-- Binary as hexstring
select public_key from sales.emps order by 1;

-- Date/time/timestamp literals

values DATE '2004-12-01';
values TIME '12:01:01';
values TIMESTAMP '2004-12-01 12:01:01';

-- Exponent literals
-- dtbug 271
select 0e0 from (values (0));

-- Verify that type of character string literals is CHARACTER as it should be
-- dtbug 401 (JIRA FRG-74)

-- singleline literals
create view literal_view(lit1,lit2,lit3,lit4,lit5) 
as values ('pumpkin ','pie','','  ',cast('' as varchar(0)));

select column_name,type_name,column_size,nullable
from sys_boot.jdbc_metadata.columns_view
where table_name='LITERAL_VIEW'
order by 1;

-- multiline literals
create view literal_view2(lit1,lit2,lit3,lit4) 
as values ('pump'
'kin ','p'
'ie',''
'',' '
' ');

select column_name,type_name,column_size,nullable
from sys_boot.jdbc_metadata.columns_view
where table_name='LITERAL_VIEW2'
order by 1;

!set outputformat csv
select * from literal_view;
select * from literal_view2;
!set outputformat table

-- FRG-38:  boundary conditions with index on real datatype

create table rtable (r real primary key);

insert into rtable values(1);

select * from rtable;

select * from rtable where r <> 1.0;

select * from rtable where r = 1.0;

select * from rtable where r > 1.0;

select * from rtable where r >= 1.0;

select * from rtable where r < 1.0;

select * from rtable where r <= 1.0;

-- End datatypes.sql


