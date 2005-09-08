-- $Id$
-- Test DDL on views

create schema s;

set schema 's';

-- test * expansion
create view v1 as select * from sales.depts;

select * from v1 order by deptno;

-- test explicit column names
create view v2(ename) as select name from sales.emps;

select * from v2 order by 1;

-- bad:  column mismatch
create view v3(empno, name, id) as select empno, name from sales.emps;

-- bad:  duplicate column names
create view v4(empno,empno) as select empno, name from sales.emps;

-- bad:  invalid view def
create view v5 as select * from nonexistent_table;

-- bad:  view with dynamic params
create view v6 as select * from sales.emps where empno = ?;

-- bad:  duplicate view name
create view v1 as select * from sales.emps;

-- bad:  duplicate view name (views and tables conflict)
create view sales.depts as select * from sales.emps;

-- bad:  ORDER BY in view
create view v7 as select * from sales.emps order by name;

-- add extra dependencies to make drop schema cascade work harder

create view v22 as select * from v2;

create view v23 as select * from v22;

create view v24 as select * from v22,v23;

drop schema s cascade;

create schema s;

create table t1(i int not null primary key);

create view v7 as select * from t1;

-- bad:  can't drop without cascade
drop table t1;
drop table t1 restrict;

-- should work
drop table t1 cascade;

-- bad:  v7 shouldn't be there any more
select * from v7;

create table t2(i int not null primary key);

create view v8 as select * from t2;

create view v9 as select * from v8;

create view v10 as select * from v9;

create view v11 as select * from v8;

-- bad: can't drop without cascade
drop view v8;
drop view v8 restrict;

-- bad: can't drop without cascade
drop view v9;

-- should work
drop view v9 cascade;

-- bad:  no longer exists
select * from v10;

drop table t2 cascade;

-- bad:  no longer exists
select * from v8;

select * from v11;

-- make sure dependencies got dropped too
select "name" from sys_cwm."Core"."Dependency" where "name"='V8$DEP';

-- multi-line view including comments and description
create view v12 
description 'foo'
   'bar' as
select empno, /* a comment */ name
  from sales.emps;

!set outputformat csv

-- make sure that the original text is stored correctly
select "originalDefinition", "description"
from sys_fem."SQL2003"."LocalView"
where "name" = 'V12';

-- End view.sql
