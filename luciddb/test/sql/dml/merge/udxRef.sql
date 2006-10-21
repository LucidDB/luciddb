--------
-- Setup
--------

create schema ct;
set schema 'ct';
set path 'ct';


---------------
-- Cursor tests
---------------

create table t1(aa int, bb varchar(20));
insert into t1 values (1, 'one'), (3, 'three'), (10, 'ten');

create table t2(a int, b varchar(20));
insert into t2 values (2, 'dos'), (3, 'tres');

--
-- UDX with multiple output columns
--
create function get_column_types(c cursor)
returns table( colname varchar(65535), coltype int, coltypename varchar(65535))
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.GetColumnTypesUdx.execute';

-- gets UDX directly
select * 
     from table(
       get_column_types(
         cursor(select * from sys_fem."Config"."FarragoConfig")))
order by 1;

-- merges UDX into table
create table params (colname varchar(65535), coltype int, coltypename varchar(65535));

insert into params(colname, coltype, coltypename) 
  (select * 
     from table(
       get_column_types(
         cursor(select * from sys_fem."Config"."FarragoConfig"))));
       
select * from params order by 1;

merge into params p
  using
    table(get_column_types(
            cursor(select * from sys_fem."Config"."FarragoConfig"))) as temp
  on p.colname = temp.colname
  when matched then 
    update set colname = upper(p.colname)
  when not matched then 
    insert (colname, coltype, coltypename)
    values (temp.colname, temp.coltype, temp.coltypename);

select * from params order by 1;

-- gets another UDX directly 
select * from table(
  get_column_types(
    cursor(values(1,'dsfs', cast ('sdfsd' as varchar(10)), TIME'12:12:12', 1.2, cast(12.2 as float)))))
order by 1;

-- merges it into table
merge into params p
  using
    table(get_column_types(
            cursor(values(1,'dsfs', cast ('sdfsd' as varchar(10)), TIME'12:12:12', 1.2, cast(12.2 as float))))) as temp
  on p.colname = temp.colname
  when matched then 
    update set colname = lower(p.colname)
  when not matched then 
    insert (colname, coltype, coltypename)
    values (temp.colname, temp.coltype, temp.coltypename);

select * from params order by 1;

drop table params;


--
-- UDX with two cursors
--
create function two_cursor_test(c1 cursor, c2 cursor)
returns table(col1 int, col2 varchar(20))
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.TestTwoCursorUdx.execute';

-- gets the UDX directly
select * from table(
  two_cursor_test(
    cursor(select * from t1),
    cursor(select * from t2)))
order by 1,2;

-- inserts and merges it into table
create table t12(c1 integer, c2 varchar(20));

insert into t12
  (select * from table(
     two_cursor_test(
       cursor(select * from t1),
       cursor(select * from t2)))
     where col2 in ('one','ten'));

merge into t12 t
  using
    table(two_cursor_test(
            cursor(select * from t1),
            cursor(select * from t2))) as temp
  on t.c1 = temp.col1
  when matched then
    update set c2 = upper(c2)
  when not matched then
    insert (c1, c2) values (temp.col1, temp.col2);

select * from t12 order by 1,2;
drop table t12;
drop table t1;
drop table t2;

-----------------------
-- Time dimension tests
-----------------------

select 
 time_key, 
 applib.calendar_quarter( time_key ), 
 applib.fiscal_quarter( time_key, 4 ), 
 applib.fiscal_month( time_key, 4 ), 
 applib.fiscal_year( time_key, 4 )
from table(applib.time_dimension( 1997, 2, 27, 1997, 3, 5))
order by time_key;

create table dt (d date, dow varchar(10), weekend char(1), cqt varchar(10));

insert into dt(d, cqt)
   (select 
      time_key, 
      applib.calendar_quarter( time_key )
      from table(applib.time_dimension( 1997, 2, 27, 1997, 3, 7))
   );

merge into dt 
  using
    table(applib.time_dimension( 1997, 2, 25, 1997, 3, 5)) as temp
  on d = temp.time_key
  when matched then
    update set dow = temp.day_of_week,
               weekend = temp.weekend
  when not matched then
    insert (d, dow)
    values (temp.time_key, temp.day_of_week);

select * from dt order by 1,2,3,4;

drop table dt;


create table period (
time_key_seq integer,
time_key date,
quarter integer,
yr integer,
calendar_quarter varchar(15) );

insert into period
select 
g_period.time_key_seq,
g_period.time_key,
g_period.quarter,
g_period.yr,
g_period.calendar_quarter
from
(select * from table(applib.time_dimension(1996, 5, 1, 1996, 5, 31)))g_period;

select * from period
order by 2;

merge into period p
using table(applib.time_dimension(1996, 5, 12, 1996, 6, 2)) as temp
on p.time_key = temp.time_key
when matched then 
  update set calendar_quarter = lower(p.calendar_quarter)
when not matched then
  insert (time_key, quarter, yr, calendar_quarter)
  values (temp.time_key, temp.quarter, temp.yr, temp.calendar_quarter);

select * from period
order by 2;


drop table period;

drop schema ct cascade;
