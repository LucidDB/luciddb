create schema cr;

set schema 'cr';

-- basic table
create table tree(parent varchar(128), child varchar(128));

insert into tree values
  ('01','02'),
  ('01','03'),
  ('01','04'),
  ('02','05'),
  ('02','06'),
  ('07','08'),
  ('08','09'),
  ('08','10'),
  ('09','11'),
  ('11','12'),
  ('11','13')
;

select * 
from table( applib.collapse_rows(cursor (select * from tree), '~'))
order by parent_value;

-- with null values
insert into tree values
  (null, '01'),
  (null, '07'),
  (null, null),
  ('04', null),
  ('02', null),
  ('09', null)
;

select * 
from table( applib.collapse_rows(cursor (select * from tree), '|'))
order by parent_value;

-- with repeats
insert into tree values
  ('09', '11'),
  ('01', '03'),
  ('04', null),
  ('09', null),
  (null, '07')
;
    
select * 
from table( applib.collapse_rows(cursor (select * from tree), ' '))
order by parent_value;

--
-- input table with non-string types
--
create table typetable(parent float, child date);

insert into typetable values
  (1.115, DATE'2006-12-13'),
  (1.114, DATE'2006-12-13'),
  (1.115, DATE'1900-05-15'),
  (123213.3249024800, DATE'2001-1-19'),
  (123213.32490248, DATE'1977-2-22'),
  (56, DATE'1867-8-8'),
  (null, DATE'2001-1-19'),
  (null, null),
  (1.115, DATE'2002-6-17'),
  (56.00001, DATE'1977-2-9'),
  (7291.08371, null)
;

select * 
from table( applib.collapse_rows(cursor (select * from typetable), '~'))
order by parent_value;

-- long concatenation
insert into typetable values
  (1.115, DATE'1111-11-11'),
  (1.115000, DATE'1989-9-11'),
  (1.115, DATE'1670-4-27'),
  (1.115, DATE'1212-12-12'),
  (1.115, DATE'2001-1-1')
;

select * 
from table( applib.collapse_rows(cursor (select * from typetable), '~'))
where parent_value = cast(1.115 as varchar(65535));

-- with view
create view vv as 
select *
from table( applib.collapse_rows( cursor (select * from typetable), '|'));

select * 
from table(
  applib.collapse_rows( cursor(
    select collapsed_row_count, parent_value from vv),
    '*'))
order by parent_value;
 
-- recursive
select * 
from table( 
  applib.collapse_rows(cursor (
    select 
      collapsed_row_count,
      parent_value || ':' || concatenated_child_values 
    from table( 
      applib.collapse_rows(cursor( select * from typetable), '#'))),
    '|'))
order by parent_value;

-- 
-- negative tests
--

-- delimiter over one character will get truncated
select * 
from table( applib.collapse_rows(cursor ( select * from tree), '~~||**'))
order by parent_value;

-- input table with incorrect number of columns
select * 
from table( applib.collapse_rows(cursor (select *, parent||'lolo' from tree), '|'))
order by parent_value;

-- cleanup
drop table tree cascade;
drop table typetable cascade;
drop schema cr cascade;