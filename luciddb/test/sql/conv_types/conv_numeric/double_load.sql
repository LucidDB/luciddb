set schema 's'
;

-- Test will load all datatypes into a double column.

drop table datatype_target
;
create table datatype_target(col double)
;

-- tinyint to double; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- smallint to double; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- integer to double; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- bigint to double; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- decimal to double; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- decimal to double; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- numeric to double; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- numeric to double; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- double to double; min/max range for source datatype [same as target]

insert into datatype_target
 select coldouble from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- float to double; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- float to double; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- real to double; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- real to double; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col double)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target order by 1
;
-- PASS: if value = 123.456789
