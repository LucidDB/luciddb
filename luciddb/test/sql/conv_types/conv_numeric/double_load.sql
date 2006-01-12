set schema 's'
;

-- Test will load all datatypes into a double column.

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- tinyint to double; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- smallint to double; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- integer to double; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- bigint to double; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- decimal to double; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- decimal to double; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- numeric to double; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- numeric to double; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- double to double; min/max range for source datatype [same as target]

insert into datatype_target
 select coldouble from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- float to double; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- float to double; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- real to double; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='double'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- real to double; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='double'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col double primary key)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target
;
-- PASS: if value = 123.456789
