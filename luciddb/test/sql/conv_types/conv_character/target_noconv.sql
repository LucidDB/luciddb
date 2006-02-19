set schema 's'
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select coltiny from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target 
 select colsmall from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select colint from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select colbig from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select coldec from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select colnum from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select coldouble from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select colfloat from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select colreal from datatype_source
;
select * from datatype_target
;
