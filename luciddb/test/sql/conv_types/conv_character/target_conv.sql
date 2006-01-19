set schema 's'
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select to_char(coltiny) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target 
 select to_char(colsmall) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select to_char(colint) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select to_char(colbig) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select to_char(coldec) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select to_char(colnum) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select to_char(coldouble) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select to_char(colfloat) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select to_char(colreal) from datatype_source
;
select * from datatype_target
;
