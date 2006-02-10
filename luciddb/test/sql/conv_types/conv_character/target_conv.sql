set schema 's'
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select cast(coltiny as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target 
 select cast(colsmall as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select cast(colint as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select cast(colbig as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select cast(coldec as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select cast(colnum as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select cast(coldouble as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;

insert into datatype_target
 select cast(colfloat as varchar(256)) from datatype_source
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col char(15))
;


insert into datatype_target
 select cast(colreal as varchar(256)) from datatype_source
;
select * from datatype_target
;
