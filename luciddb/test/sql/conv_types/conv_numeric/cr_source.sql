-- The table, datatype_source, contains each numeric datatype for use in
-- the conversion tests.  The column 'target_type' is the datatype
-- 

create schema s
;
set schema 's'
;


create table datatype_source(
 target_type varchar(10)
,range_for varchar(6)	   -- min/max values for 'source' or 'target' 
,coltiny tinyint
,colsmall smallint
,colint integer
,colbig bigint
,coldec decimal(14,4)
,colnum numeric(14,4)
,coldouble double
,colfloat float
,colreal real
)
;


insert into datatype_source 
values('tinyint','target',0,0,0,0,0,0,0,0,0)
;

insert into datatype_source
values('tinyint','target',127,127,127,127,127,127,127,127,127)
;

insert into datatype_source
values('tinyint','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('tinyint','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'tinyint'
;


insert into datatype_source
values('smallint','target',-32768,-32768,-32768,-32768,-32768,-32768,-32768,-32768,-32768)
;

insert into datatype_source
values('smallint','target',32767,32767,32767,32767,32767,32767,32767,32767,32767)
;

insert into datatype_source
values('smallint','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('smallint','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'smallint'
;


insert into datatype_source
values('integer','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('integer','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('integer','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('integer','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'integer'
;


insert into datatype_source
values('bigint','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('bigint','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('bigint','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('bigint','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'bigint'
;


insert into datatype_source
values('decimal','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('decimal','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('decimal','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('decimal','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'decimal'
;


insert into datatype_source
values('numeric','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('numeric','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('numeric','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('numeric','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'numeric'
;


insert into datatype_source
values('double','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('double','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('double','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('double','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'double'
;


insert into datatype_source
values('float','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('float','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('float','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('float','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'float'
;


insert into datatype_source
values('real','target',-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('real','target',2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

insert into datatype_source
values('real','source',0,-32768,-2147483647,-2147483647,-2147483647,-2147483647,-4294967296,-4294967296,-4294967296)
;

insert into datatype_source
values('real','source',127,32767,2147483647,2147483647,2147483647,2147483647,4294967296,4294967296,4294967296)
;

select * from datatype_source where target_type = 'real'
;
