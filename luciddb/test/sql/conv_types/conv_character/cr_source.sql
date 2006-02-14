-- The table, datatype_source, contains each numeric datatype for use in
-- the conversion tests.  
-- 

create schema s
;
set schema 's'
;

-- primary key constraint: LDB-9
create table datatype_source(
 coltiny tinyint
,colsmall smallint
,colint integer
,colbig bigint
,coldec decimal(10,4)
,colnum numeric(10,4)
,coldouble double
,colfloat float
,colreal real
,primary key(coltiny,colsmall,colint,colbig,coldec,colnum,coldouble,colfloat,colreal)
)
;

insert into datatype_source
values(127,32767,2147483647,2147483647,214748.3647,214748.3647,4294967296,4294967296,4294967296)
;

select * from datatype_source
;
