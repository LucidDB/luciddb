--
-- The table, datatype_source, contains all datatypes for use in
-- the long datatype conversion tests.  
-- 

create schema s
;
set schema 's'
;

create table datatype_source(
--colbit bit
coltiny tinyint
,colsmall smallint
,colint integer
,colbig bigint
-- ,coldec decimal(10,4)
-- ,colnum numeric(16,2)

,coldouble double
,colfloat float
,colreal real

,colchar char(25)
,colvchar varchar(100)
,colbin binary(19)
,colvbin varbinary(256)

,coltime time
,coldate date
,coltmstamp timestamp

-- ,collchar long varchar
-- ,collbin long varbinary
,primary key(coltiny)
)
;

insert into datatype_source values 
--(1, 120, 30000, 45678921, 12121212121212, 987654.3210, 98765432109876.54, 
-- 333333.33333333, 555.55, 7.777777,
-- '34-12', 'a a a a a a', '10101', '11110000111100001111',
-- '6:40:0', '1917-11-7', '1990-3-24 6:40:0',
-- 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
-- '01010101010101010101010101111111111111010101011000000001'
-- )
(120, 30000, 45678921, 12121212121212,
 333333.33333333, 555.55, 7.777777,
 '34-12', 'a a a a a a', 10101, 11110000111100001111,
 '6:40:0', '1917-11-7', '1990-3-24 6:40:0'
 )
;


select * from datatype_source
;
