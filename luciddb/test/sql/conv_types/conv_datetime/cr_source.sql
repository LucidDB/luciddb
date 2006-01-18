--
-- The table, datatype_source, contains all datatypes for use in
-- the date/time conversion tests.  
-- 

create schema s
;
set schema 's'
;


create table datatype_source(
pkey integer
,colname varchar(20)
-- ,colbit bit
,coltiny tinyint
,colsmall smallint
,colint integer
,colbig bigint
-- ,coldec decimal(10,4)
-- ,colnum numeric(16,2)

,coldouble double
,colfloat float
,colreal real

,colchar char(17)
,colvchar varchar(100)

--,colbin binary(11)
,colvbin varbinary(256)

,coltime time
,coldate date
,coltmstamp timestamp
-- ,primary key(pkey,colname,colbit,coltiny,colsmall,colint,colbig,coldec,colnum,coldouble,colfloat,colreal,colchar,colvchar,colbin,colvbin,coltime,coldate,coltmstamp)
,primary key(pkey)
)
;


insert into datatype_source values 
--(0,'BAD', 1, 120, 30000, 45678921, 12121212121212, 987654.3210, 98765432109876.54, 
-- 333333.33333333, 555.55, 7.777777,
-- '34-12', '45-56:78', '10101', '11110000111100001111',
-- '6:40:0', '1917-11-7', '1990-3-24 6:40:0')
(0,'BAD', 120, 30000, 45678921, 12121212121212, 
 333333.33333333, 555.55, 7.777777,
 '34-12', '45-56:78',
-- CAST(X'15' as binary(11)), 
X'0F0F0F',
 '6:40:0', '1917-11-7', '1990-3-24 6:40:0')
;


insert into datatype_source 
 ( pkey, colname, colchar, colvchar, coltime, coltmstamp ) 
values 
 ( 1, 'TIME', '4:5:11', '11:11:11',
   '0:0:0', '9-9-30 21:19:51.175' )
;

insert into datatype_source 
 ( pkey, colname, colchar, colvchar, coldate, coltmstamp ) 
values 
 ( 2, 'DATE', '1888-10-3', '1760-3-11',
   '1974-2-1', '1972-9-30 2:0:0' )
;

insert into datatype_source 
 ( pkey, colname, colchar, colvchar, coltime, coldate, coltmstamp ) 
values 
 ( 3, 'TIMESTAMP', '2323-6-26 0:0:56', '1945-5-9 7:43:11.2',
   '2:12:47', '2002-11-16', '2002-11-16 2:12:47' )
;

insert into datatype_source 
 ( pkey, colname, colchar, colvchar, coltime, coltmstamp ) 
values 
 ( 4, 'TIME', '1400-3-11 4:5:11.321', '1760-3-11 11:11:11',
   '4:5:5.345', '1060-3-11 4:5:11.321' )
;

insert into datatype_source 
 ( pkey, colname, colchar, colvchar, coldate, coltmstamp ) 
values 
 ( 5, 'DATE', '1400-3-11 4:5:11.321', '1775-3-11 11:11:11',
   '1963-4-27', '1963-4-27 2:30:34' )
;

insert into datatype_source 
 ( pkey, colname, colchar, colvchar, coltime, coldate, coltmstamp ) 
values 
 ( 6, 'TIMESTAMP', '2323-6-26', '12:12',
   '2:16:29', '1957-9-29', '1957-9-29 2:16:29.33' )
;

select * from datatype_source where colname = 'BAD'
;

select colchar, colvchar, coltime, coltmstamp
 from datatype_source where colname = 'TIME'
;

select colchar, colvchar, coldate, coltmstamp
 from datatype_source where colname = 'DATE'
;

select colchar, colvchar, coltime, coldate, coltmstamp
 from datatype_source where colname = 'TIMESTAMP'
;

