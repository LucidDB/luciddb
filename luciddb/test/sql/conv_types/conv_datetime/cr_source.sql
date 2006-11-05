--
-- The table, datatype_source, contains all datatypes for use in
-- the date/time conversion tests.  
-- 

create schema s
;
set schema 's'
;


create table datatype_source(
colname varchar(20)
-- ,colbit bit
,colbit boolean
,coltiny tinyint
,colsmall smallint
,colint integer
,colbig bigint
,coldec decimal(10,4)
,colnum numeric(16,2)

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
)
;


insert into datatype_source values 
--('BAD', 1, 120, 30000, 45678921, 12121212121212, 987654.3210, 98765432109876.54, 
-- 333333.33333333, 555.55, 7.777777,
-- '34-12', '45-56:78', '10101', '11110000111100001111',
-- '6:40:0', '1917-11-7', '1990-3-24 6:40:0')
('BAD', true, 120, 30000, 45678921, 12121212121212, 987654.3210, 98765432109876.54, 
 333333.33333333, 555.55, 7.777777,
 '34-12', '45-56:78',
-- CAST(X'15' as binary(11)), 
X'0F0F0F',
time '6:40:0', date '1917-11-7', timestamp '1990-3-24 6:40:0')
;


insert into datatype_source 
 ( colname, colchar, colvchar, coltime, coltmstamp ) 
values 
 ( 'TIME', '4:5:11', '11:11:11',
   time '0:0:0', timestamp '9-9-30 21:19:51.175' )
;

insert into datatype_source 
 ( colname, colchar, colvchar, coldate, coltmstamp ) 
values 
 ( 'DATE', '1888-10-3 00:00:00', '1760-3-11 0:0:0',
   date '1974-2-1', timestamp '1972-9-30 2:0:0' )
;

insert into datatype_source 
 ( colname, colchar, colvchar, coltime, coldate, coltmstamp ) 
values 
 ( 'TIMESTAMP', '2323-6-26 0:0:56', '1945-5-9 7:43:11.2',
   time '2:12:47', date '2002-11-16', timestamp '2002-11-16 2:12:47' )
;

-- FRG-57
insert into datatype_source 
 ( colname, colchar, colvchar, coltime, coltmstamp ) 
values 
 ( 'TIME', '1400-3-11 4:5:11.321', '1760-3-11 11:11:11',
   time '4:5:5.345', timestamp '1060-3-11 4:5:11.321' )
;

-- FRG-57
insert into datatype_source 
 ( colname, colchar, colvchar, coldate, coltmstamp ) 
values 
 ( 'DATE', '1400-3-11 4:5:11.321', '1775-3-11 11:11:11',
   date '1963-4-27', timestamp '1963-4-27 2:30:34' )
;

insert into datatype_source 
 ( colname, colchar, colvchar, coltime, coldate, coltmstamp ) 
values 
 ( 'TIMESTAMP', '2323-6-26', '12:12',
   time '2:16:29', date '1957-9-29', timestamp '1957-9-29 2:16:29.33' )
;

select 
  colname, colbit, coltiny, colsmall, colint, colbig, coldec, colnum,
  coldouble, colreal, colchar, colvchar, colvbin, coltime, coldate, coltmstamp
from datatype_source where colname = 'BAD'
;

-- set numberFormat since floating point differs based on VM
!set numberFormat 0.00
select colfloat from datatype_source where colname = 'BAD';
!set numberFormat default

select colchar, colvchar, coltime, coltmstamp
 from datatype_source where colname = 'TIME'
;

select colchar, colvchar, coldate, coltmstamp
 from datatype_source where colname = 'DATE'
;

select colchar, colvchar, coltime, coldate, coltmstamp
 from datatype_source where colname = 'TIMESTAMP'
;

