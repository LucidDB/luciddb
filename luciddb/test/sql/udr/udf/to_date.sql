-- $Id$
-- Tests for toDate UDF
set schema 'udftest';
set path 'udftest';

create table strdates (colname varchar(10), colstr varchar(50), colmask varchar(50));

insert into strdates values
('GOOD', '10f 12t 1999p', 'DDf MMt YYYYp'),
('GOOD', '1468 30 08', 'YYYY DD MM'),
('GOOD', '11p 19t 1650g', 'MMp DDt YYYYg'),
('GOOD', '10 2001 31', 'mM YyYy Dd'),
('GOOD', '1995.01.05', 'YYYY.MM.DD'),
('GOOD', '8 67 1', 'm yy d');

-- failures
values applib.convert_date('JAN, 23 2009', 'mmm, dd yyyy');
values applib.convert_date('12m, 9d, 1004y', 'mmm, 23m, 1004y');
values applib.convert_date('7-9-97', 'DD-MM-YY');

-- null input
values applib.convert_date(cast(null as varchar(22)), 'mm.dd.yyyy');
values applib.convert_date('jan 23, 2003', cast(null as varchar(10)));

-- create view with reference
create view td as
select colname, applib.convert_date(colstr, colmask)
from strdates;

select * from td
order by 1;

-- nested
select applib.convert_date( cast( applib.convert_date(colstr, colmask) as varchar(50)), 'YYYY-MM-DD') 
from strdates;

-- cleanup
drop view td;
drop table strdates;
