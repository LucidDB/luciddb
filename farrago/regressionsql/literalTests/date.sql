-- $Id$ 

-- test date literal

VALUES date 'January 8, 1999';
VALUES date '1999-01-08';
VALUES date '1999-01-18';
VALUES date '1/8/1999';
VALUES date '1/18/1999';
VALUES date '18/1/1999';
VALUES date '01/02/03';
VALUES date '19990108';
VALUES date '990108';
VALUES date '1999.008';
VALUES date 'J2451187';
VALUES date 'January 8, 99 BC';
VALUES date '99-Jan-08';
VALUES date '1999-Jan-08';
VALUES date '08-Jan-99';
VALUES date '08-Jan-1999';
VALUES date 'Jan-08-99';
VALUES date 'Jan-08-1999';
VALUES date '99-08-Jan';
VALUES date '1999-08-Jan';
VALUES date '99 Jan 08';
VALUES date '1999 Jan 08';
VALUES date '08 Jan 99';
VALUES date '08 Jan 1999';
VALUES date 'Jan 08 99';
VALUES date 'Jan 08 1999';
VALUES date '99 08 Jan';
VALUES date '1999 08 Jan';
VALUES date '99-01-08';
VALUES date '1999-01-08';
VALUES date '08-01-99';
VALUES date '08-01-1999';
VALUES date '01-08-99';
VALUES date '01-08-1999';
VALUES date '99-08-01';
VALUES date '1999-08-01';
VALUES date '99 01 08';
VALUES date '1999 01 08';
VALUES date '08 01 99';
VALUES date '08 01 1999';
VALUES date '01 08 99';
VALUES date '01 08 1999';
VALUES date '99 08 01';
VALUES date '1999 08 01';
VALUES date 'January 8, 1999';
VALUES date '1999-01-08';
VALUES date '1999-01-18';
VALUES date '1/8/1999';
VALUES date '1/18/1999';
VALUES date '18/1/1999';
VALUES date '01/02/03';
VALUES date '19990108';
VALUES date '990108';
VALUES date '1999.008';
VALUES date 'J2451187';
VALUES date 'January 8, 99 BC';
VALUES date '99-Jan-08';
VALUES date '1999-Jan-08';
VALUES date '08-Jan-99';
VALUES date '08-Jan-1999';
VALUES date 'Jan-08-99';
VALUES date 'Jan-08-1999';
VALUES date '99-08-Jan';
VALUES date '1999-08-Jan';
VALUES date '99 Jan 08';
VALUES date '1999 Jan 08';
VALUES date '08 Jan 99';
VALUES date '08 Jan 1999';
VALUES date 'Jan 08 99';
VALUES date 'Jan 08 1999';
VALUES date '99 08 Jan';
VALUES date '1999 08 Jan';
VALUES date '99-01-08';
VALUES date '1999-01-08';
VALUES date '08-01-99';
VALUES date '08-01-1999';
VALUES date '01-08-99';
VALUES date '01-08-1999';
VALUES date '99-08-01';
VALUES date '1999-08-01';
VALUES date '99 01 08';
VALUES date '1999 01 08';
VALUES date '08 01 99';
VALUES date '08 01 1999';
VALUES date '01 08 99';
VALUES date '01 08 1999';
VALUES date '99 08 01';
VALUES date '1999 08 01';
VALUES date 'January 8, 1999';
VALUES date '1999-01-08';
VALUES date '1999-01-18';
VALUES date '1/8/1999';
VALUES date '1/18/1999';
VALUES date '18/1/1999';
VALUES date '01/02/03';
VALUES date '19990108';
VALUES date '990108';
VALUES date '1999.008';
VALUES date 'J2451187';
VALUES date 'January 8, 99 BC';
VALUES date '99-Jan-08';
VALUES date '1999-Jan-08';
VALUES date '08-Jan-99';
VALUES date '08-Jan-1999';
VALUES date 'Jan-08-99';
VALUES date 'Jan-08-1999';
VALUES date '99-08-Jan';
VALUES date '1999-08-Jan';
VALUES date '99 Jan 08';
VALUES date '1999 Jan 08';
VALUES date '08 Jan 99';
VALUES date '08 Jan 1999';
VALUES date 'Jan 08 99';
VALUES date 'Jan 08 1999';
VALUES date '99 08 Jan';
VALUES date '1999 08 Jan';
VALUES date '99-01-08';
VALUES date '1999-01-08';
VALUES date '08-01-99';
VALUES date '08-01-1999';
VALUES date '01-08-99';
VALUES date '01-08-1999';
VALUES date '99-08-01';
VALUES date '1999-08-01';
VALUES date '99 01 08';
VALUES date '1999 01 08';
VALUES date '08 01 99';
VALUES date '08 01 1999';
VALUES date '01 08 99';
VALUES date '01 08 1999';
VALUES date '99 08 01';
VALUES date '1999 08 01';

-- more ISO format tests that should work
values date '1969-0004-0026';
values date '2005-4-3';
values date '2004-2-30';
values date '20005-4-3';
-- these should fail (bad month or date)
values date '2003-10-32';
values date '2003-13-10';

-- test datatype
create schema test;
set schema 'test';
create table t_date(date_col date not null primary key, date_col2 date);

-- negative test
insert into t_date values('string value',null);
insert into t_date values(true,null);
insert into t_date values(1234,null);
insert into t_date values(1e400,null);
insert into t_date values(-1.2345678901234e-200,null);
insert into t_date values(-1234.03,null);
insert into t_date values(x'ff',null);
insert into t_date values(time '12:01:01',null);
-- the next insert statement should be allowed.
-- insert into t_date values(timestamp '2004-12-01 12:01:01',null);

-- insert the right values
insert into t_date values(date '1999-01-08',null);

-- null value test
--insert into t_date values(null, null); 

select * from t_date;

drop table t_date;
drop schema test;
