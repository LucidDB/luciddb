-- $Id$ 

SELECT 'unknown' from values('TRUE');

-- test datatype
create schema test;
set schema test;
create table t_char(char_col char(1) not null primary key, char_col2 char(1));

-- negative test
insert into t_char values(true,null);
insert into t_char values(1234,null);
insert into t_char values(10e400,null);
insert into t_char values(-1.2345678901234e-200,null);
insert into t_char values(-1234.03,null);
insert into t_char values(x'ff',null);
insert into t_char values(b'10',null);
-- char(1) too short to hold these - will give error
insert into t_char values(time '12:01:01',null);
insert into t_char values(date '1999-01-08',null);
insert into t_char values(TIMESTAMP '2004-12-01 12:01:01',null);

-- REVIEW: SZ: 8/11/2004: not strictly an error.  this should truncate and
--  generate a warning, which we'll probably never see in sqlline.
insert into t_char values('string value',null);

-- insert the right values
insert into t_char values('m',null);

-- null value test
insert into t_char values(null, null); 

select * from t_char;

drop table t_char;

-- test char datatype with bigger width
create table t_char(char_col char(20) not null primary key, char_col2 char(1));
insert into t_char values(time '12:01:01',null);
insert into t_char values(date '1999-01-08',null);
insert into t_char values(TIMESTAMP '2004-12-01 12:01:01',null);

select * from t_char;

drop table t_char;

-- test varchar datatype
create schema test;
set schema test;
create table t_varchar(varchar_col varchar(30) not null primary key, varchar_col2 varchar(1));

-- negative test
insert into t_varchar values(true,null);
insert into t_varchar values(false,null);
insert into t_varchar values(10e400,null);
insert into t_varchar values(x'ff',null);
insert into t_varchar values(b'10',null);
select * from t_varchar;

-- warning tests
-- 31 character string truncates to 30, but should get an warning
insert into t_varchar values('1234567890123456789012345678901',null);
select * from t_varchar;

-- positive tests
insert into t_varchar values(4444,null);
insert into t_varchar values(99999999,null);
insert into t_varchar values(0.0,null);
insert into t_varchar values(-1.1,null);
insert into t_varchar values(-4444.22,null);
insert into t_varchar values('1.23E-50',null);
insert into t_varchar values('1.23E50',null);
insert into t_varchar values('-1.23E50',null);
insert into t_varchar values('-1.23E-50',null);
insert into t_varchar values(-1.2345678901234e-200,null);
insert into t_varchar values(time '12:01:01',null);
insert into t_varchar values(date '1999-01-08',null);
insert into t_varchar values(TIMESTAMP '2004-12-01 12:01:01',null);

-- null value test
insert into t_varchar values(null, null); 

select * from t_varchar;

drop table t_varchar;

