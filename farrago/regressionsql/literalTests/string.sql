-- $Id$ 

SELECT 'unknown' from values('TRUE');

-- test datatype
create schema test;
set schema test;
create table t_char(char_col char(1) not null primary key, char_col2 char(1));

-- negative test
-- bug  ( fix me)
--insert into t_char values('string value',null);
insert into t_char values(true,null);
insert into t_char values(1234,null);
insert into t_char values(10e400,null);
insert into t_char values(-1.2345678901234e-200,null);
insert into t_char values(-1234.03,null);
insert into t_char values(x'ff',null);
insert into t_char values(b'10',null);
-- bug: fix me
--insert into t_char values(time '12:01:01',null);
--insert into t_char values(date '1999-01-08',null);
-- bug: the value is not insert correctly. You can use the select
-- statement to verify
-- insert into t_char values(TIMESTAMP '2004-12-01 12:01:01',null);

-- insert the right values
insert into t_char values('m',null);

-- null value test
--insert into t_char values(null, null); 

select * from t_char;

drop table t_char;

-- test varchar datatype
create schema test;
set schema test;
create table t_varchar(varchar_col varchar(10) not null primary key, varchar_col2 varchar(1));

-- negative test
insert into t_varchar values('string value123456789',null);
select * from t_varchar;
insert into t_varchar values(true,null);
insert into t_varchar values(1234,null);
insert into t_varchar values(10e400,null);
insert into t_varchar values(-1.2345678901234e-200,null);
insert into t_varchar values(-1234.03,null);
insert into t_varchar values(x'ff',null);
insert into t_varchar values(b'10',null);
-- bug: fix me
--insert into t_varchar values(time '12:01:01',null);
--insert into t_varchar values(date '1999-01-08',null);
-- reenable the following after insert timestamp bug is fixed
--insert into t_varchar values(TIMESTAMP '2004-12-01 12:01:01',null);

-- insert the right values
insert into t_varchar values('1234567890',null);

-- null value test
--insert into t_varchar values(null, null); 

select * from t_varchar;

drop table t_varchar;

