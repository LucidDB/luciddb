------------------------------------------------------------------------------
-----
-- Basic setup for our tests
-----
------------------------------------------------------------------------------

--{{{ Run basic setup

--create shema
CREATE SCHEMA CST
;

-- Create and populate a table with all datatypes we support

alter system set "codeCacheMaxBytes"=min;

CREATE TABLE CST.alltypes (
t1 bigint primary key, 
-- t2 bit, 
t2 boolean, 
t3 char(21),
t4 date, 
t5 decimal, 
t6 double,
t7 float, 
t8 integer, 
-- t9 long varbinary,
-- t10 long varchar,
t11_1 numeric(8,4), t11_2 numeric(6,2), 
t12 real, 
t13 smallint,
t14 time, 
t15 timestamp, 
t16 tinyint, 
t17 varbinary(10), 
t18 varchar(23))
;

--populate the table
INSERT INTO CST.alltypes VALUES (
124429,
-- 1, 
true, 
'v lesu rodilas', 
date '1996-11-30', 
4.56, 
134.23321, 
223.1123,
1324259, 
-- B'101',
-- 'v lesu on rosla',
12.3, 45.3,
1232.4581,
69,
time '9:21:43',
timestamp '1996-11-30 9:21:43',
15,
X'1101001010',
'zimoy i letom stroynaya')
;


--see what we have in there
SELECT * 
FROM CST.alltypes
;

--}}}

