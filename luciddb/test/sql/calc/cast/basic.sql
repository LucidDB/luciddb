------------------------------------------------------------------------------
-----
--  Tests for basic CAST functionality
-----
------------------------------------------------------------------------------

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

-- t1
SELECT t1, CAST (t1 AS bigint) FROM CST.alltypes;
-- SELECT t1, CAST (t1 AS bit) FROM CST.alltypes;
SELECT t1, CAST (t1 AS boolean) FROM CST.alltypes;
SELECT t1, CAST (t1 AS char(21)) FROM CST.alltypes;
SELECT t1, CAST (t1 AS date) FROM CST.alltypes;
SELECT t1, CAST (t1 AS decimal) FROM CST.alltypes;
SELECT t1, CAST (t1 AS double) FROM CST.alltypes;
SELECT t1, CAST (t1 AS float) FROM CST.alltypes;
SELECT t1, CAST (t1 AS integer) FROM CST.alltypes;
-- SELECT t1, CAST (t1 AS long varbinary) FROM CST.alltypes;
-- SELECT t1, CAST (t1 AS long varchar) FROM CST.alltypes;
SELECT t1, CAST (t1 AS numeric(8,4)) FROM CST.alltypes;
SELECT t1, CAST (t1 AS numeric(6,2)) FROM CST.alltypes;
SELECT t1, CAST (t1 AS real) FROM CST.alltypes;
SELECT t1, CAST (t1 AS smallint) FROM CST.alltypes;
SELECT t1, CAST (t1 AS time) FROM CST.alltypes;
SELECT t1, CAST (t1 AS timestamp) FROM CST.alltypes;
SELECT t1, CAST (t1 AS tinyint) FROM CST.alltypes;
SELECT t1, CAST (t1 AS varbinary(10)) FROM CST.alltypes;
SELECT t1, CAST (t1 AS varchar(23)) FROM CST.alltypes;

-- t2
SELECT t2, CAST (t2 AS bigint) FROM CST.alltypes;
-- SELECT t2, CAST (t2 AS bit) FROM CST.alltypes;
SELECT t2, CAST (t2 AS boolean) FROM CST.alltypes;
SELECT t2, CAST (t2 AS char(21)) FROM CST.alltypes;
SELECT t2, CAST (t2 AS date) FROM CST.alltypes;
SELECT t2, CAST (t2 AS decimal) FROM CST.alltypes;
SELECT t2, CAST (t2 AS double) FROM CST.alltypes;
SELECT t2, CAST (t2 AS float) FROM CST.alltypes;
SELECT t2, CAST (t2 AS integer) FROM CST.alltypes;
-- SELECT t2, CAST (t2 AS long varbinary) FROM CST.alltypes;
-- SELECT t2, CAST (t2 AS long varchar) FROM CST.alltypes;
SELECT t2, CAST (t2 AS numeric(8,4)) FROM CST.alltypes;
SELECT t2, CAST (t2 AS numeric(6,2)) FROM CST.alltypes;
SELECT t2, CAST (t2 AS real) FROM CST.alltypes;
SELECT t2, CAST (t2 AS smallint) FROM CST.alltypes;
SELECT t2, CAST (t2 AS time) FROM CST.alltypes;
SELECT t2, CAST (t2 AS timestamp) FROM CST.alltypes;
SELECT t2, CAST (t2 AS tinyint) FROM CST.alltypes;
SELECT t2, CAST (t2 AS varbinary(10)) FROM CST.alltypes;
SELECT t2, CAST (t2 AS varchar(23)) FROM CST.alltypes;

-- t3
SELECT t3, CAST (t3 AS bigint) FROM CST.alltypes;
-- SELECT t3, CAST (t3 AS bit) FROM CST.alltypes;
SELECT t3, CAST (t3 AS boolean) FROM CST.alltypes;
SELECT t3, CAST (t3 AS char(21)) FROM CST.alltypes;
SELECT t3, CAST (t3 AS date) FROM CST.alltypes;
SELECT t3, CAST (t3 AS decimal) FROM CST.alltypes;
SELECT t3, CAST (t3 AS double) FROM CST.alltypes;
SELECT t3, CAST (t3 AS float) FROM CST.alltypes;
SELECT t3, CAST (t3 AS integer) FROM CST.alltypes;
-- SELECT t3, CAST (t3 AS long varbinary) FROM CST.alltypes;
-- SELECT t3, CAST (t3 AS long varchar) FROM CST.alltypes;
SELECT t3, CAST (t3 AS numeric(8,4)) FROM CST.alltypes;
SELECT t3, CAST (t3 AS numeric(6,2)) FROM CST.alltypes;
SELECT t3, CAST (t3 AS real) FROM CST.alltypes;
SELECT t3, CAST (t3 AS smallint) FROM CST.alltypes;
SELECT t3, CAST (t3 AS time) FROM CST.alltypes;
SELECT t3, CAST (t3 AS timestamp) FROM CST.alltypes;
SELECT t3, CAST (t3 AS tinyint) FROM CST.alltypes;
SELECT t3, CAST (t3 AS varbinary(10)) FROM CST.alltypes;
SELECT t3, CAST (t3 AS varchar(23)) FROM CST.alltypes;

-- t4
SELECT t4, CAST (t4 AS bigint) FROM CST.alltypes;
-- SELECT t4, CAST (t4 AS bit) FROM CST.alltypes;
SELECT t4, CAST (t4 AS boolean) FROM CST.alltypes;
SELECT t4, CAST (t4 AS char(21)) FROM CST.alltypes;
SELECT t4, CAST (t4 AS date) FROM CST.alltypes;
SELECT t4, CAST (t4 AS decimal) FROM CST.alltypes;
SELECT t4, CAST (t4 AS double) FROM CST.alltypes;
SELECT t4, CAST (t4 AS float) FROM CST.alltypes;
SELECT t4, CAST (t4 AS integer) FROM CST.alltypes;
-- SELECT t4, CAST (t4 AS long varbinary) FROM CST.alltypes;
-- SELECT t4, CAST (t4 AS long varchar) FROM CST.alltypes;
SELECT t4, CAST (t4 AS numeric(8,4)) FROM CST.alltypes;
SELECT t4, CAST (t4 AS numeric(6,2)) FROM CST.alltypes;
SELECT t4, CAST (t4 AS real) FROM CST.alltypes;
SELECT t4, CAST (t4 AS smallint) FROM CST.alltypes;
SELECT t4, CAST (t4 AS time) FROM CST.alltypes;
-- FRG-20
SELECT t4, CAST (t4 AS timestamp) FROM CST.alltypes;
SELECT t4, CAST (t4 AS tinyint) FROM CST.alltypes;
SELECT t4, CAST (t4 AS varbinary(10)) FROM CST.alltypes;
SELECT t4, CAST (t4 AS varchar(23)) FROM CST.alltypes;

-- t5
SELECT t5, CAST (t5 AS bigint) FROM CST.alltypes;
-- SELECT t5, CAST (t5 AS bit) FROM CST.alltypes;
SELECT t5, CAST (t5 AS boolean) FROM CST.alltypes;
SELECT t5, CAST (t5 AS char(21)) FROM CST.alltypes;
SELECT t5, CAST (t5 AS date) FROM CST.alltypes;
SELECT t5, CAST (t5 AS decimal) FROM CST.alltypes;
SELECT t5, CAST (t5 AS double) FROM CST.alltypes;
SELECT t5, CAST (t5 AS float) FROM CST.alltypes;
SELECT t5, CAST (t5 AS integer) FROM CST.alltypes;
-- SELECT t5, CAST (t5 AS long varbinary) FROM CST.alltypes;
-- SELECT t5, CAST (t5 AS long varchar) FROM CST.alltypes;
SELECT t5, CAST (t5 AS numeric(8,4)) FROM CST.alltypes;
SELECT t5, CAST (t5 AS numeric(6,2)) FROM CST.alltypes;
SELECT t5, CAST (t5 AS real) FROM CST.alltypes;
SELECT t5, CAST (t5 AS smallint) FROM CST.alltypes;
SELECT t5, CAST (t5 AS time) FROM CST.alltypes;
SELECT t5, CAST (t5 AS timestamp) FROM CST.alltypes;
SELECT t5, CAST (t5 AS tinyint) FROM CST.alltypes;
SELECT t5, CAST (t5 AS varbinary(10)) FROM CST.alltypes;
SELECT t5, CAST (t5 AS varchar(23)) FROM CST.alltypes;

-- t6
SELECT t6, CAST (t6 AS bigint) FROM CST.alltypes;
-- SELECT t6, CAST (t6 AS bit) FROM CST.alltypes;
SELECT t6, CAST (t6 AS boolean) FROM CST.alltypes;
SELECT t6, CAST (t6 AS char(21)) FROM CST.alltypes;
SELECT t6, CAST (t6 AS date) FROM CST.alltypes;
SELECT t6, CAST (t6 AS decimal) FROM CST.alltypes;
SELECT t6, CAST (t6 AS double) FROM CST.alltypes;
SELECT t6, CAST (t6 AS float) FROM CST.alltypes;
SELECT t6, CAST (t6 AS integer) FROM CST.alltypes;
-- SELECT t6, CAST (t6 AS long varbinary) FROM CST.alltypes;
-- SELECT t6, CAST (t6 AS long varchar) FROM CST.alltypes;
SELECT t6, CAST (t6 AS numeric(8,4)) FROM CST.alltypes;
SELECT t6, CAST (t6 AS numeric(6,2)) FROM CST.alltypes;
SELECT t6, CAST (t6 AS real) FROM CST.alltypes;
SELECT t6, CAST (t6 AS smallint) FROM CST.alltypes;
SELECT t6, CAST (t6 AS time) FROM CST.alltypes;
SELECT t6, CAST (t6 AS timestamp) FROM CST.alltypes;
SELECT t6, CAST (t6 AS tinyint) FROM CST.alltypes;
SELECT t6, CAST (t6 AS varbinary(10)) FROM CST.alltypes;
SELECT t6, CAST (t6 AS varchar(23)) FROM CST.alltypes;

-- t7
SELECT t7, CAST (t7 AS bigint) FROM CST.alltypes;
-- SELECT t7, CAST (t7 AS bit) FROM CST.alltypes;
SELECT t7, CAST (t7 AS boolean) FROM CST.alltypes;
SELECT t7, CAST (t7 AS char(21)) FROM CST.alltypes;
SELECT t7, CAST (t7 AS date) FROM CST.alltypes;
SELECT t7, CAST (t7 AS decimal) FROM CST.alltypes;
SELECT t7, CAST (t7 AS double) FROM CST.alltypes;
SELECT t7, CAST (t7 AS float) FROM CST.alltypes;
SELECT t7, CAST (t7 AS integer) FROM CST.alltypes;
-- SELECT t7, CAST (t7 AS long varbinary) FROM CST.alltypes;
-- SELECT t7, CAST (t7 AS long varchar) FROM CST.alltypes;
SELECT t7, CAST (t7 AS numeric(8,4)) FROM CST.alltypes;
SELECT t7, CAST (t7 AS numeric(6,2)) FROM CST.alltypes;
SELECT t7, CAST (t7 AS real) FROM CST.alltypes;
SELECT t7, CAST (t7 AS smallint) FROM CST.alltypes;
SELECT t7, CAST (t7 AS time) FROM CST.alltypes;
SELECT t7, CAST (t7 AS timestamp) FROM CST.alltypes;
SELECT t7, CAST (t7 AS tinyint) FROM CST.alltypes;
SELECT t7, CAST (t7 AS varbinary(10)) FROM CST.alltypes;
SELECT t7, CAST (t7 AS varchar(23)) FROM CST.alltypes;

-- t8
SELECT t8, CAST (t8 AS bigint) FROM CST.alltypes;
-- SELECT t8, CAST (t8 AS bit) FROM CST.alltypes;
SELECT t8, CAST (t8 AS boolean) FROM CST.alltypes;
SELECT t8, CAST (t8 AS char(21)) FROM CST.alltypes;
SELECT t8, CAST (t8 AS date) FROM CST.alltypes;
SELECT t8, CAST (t8 AS decimal) FROM CST.alltypes;
SELECT t8, CAST (t8 AS double) FROM CST.alltypes;
SELECT t8, CAST (t8 AS float) FROM CST.alltypes;
SELECT t8, CAST (t8 AS integer) FROM CST.alltypes;
-- SELECT t8, CAST (t8 AS long varbinary) FROM CST.alltypes;
-- SELECT t8, CAST (t8 AS long varchar) FROM CST.alltypes;
SELECT t8, CAST (t8 AS numeric(8,4)) FROM CST.alltypes;
SELECT t8, CAST (t8 AS numeric(6,2)) FROM CST.alltypes;
SELECT t8, CAST (t8 AS real) FROM CST.alltypes;
SELECT t8, CAST (t8 AS smallint) FROM CST.alltypes;
SELECT t8, CAST (t8 AS time) FROM CST.alltypes;
SELECT t8, CAST (t8 AS timestamp) FROM CST.alltypes;
SELECT t8, CAST (t8 AS tinyint) FROM CST.alltypes;
SELECT t8, CAST (t8 AS varbinary(10)) FROM CST.alltypes;
SELECT t8, CAST (t8 AS varchar(23)) FROM CST.alltypes;

-- t9
SELECT t9, CAST (t9 AS bigint) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS bit) FROM CST.alltypes;
SELECT t9, CAST (t9 AS boolean) FROM CST.alltypes;
SELECT t9, CAST (t9 AS char(21)) FROM CST.alltypes;
SELECT t9, CAST (t9 AS date) FROM CST.alltypes;
SELECT t9, CAST (t9 AS decimal) FROM CST.alltypes;
SELECT t9, CAST (t9 AS double) FROM CST.alltypes;
SELECT t9, CAST (t9 AS float) FROM CST.alltypes;
SELECT t9, CAST (t9 AS integer) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS long varbinary) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS long varchar) FROM CST.alltypes;
SELECT t9, CAST (t9 AS numeric(8,4)) FROM CST.alltypes;
SELECT t9, CAST (t9 AS numeric(6,2)) FROM CST.alltypes;
SELECT t9, CAST (t9 AS real) FROM CST.alltypes;
SELECT t9, CAST (t9 AS smallint) FROM CST.alltypes;
SELECT t9, CAST (t9 AS time) FROM CST.alltypes;
SELECT t9, CAST (t9 AS timestamp) FROM CST.alltypes;
SELECT t9, CAST (t9 AS tinyint) FROM CST.alltypes;
SELECT t9, CAST (t9 AS varbinary(10)) FROM CST.alltypes;
SELECT t9, CAST (t9 AS varchar(23)) FROM CST.alltypes;

-- t10
SELECT t10, CAST (t10 AS bigint) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS bit) FROM CST.alltypes;
SELECT t10, CAST (t10 AS boolean) FROM CST.alltypes;
SELECT t10, CAST (t10 AS char(21)) FROM CST.alltypes;
SELECT t10, CAST (t10 AS date) FROM CST.alltypes;
SELECT t10, CAST (t10 AS decimal) FROM CST.alltypes;
SELECT t10, CAST (t10 AS double) FROM CST.alltypes;
SELECT t10, CAST (t10 AS float) FROM CST.alltypes;
SELECT t10, CAST (t10 AS integer) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS long varbinary) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS long varchar) FROM CST.alltypes;
SELECT t10, CAST (t10 AS numeric(8,4)) FROM CST.alltypes;
SELECT t10, CAST (t10 AS numeric(6,2)) FROM CST.alltypes;
SELECT t10, CAST (t10 AS real) FROM CST.alltypes;
SELECT t10, CAST (t10 AS smallint) FROM CST.alltypes;
SELECT t10, CAST (t10 AS time) FROM CST.alltypes;
SELECT t10, CAST (t10 AS timestamp) FROM CST.alltypes;
SELECT t10, CAST (t10 AS tinyint) FROM CST.alltypes;
SELECT t10, CAST (t10 AS varbinary(10)) FROM CST.alltypes;
SELECT t10, CAST (t10 AS varchar(23)) FROM CST.alltypes;

-- t11_1
SELECT t11_1, CAST (t11_1 AS bigint) FROM CST.alltypes;
-- SELECT t11_1, CAST (t11_1 AS bit) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS boolean) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS char(21)) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS date) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS decimal) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS double) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS float) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS integer) FROM CST.alltypes;
-- SELECT t11_1, CAST (t11_1 AS long varbinary) FROM CST.alltypes;
-- SELECT t11_1, CAST (t11_1 AS long varchar) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS numeric(8,4)) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS numeric(6,2)) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS real) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS smallint) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS time) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS timestamp) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS tinyint) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS varbinary(10)) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS varchar(23)) FROM CST.alltypes;

-- t11_2
SELECT t11_2, CAST (t11_2 AS bigint) FROM CST.alltypes;
-- SELECT t11_2, CAST (t11_2 AS bit) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS boolean) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS char(21)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS date) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS decimal) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS double) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS float) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS integer) FROM CST.alltypes;
-- SELECT t11_2, CAST (t11_2 AS long varbinary) FROM CST.alltypes;
-- SELECT t11_2, CAST (t11_2 AS long varchar) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS numeric(8,4)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS numeric(6,2)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS real) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS smallint) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS time) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS timestamp) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS tinyint) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS varbinary(10)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS varchar(23)) FROM CST.alltypes;

-- t12
SELECT t12, CAST (t12 AS bigint) FROM CST.alltypes;
-- SELECT t12, CAST (t12 AS bit) FROM CST.alltypes;
SELECT t12, CAST (t12 AS boolean) FROM CST.alltypes;
SELECT t12, CAST (t12 AS char(21)) FROM CST.alltypes;
SELECT t12, CAST (t12 AS date) FROM CST.alltypes;
SELECT t12, CAST (t12 AS decimal) FROM CST.alltypes;
SELECT t12, CAST (t12 AS double) FROM CST.alltypes;
SELECT t12, CAST (t12 AS float) FROM CST.alltypes;
SELECT t12, CAST (t12 AS integer) FROM CST.alltypes;
-- SELECT t12, CAST (t12 AS long varbinary) FROM CST.alltypes;
-- SELECT t12, CAST (t12 AS long varchar) FROM CST.alltypes;
SELECT t12, CAST (t12 AS numeric(8,4)) FROM CST.alltypes;
SELECT t12, CAST (t12 AS numeric(6,2)) FROM CST.alltypes;
SELECT t12, CAST (t12 AS real) FROM CST.alltypes;
SELECT t12, CAST (t12 AS smallint) FROM CST.alltypes;
SELECT t12, CAST (t12 AS time) FROM CST.alltypes;
SELECT t12, CAST (t12 AS timestamp) FROM CST.alltypes;
SELECT t12, CAST (t12 AS tinyint) FROM CST.alltypes;
SELECT t12, CAST (t12 AS varbinary(10)) FROM CST.alltypes;
SELECT t12, CAST (t12 AS varchar(23)) FROM CST.alltypes;

-- t13
SELECT t13, CAST (t13 AS bigint) FROM CST.alltypes;
-- SELECT t13, CAST (t13 AS bit) FROM CST.alltypes;
SELECT t13, CAST (t13 AS boolean) FROM CST.alltypes;
SELECT t13, CAST (t13 AS char(21)) FROM CST.alltypes;
SELECT t13, CAST (t13 AS date) FROM CST.alltypes;
SELECT t13, CAST (t13 AS decimal) FROM CST.alltypes;
SELECT t13, CAST (t13 AS double) FROM CST.alltypes;
SELECT t13, CAST (t13 AS float) FROM CST.alltypes;
SELECT t13, CAST (t13 AS integer) FROM CST.alltypes;
-- SELECT t13, CAST (t13 AS long varbinary) FROM CST.alltypes;
-- SELECT t13, CAST (t13 AS long varchar) FROM CST.alltypes;
SELECT t13, CAST (t13 AS numeric(8,4)) FROM CST.alltypes;
SELECT t13, CAST (t13 AS numeric(6,2)) FROM CST.alltypes;
SELECT t13, CAST (t13 AS real) FROM CST.alltypes;
SELECT t13, CAST (t13 AS smallint) FROM CST.alltypes;
SELECT t13, CAST (t13 AS time) FROM CST.alltypes;
SELECT t13, CAST (t13 AS timestamp) FROM CST.alltypes;
SELECT t13, CAST (t13 AS tinyint) FROM CST.alltypes;
SELECT t13, CAST (t13 AS varbinary(10)) FROM CST.alltypes;
SELECT t13, CAST (t13 AS varchar(23)) FROM CST.alltypes;

-- t14
SELECT t14, CAST (t14 AS bigint) FROM CST.alltypes;
-- SELECT t14, CAST (t14 AS bit) FROM CST.alltypes;
SELECT t14, CAST (t14 AS boolean) FROM CST.alltypes;
SELECT t14, CAST (t14 AS char(21)) FROM CST.alltypes;
SELECT t14, CAST (t14 AS date) FROM CST.alltypes;
SELECT t14, CAST (t14 AS decimal) FROM CST.alltypes;
SELECT t14, CAST (t14 AS double) FROM CST.alltypes;
SELECT t14, CAST (t14 AS float) FROM CST.alltypes;
SELECT t14, CAST (t14 AS integer) FROM CST.alltypes;
-- SELECT t14, CAST (t14 AS long varbinary) FROM CST.alltypes;
-- SELECT t14, CAST (t14 AS long varchar) FROM CST.alltypes;
SELECT t14, CAST (t14 AS numeric(8,4)) FROM CST.alltypes;
SELECT t14, CAST (t14 AS numeric(6,2)) FROM CST.alltypes;
SELECT t14, CAST (t14 AS real) FROM CST.alltypes;
SELECT t14, CAST (t14 AS smallint) FROM CST.alltypes;
SELECT t14, CAST (t14 AS time) FROM CST.alltypes;
SELECT t14, CAST (t14 AS timestamp) FROM CST.alltypes;
SELECT t14, CAST (t14 AS tinyint) FROM CST.alltypes;
SELECT t14, CAST (t14 AS varbinary(10)) FROM CST.alltypes;
SELECT t14, CAST (t14 AS varchar(23)) FROM CST.alltypes;

-- t15
SELECT t15, CAST (t15 AS bigint) FROM CST.alltypes;
-- SELECT t15, CAST (t15 AS bit) FROM CST.alltypes;
SELECT t15, CAST (t15 AS boolean) FROM CST.alltypes;
SELECT t15, CAST (t15 AS char(21)) FROM CST.alltypes;
SELECT t15, CAST (t15 AS date) FROM CST.alltypes;
SELECT t15, CAST (t15 AS decimal) FROM CST.alltypes;
SELECT t15, CAST (t15 AS double) FROM CST.alltypes;
SELECT t15, CAST (t15 AS float) FROM CST.alltypes;
SELECT t15, CAST (t15 AS integer) FROM CST.alltypes;
-- SELECT t15, CAST (t15 AS long varbinary) FROM CST.alltypes;
-- SELECT t15, CAST (t15 AS long varchar) FROM CST.alltypes;
SELECT t15, CAST (t15 AS numeric(8,4)) FROM CST.alltypes;
SELECT t15, CAST (t15 AS numeric(6,2)) FROM CST.alltypes;
SELECT t15, CAST (t15 AS real) FROM CST.alltypes;
SELECT t15, CAST (t15 AS smallint) FROM CST.alltypes;
SELECT t15, CAST (t15 AS time) FROM CST.alltypes;
SELECT t15, CAST (t15 AS timestamp) FROM CST.alltypes;
SELECT t15, CAST (t15 AS tinyint) FROM CST.alltypes;
SELECT t15, CAST (t15 AS varbinary(10)) FROM CST.alltypes;
SELECT t15, CAST (t15 AS varchar(23)) FROM CST.alltypes;

-- t16
SELECT t16, CAST (t16 AS bigint) FROM CST.alltypes;
-- SELECT t16, CAST (t16 AS bit) FROM CST.alltypes;
SELECT t16, CAST (t16 AS boolean) FROM CST.alltypes;
SELECT t16, CAST (t16 AS char(21)) FROM CST.alltypes;
SELECT t16, CAST (t16 AS date) FROM CST.alltypes;
SELECT t16, CAST (t16 AS decimal) FROM CST.alltypes;
SELECT t16, CAST (t16 AS double) FROM CST.alltypes;
SELECT t16, CAST (t16 AS float) FROM CST.alltypes;
SELECT t16, CAST (t16 AS integer) FROM CST.alltypes;
-- SELECT t16, CAST (t16 AS long varbinary) FROM CST.alltypes;
-- SELECT t16, CAST (t16 AS long varchar) FROM CST.alltypes;
SELECT t16, CAST (t16 AS numeric(8,4)) FROM CST.alltypes;
SELECT t16, CAST (t16 AS numeric(6,2)) FROM CST.alltypes;
SELECT t16, CAST (t16 AS real) FROM CST.alltypes;
SELECT t16, CAST (t16 AS smallint) FROM CST.alltypes;
SELECT t16, CAST (t16 AS time) FROM CST.alltypes;
SELECT t16, CAST (t16 AS timestamp) FROM CST.alltypes;
SELECT t16, CAST (t16 AS tinyint) FROM CST.alltypes;
SELECT t16, CAST (t16 AS varbinary(10)) FROM CST.alltypes;
SELECT t16, CAST (t16 AS varchar(23)) FROM CST.alltypes;

-- t17
SELECT t17, CAST (t17 AS bigint) FROM CST.alltypes;
-- SELECT t17, CAST (t17 AS bit) FROM CST.alltypes;
SELECT t17, CAST (t17 AS boolean) FROM CST.alltypes;
SELECT t17, CAST (t17 AS char(21)) FROM CST.alltypes;
SELECT t17, CAST (t17 AS date) FROM CST.alltypes;
SELECT t17, CAST (t17 AS decimal) FROM CST.alltypes;
SELECT t17, CAST (t17 AS double) FROM CST.alltypes;
SELECT t17, CAST (t17 AS float) FROM CST.alltypes;
SELECT t17, CAST (t17 AS integer) FROM CST.alltypes;
-- SELECT t17, CAST (t17 AS long varbinary) FROM CST.alltypes;
-- SELECT t17, CAST (t17 AS long varchar) FROM CST.alltypes;
SELECT t17, CAST (t17 AS numeric(8,4)) FROM CST.alltypes;
SELECT t17, CAST (t17 AS numeric(6,2)) FROM CST.alltypes;
SELECT t17, CAST (t17 AS real) FROM CST.alltypes;
SELECT t17, CAST (t17 AS smallint) FROM CST.alltypes;
SELECT t17, CAST (t17 AS time) FROM CST.alltypes;
SELECT t17, CAST (t17 AS timestamp) FROM CST.alltypes;
SELECT t17, CAST (t17 AS tinyint) FROM CST.alltypes;
SELECT t17, CAST (t17 AS varbinary(10)) FROM CST.alltypes;
SELECT t17, CAST (t17 AS varchar(23)) FROM CST.alltypes;

-- t18
SELECT t18, CAST (t18 AS bigint) FROM CST.alltypes;
-- SELECT t18, CAST (t18 AS bit) FROM CST.alltypes;
SELECT t18, CAST (t18 AS boolean) FROM CST.alltypes;
SELECT t18, CAST (t18 AS char(21)) FROM CST.alltypes;
SELECT t18, CAST (t18 AS date) FROM CST.alltypes;
SELECT t18, CAST (t18 AS decimal) FROM CST.alltypes;
SELECT t18, CAST (t18 AS double) FROM CST.alltypes;
SELECT t18, CAST (t18 AS float) FROM CST.alltypes;
SELECT t18, CAST (t18 AS integer) FROM CST.alltypes;
-- SELECT t18, CAST (t18 AS long varbinary) FROM CST.alltypes;
-- SELECT t18, CAST (t18 AS long varchar) FROM CST.alltypes;
SELECT t18, CAST (t18 AS numeric(8,4)) FROM CST.alltypes;
SELECT t18, CAST (t18 AS numeric(6,2)) FROM CST.alltypes;
SELECT t18, CAST (t18 AS real) FROM CST.alltypes;
SELECT t18, CAST (t18 AS smallint) FROM CST.alltypes;
SELECT t18, CAST (t18 AS time) FROM CST.alltypes;
SELECT t18, CAST (t18 AS timestamp) FROM CST.alltypes;
SELECT t18, CAST (t18 AS tinyint) FROM CST.alltypes;
SELECT t18, CAST (t18 AS varbinary(10)) FROM CST.alltypes;
SELECT t18, CAST (t18 AS varchar(23)) FROM CST.alltypes;

