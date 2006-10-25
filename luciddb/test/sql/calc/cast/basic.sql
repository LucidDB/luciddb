------------------------------------------------------------------------------
-----
--  Tests for basic CAST functionality
-----
------------------------------------------------------------------------------


-- t1 bigint
SELECT t1, CAST (t1 AS bigint) FROM CST.alltypes;
-- SELECT t1, CAST (t1 AS bit) FROM CST.alltypes;
SELECT t1, CAST (t1 AS boolean) FROM CST.alltypes;
SELECT t1, CAST (t1 AS char(21)) FROM CST.alltypes;
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
SELECT t1, CAST (t1 AS tinyint) FROM CST.alltypes;
SELECT t1, CAST (t1 AS varbinary(10)) FROM CST.alltypes;
SELECT t1, CAST (t1 AS varchar(23)) FROM CST.alltypes;

-- t2 boolean
SELECT t2, CAST (t2 AS bigint) FROM CST.alltypes;
-- SELECT t2, CAST (t2 AS bit) FROM CST.alltypes;
SELECT t2, CAST (t2 AS boolean) FROM CST.alltypes;
SELECT t2, CAST (t2 AS char(21)) FROM CST.alltypes;
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
SELECT t2, CAST (t2 AS tinyint) FROM CST.alltypes;
SELECT t2, CAST (t2 AS varbinary(10)) FROM CST.alltypes;
SELECT t2, CAST (t2 AS varchar(23)) FROM CST.alltypes;

-- t3 char(21)
SELECT t3, CAST (t3 AS bigint) FROM CST.alltypes;
-- SELECT t3, CAST (t3 AS bit) FROM CST.alltypes;
SELECT t3, CAST (t3 AS boolean) FROM CST.alltypes;
SELECT t3, CAST (t3 AS char(21)) FROM CST.alltypes;
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
SELECT t3, CAST (t3 AS tinyint) FROM CST.alltypes;
SELECT t3, CAST (t3 AS varbinary(10)) FROM CST.alltypes;
SELECT t3, CAST (t3 AS varchar(23)) FROM CST.alltypes;

-- t5 decimal
SELECT t5, CAST (t5 AS bigint) FROM CST.alltypes;
-- SELECT t5, CAST (t5 AS bit) FROM CST.alltypes;
SELECT t5, CAST (t5 AS boolean) FROM CST.alltypes;
SELECT t5, CAST (t5 AS char(21)) FROM CST.alltypes;
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
SELECT t5, CAST (t5 AS tinyint) FROM CST.alltypes;
SELECT t5, CAST (t5 AS varbinary(10)) FROM CST.alltypes;
SELECT t5, CAST (t5 AS varchar(23)) FROM CST.alltypes;

-- t6 double
SELECT t6, CAST (t6 AS bigint) FROM CST.alltypes;
-- SELECT t6, CAST (t6 AS bit) FROM CST.alltypes;
SELECT t6, CAST (t6 AS boolean) FROM CST.alltypes;
SELECT t6, CAST (t6 AS char(21)) FROM CST.alltypes;
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
SELECT t6, CAST (t6 AS tinyint) FROM CST.alltypes;
SELECT t6, CAST (t6 AS varbinary(10)) FROM CST.alltypes;
SELECT t6, CAST (t6 AS varchar(23)) FROM CST.alltypes;

-- t7 float
SELECT t7, CAST (t7 AS bigint) FROM CST.alltypes;
-- SELECT t7, CAST (t7 AS bit) FROM CST.alltypes;
SELECT t7, CAST (t7 AS boolean) FROM CST.alltypes;
SELECT t7, CAST (t7 AS char(21)) FROM CST.alltypes;
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
SELECT t7, CAST (t7 AS tinyint) FROM CST.alltypes;
SELECT t7, CAST (t7 AS varbinary(10)) FROM CST.alltypes;
SELECT t7, CAST (t7 AS varchar(23)) FROM CST.alltypes;

-- t8 integer
SELECT t8, CAST (t8 AS bigint) FROM CST.alltypes;
-- SELECT t8, CAST (t8 AS bit) FROM CST.alltypes;
SELECT t8, CAST (t8 AS boolean) FROM CST.alltypes;
SELECT t8, CAST (t8 AS char(21)) FROM CST.alltypes;
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
SELECT t8, CAST (t8 AS tinyint) FROM CST.alltypes;
SELECT t8, CAST (t8 AS varbinary(10)) FROM CST.alltypes;
SELECT t8, CAST (t8 AS varchar(23)) FROM CST.alltypes;

-- t9 long varbinary
-- SELECT t9, CAST (t9 AS bigint) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS bit) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS boolean) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS char(21)) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS decimal) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS double) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS float) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS integer) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS long varbinary) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS long varchar) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS numeric(8,4)) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS numeric(6,2)) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS real) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS smallint) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS tinyint) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS varbinary(10)) FROM CST.alltypes;
-- SELECT t9, CAST (t9 AS varchar(23)) FROM CST.alltypes;

-- t10 long varchar
-- SELECT t10, CAST (t10 AS bigint) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS bit) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS boolean) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS char(21)) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS decimal) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS double) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS float) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS integer) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS long varbinary) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS long varchar) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS numeric(8,4)) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS numeric(6,2)) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS real) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS smallint) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS tinyint) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS varbinary(10)) FROM CST.alltypes;
-- SELECT t10, CAST (t10 AS varchar(23)) FROM CST.alltypes;

-- t11_1 numeric(8,4)
SELECT t11_1, CAST (t11_1 AS bigint) FROM CST.alltypes;
-- SELECT t11_1, CAST (t11_1 AS bit) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS boolean) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS char(21)) FROM CST.alltypes;
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
SELECT t11_1, CAST (t11_1 AS tinyint) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS varbinary(10)) FROM CST.alltypes;
SELECT t11_1, CAST (t11_1 AS varchar(23)) FROM CST.alltypes;

-- t11_2 numeric(6,2)
SELECT t11_2, CAST (t11_2 AS bigint) FROM CST.alltypes;
-- SELECT t11_2, CAST (t11_2 AS bit) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS boolean) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS char(21)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS decimal) FROM CST.alltypes;
-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000
SELECT t11_2, CAST (t11_2 AS double) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS float) FROM CST.alltypes;
!set numberFormat default
SELECT t11_2, CAST (t11_2 AS integer) FROM CST.alltypes;
-- SELECT t11_2, CAST (t11_2 AS long varbinary) FROM CST.alltypes;
-- SELECT t11_2, CAST (t11_2 AS long varchar) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS numeric(8,4)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS numeric(6,2)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS real) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS smallint) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS tinyint) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS varbinary(10)) FROM CST.alltypes;
SELECT t11_2, CAST (t11_2 AS varchar(23)) FROM CST.alltypes;

-- t12 real
SELECT t12, CAST (t12 AS bigint) FROM CST.alltypes;
-- SELECT t12, CAST (t12 AS bit) FROM CST.alltypes;
SELECT t12, CAST (t12 AS boolean) FROM CST.alltypes;
SELECT t12, CAST (t12 AS char(21)) FROM CST.alltypes;
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
SELECT t12, CAST (t12 AS tinyint) FROM CST.alltypes;
SELECT t12, CAST (t12 AS varbinary(10)) FROM CST.alltypes;
SELECT t12, CAST (t12 AS varchar(23)) FROM CST.alltypes;

-- t13 smallint
SELECT t13, CAST (t13 AS bigint) FROM CST.alltypes;
-- SELECT t13, CAST (t13 AS bit) FROM CST.alltypes;
SELECT t13, CAST (t13 AS boolean) FROM CST.alltypes;
SELECT t13, CAST (t13 AS char(21)) FROM CST.alltypes;
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
SELECT t13, CAST (t13 AS tinyint) FROM CST.alltypes;
SELECT t13, CAST (t13 AS varbinary(10)) FROM CST.alltypes;
SELECT t13, CAST (t13 AS varchar(23)) FROM CST.alltypes;

-- t16 tinyint
SELECT t16, CAST (t16 AS bigint) FROM CST.alltypes;
-- SELECT t16, CAST (t16 AS bit) FROM CST.alltypes;
SELECT t16, CAST (t16 AS boolean) FROM CST.alltypes;
SELECT t16, CAST (t16 AS char(21)) FROM CST.alltypes;
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
SELECT t16, CAST (t16 AS tinyint) FROM CST.alltypes;
SELECT t16, CAST (t16 AS varbinary(10)) FROM CST.alltypes;
SELECT t16, CAST (t16 AS varchar(23)) FROM CST.alltypes;

-- t17 varbinary(10)
SELECT t17, CAST (t17 AS bigint) FROM CST.alltypes;
-- SELECT t17, CAST (t17 AS bit) FROM CST.alltypes;
SELECT t17, CAST (t17 AS boolean) FROM CST.alltypes;
SELECT t17, CAST (t17 AS char(21)) FROM CST.alltypes;
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
SELECT t17, CAST (t17 AS tinyint) FROM CST.alltypes;
SELECT t17, CAST (t17 AS varbinary(10)) FROM CST.alltypes;
SELECT t17, CAST (t17 AS varchar(23)) FROM CST.alltypes;

-- t18 varchar(23)
SELECT t18, CAST (t18 AS bigint) FROM CST.alltypes;
-- SELECT t18, CAST (t18 AS bit) FROM CST.alltypes;
SELECT t18, CAST (t18 AS boolean) FROM CST.alltypes;
SELECT t18, CAST (t18 AS char(21)) FROM CST.alltypes;
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
SELECT t18, CAST (t18 AS tinyint) FROM CST.alltypes;
SELECT t18, CAST (t18 AS varbinary(10)) FROM CST.alltypes;
SELECT t18, CAST (t18 AS varchar(23)) FROM CST.alltypes;

