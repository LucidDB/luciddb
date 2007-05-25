-- test binary math operations in calculator
-- Was calc6.sql
!set shownestederrs true
set schema 's';

select 10 + 100 + 1000 + n1 + n2 + n3 from boris
;
select 1000 - 100 - 10 - n1 - n2 - n3 from boris
;
select 1 * 10 * 100 * n1 * n2 * n3 from boris
;
select 10000 / 10 / n2 from boris
;
select 1000 + 1000 - 1000 * n2 / n2, ((n2 - n1 - n3) * 10) / n3 from boris
;
select (10.25 + 4.75) - 2.54362 + 2.54362, 8.4235 * 10000 from boris
;
select (102345 / 10.2345) / .125 from boris
;
select nnull + 2 / 8, 1000 * nnull, 10 - nnull, 100 / nnull from boris
;
-- SELECT n, n+n, n-n, n/n, n*n FROM System.ten order by 1;

-- Test against our standard data set

SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS SUMMY
FROM TEST_INTEGER_TABLE ORDER BY n1,n2,n3,n4,n5;
-- FRG-45
SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS SUMMY
FROM TEST_NUMERIC_TABLE ORDER BY n1,n2,n3,n4,n5;

-- FRG-209
SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS SUMMY
FROM TEST_REAL_TABLE WHERE N1 <> 1.001 OR N1 IS NULL ORDER BY n1,n2,n3,n4,n5;
-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000
SELECT  N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS SUMMY
FROM TEST_REAL_TABLE where N1 = CAST(1.001 as FLOAT);
!set numberFormat default

SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS DIFFY
FROM TEST_INTEGER_TABLE ORDER BY n1,n2,n3,n4,n5;
SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS DIFFY
FROM TEST_NUMERIC_TABLE ORDER BY n1,n2,n3,n4,n5;

-- FRG-209
SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS DIFFY
FROM TEST_REAL_TABLE WHERE N1 <> 1.001 OR N1 IS NULL ORDER BY n1,n2,n3,n4,n5;
-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000
SELECT N1, N2, N3, N4, N5, (N5 - N4 - N3 - N2 - N1) AS DIFFY
FROM TEST_REAL_TABLE where N1 = CAST(1.001 as FLOAT);
!set numberFormat default

-- Test association rules and precedence
SELECT N1, N2, N3,
((N3 - N2) - N1) AS DIFF1, (N3 - N2 - N1) AS DIFF2,
(N3 - (N2 - N1)) AS DIFF3
FROM TEST_INTEGER_TABLE ORDER BY N1, N2, N3;
SELECT N1, N2, N3,
((N3 - N2) - N1) AS DIFF1, (N3 - N2 - N1) AS DIFF2,
(N3 - (N2 - N1)) AS DIFF3
FROM TEST_NUMERIC_TABLE ORDER BY N1, N2, N3;

-- FRG-209
SELECT N1, N2, N3,
((N3 - N2) - N1) AS DIFF1, (N3 - N2 - N1) AS DIFF2,
(N3 - (N2 - N1)) AS DIFF3
FROM TEST_REAL_TABLE WHERE N1 <> 1.001 OR N1 IS NULL ORDER BY N1, N2, N3;
-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000
SELECT N1, N2, N3,
((N3 - N2) - N1) AS DIFF1, (N3 - N2 - N1) AS DIFF2,
(N3 - (N2 - N1)) AS DIFF3
FROM TEST_REAL_TABLE WHERE N1 = CAST(1.001 as FLOAT);
!set numberFormat default

SELECT N1, N2, N3,
((N3 * N2) - N1) AS DIFF1, (N3 * N2 - N1) AS DIFF2,
(N3 * (N2 - N1)) AS DIFF3
FROM TEST_INTEGER_TABLE ORDER BY N1, N2, N3;
SELECT N1, N2, N3,
((N3 * N2) - N1) AS DIFF1, (N3 * N2 - N1) AS DIFF2,
(N3 * (N2 - N1)) AS DIFF3
FROM TEST_NUMERIC_TABLE ORDER BY N1, N2, N3;
SELECT N1, N2, N3,
((N3 * N2) - N1) AS DIFF1, (N3 * N2 - N1) AS DIFF2,
(N3 * (N2 - N1)) AS DIFF3
FROM TEST_REAL_TABLE ORDER BY N1, N2, N3;
SELECT N1, N2, N3,
((N3 + N2) * N1) AS MUL1, (N3 + N2 * N1) AS MUL2,
(N3 + (N2 * N1)) AS MUL3
FROM TEST_INTEGER_TABLE ORDER BY N1, N2, N3;
SELECT N1, N2, N3,
((N3 + N2) * N1) AS MUL1, (N3 + N2 * N1) AS MUL2,
(N3 + (N2 * N1)) AS MUL3
FROM TEST_NUMERIC_TABLE ORDER BY N1, N2, N3;

-- FRG-209
SELECT N1, N2, N3,
((N3 + N2) * N1) AS MUL1, (N3 + N2 * N1) AS MUL2,
(N3 + (N2 * N1)) AS MUL3
FROM TEST_REAL_TABLE WHERE N1 <> 1.001 OR N1 IS NULL ORDER BY N1, N2, N3;
-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000
SELECT N1, N2, N3,
((N3 + N2) * N1) AS MUL1, (N3 + N2 * N1) AS MUL2,
(N3 + (N2 * N1)) AS MUL3
FROM TEST_REAL_TABLE WHERE N1 = CAST(1.001 as FLOAT);
!set numberFormat default

-- Tests for multiplication when either precision of scale of the
-- result is not equal to the sum of those of operands

SELECT N1, N2, N3, N4, N5,
N1 * N2, N1 * N3, N1 * N5, N3 * N5, N5 * N3, N5 * N2 * N3
FROM TEST_NUMERIC_TABLE ORDER BY N1, N2, N3
;

-- End test
