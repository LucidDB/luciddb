-- unary math operations in calculator
-- only one is unary negation

-- Test against our standard data set

set schema 's';

SELECT N1, N2, N3, N4, N5, '|', -N2, - (N3 + N4), - (N4 - N5)
FROM TEST_INTEGER_TABLE ORDER BY N1,N2,N3,N4,N5
;

SELECT N1, N2, N3, N4, N5, '|', -N2, - (N3 + N4), - (N4 - N5)
FROM TEST_NUMERIC_TABLE ORDER BY N1,N2,N3,N4,N5
;

-- FRG-209
SELECT N1, N2, N3, N4, N5, '|', -N2, - (N3 + N4), - (N4 - N5)
FROM TEST_REAL_TABLE WHERE N1 <> 1.001 OR N1 IS NULL ORDER BY N1,N2,N3,N4,N5
;
-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000
SELECT N1, N2, N3, N4, N5, '|', -N2, - (N3 + N4), - (N4 - N5)
FROM TEST_REAL_TABLE WHERE N1 = CAST(1.001 as FLOAT);
!set numberFormat default

-- this should work, the result should get promoted to an I8
-- but it does not (bug 15315)
-- LDB-21
SELECT - N1 FROM TEST_INTEGER_TABLE
WHERE N1 = -2147483648
;

-- these are workarounds for the above
-- LDB-21
SELECT - CAST (N1 AS bigint), N1 * -1 FROM TEST_INTEGER_TABLE
WHERE N1 = -2147483648
;

-- End test
