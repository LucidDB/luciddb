-- Simple SELF JOIN tests
-- Cover the following join types
-- a) EQUIV JOIN
-- b) NON EQUIVE JOIN
-- c) RANGE JOIN

set schema 's';

-- EQUALS JOIN

SELECT B1.KSEQ as B1KSEQ, B1.K2 as B1K2, B2.K2 as B2K2 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 = B2.K2 AND B1.KSEQ < 20
ORDER BY B1KSEQ, B1K2, B2K2
;

SELECT B1.KSEQ, B1.K2, B2.K4 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 = B2.K4 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K4
;

SELECT B1.KSEQ, B1.K2, B2.K5 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 = B2.K5 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K5 
;

SELECT B1.KSEQ, B1.K2, B2.K10 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 = B2.K10 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K10
;

-- NON-EQUALS JOIN

SELECT B1.KSEQ as B1KSEQ, B1.K2 as B1K2, B2.K2 as B2K2 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 <> B2.K2 AND B1.KSEQ < 20
ORDER BY B1KSEQ, B1K2, B2K2
;

SELECT B1.KSEQ, B1.K2, B2.K4 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 <> B2.K4 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K4 
;

SELECT B1.KSEQ, B1.K2, B2.K5 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 = B2.K5 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K5 
;

SELECT B1.KSEQ as B1KSEQ, B1.K2 as B1K2, B2.K2 as B2K2
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 = B2.K10 AND B1.KSEQ < 20
ORDER BY B1KSEQ, B1K2, B2K2 
;

-- RANGE JOIN

SELECT B1.KSEQ as B1KSEQ, B1.K2 as B1K2, B2.K2 as B2K2 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 > B2.K2 AND B1.KSEQ < 20
ORDER BY B1KSEQ, B1K2, B2K2
;

SELECT B1.KSEQ, B1.K2, B2.K4 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 > B2.K4 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K4 
;

SELECT B1.KSEQ, B1.K2, B2.K5 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 > B2.K5 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K5 
;

SELECT B1.KSEQ, B1.K2, B2.K10 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 > B2.K10 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K10 
;

SELECT B1.KSEQ, B1.K2 as B1K2, B2.K2 as B2K2 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 < B2.K2 AND B1.KSEQ < 20
ORDER BY KSEQ, B1K2, B2K2 
;

SELECT B1.KSEQ, B1.K2, B2.K4 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 < B2.K4 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K4 
;

SELECT B1.KSEQ, B1.K2, B2.K5 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 < B2.K5 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K5 
;

SELECT B1.KSEQ, B1.K2, B2.K10 FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 < B2.K10 AND B1.KSEQ < 20
ORDER BY KSEQ, K2, K10 
;

SELECT B1.KSEQ as B1KSEQ, B1.K2 as B1K2, B2.K2 as B2K2, B2.K4 as B2K4 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 BETWEEN B2.K2 AND B2.K4 AND B1.KSEQ < 20
ORDER BY B1KSEQ, B1K2, B2K2, B2K4
;

SELECT B1.KSEQ as B1KSEQ, B1.K2 as B1K2, B2.K2 as B2K2, B2.K5 as B2K5 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 BETWEEN B2.K2 AND B2.K5 AND B1.KSEQ < 20
ORDER BY B1KSEQ, B1K2, B2K2, B2K5
;

SELECT B1.KSEQ, B1.K2 as B1K2, B2.K2 as B2K2, B2.K10 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 BETWEEN B2.K2 AND B2.K10 AND B1.KSEQ < 20
ORDER BY KSEQ, B1K2, B2K2, K10
;

SELECT B1.KSEQ, B1.K2 as B1K2, B2.K2 as B2K2, B2.K25 
FROM BENCH100 B1, BENCH100 B2 
WHERE B1.K2 BETWEEN B2.K2 AND B2.K25 AND B1.KSEQ < 20
ORDER BY KSEQ, B1K2, B2K2, K25
;

