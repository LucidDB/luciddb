--
-- qin.sql - test in lists
--

-- indexed
SELECT KSEQ FROM BENCH100 WHERE K4 IN (1,2);

-- non indexed
SELECT KSEQ FROM BENCH100 WHERE K25 IN (1,2,5,7); 

-- both
SELECT KSEQ FROM BENCH100 WHERE K4 IN (3,4) or K10 IN (9,4,7);              

-- out of range values
SELECT KSEQ FROM BENCH100 WHERE K4 IN (1,2,100,-25);              
SELECT KSEQ FROM BENCH100 WHERE K25 IN (1,2,100,-25);              

-- duplicate values
SELECT KSEQ FROM BENCH100 WHERE K4 IN (1,2,1,2,2,3);              
SELECT KSEQ FROM BENCH100 WHERE K25 IN (1,2,3,3,25,1);              
