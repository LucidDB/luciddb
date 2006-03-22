-- see bug description for more details

CREATE SCHEMA BUG11810
;

CREATE TABLE BUG11810.T1(COL1 decimal(19,8), COL2 bigint)
;

INSERT INTO BUG11810.T1 VALUES(4.00000, 2)
;

-- the result of division used to be incorrect
SELECT COL1 / COL2 FROM BUG11810.T1
;
