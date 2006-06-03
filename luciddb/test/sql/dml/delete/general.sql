------------------------------------------------------------------------------
-----
--  General tests for DELETE
-----
------------------------------------------------------------------------------

--{{{ DELETE with bind variables in the WHERE clause

CREATE TABLE DT.DB1 ( x int, s varchar(20))
;

-- populate the table
-- EXECUTE SCRIPT as
-- int i = 0;
-- for ( i = 0 ; i <= 9 ; i++ )
-- {
--      INSERT INTO DT.DB1 VALUES (?i, 'hello world');
-- }
-- ;

INSERT INTO DT.DB1 VALUES (0, 'hello world');
INSERT INTO DT.DB1 VALUES (1, 'hello world');
INSERT INTO DT.DB1 VALUES (2, 'hello world');
INSERT INTO DT.DB1 VALUES (3, 'hello world');
INSERT INTO DT.DB1 VALUES (4, 'hello world');
INSERT INTO DT.DB1 VALUES (5, 'hello world');
INSERT INTO DT.DB1 VALUES (6, 'hello world');
INSERT INTO DT.DB1 VALUES (7, 'hello world');
INSERT INTO DT.DB1 VALUES (8, 'hello world');
INSERT INTO DT.DB1 VALUES (9, 'hello world');

SELECT * FROM DT.DB1
ORDER by 1;

-- DELETE all even numbers from the table
-- EXECUTE SCRIPT as
-- int i = 0;
-- for ( i = 0 ; i <= 9 ; i += 2 )
-- {
--      DELETE FROM DT.DB1 WHERE x = ?i;
-- }
-- ;

DELETE FROM DT.DB1 WHERE x = 0;
DELETE FROM DT.DB1 WHERE x = 2;
DELETE FROM DT.DB1 WHERE x = 4;
DELETE FROM DT.DB1 WHERE x = 6;
DELETE FROM DT.DB1 WHERE x = 8;

SELECT * FROM DT.DB1
ORDER by 1;

-- DELETE all ood numbers from the table using a more complex WHERE expression
-- EXECUTE SCRIPT as
-- int i = 0;
-- int j = 3;
-- for ( i = 1 ; i <= 9 ; i += 2 )
-- {
--      DELETE FROM DT.DB1 WHERE x + 2 * ?i + ?j + 1 - ?j - 3 * ?i = 1;
-- }
-- ;

DELETE FROM DT.DB1 WHERE x + 2 * 1 + 3 + 1 - 3 - 3 * 1 = 1;
DELETE FROM DT.DB1 WHERE x + 2 * 3 + 3 + 1 - 3 - 3 * 3 = 1;
DELETE FROM DT.DB1 WHERE x + 2 * 5 + 3 + 1 - 3 - 3 * 5 = 1;
DELETE FROM DT.DB1 WHERE x + 2 * 7 + 3 + 1 - 3 - 3 * 7 = 1;
DELETE FROM DT.DB1 WHERE x + 2 * 9 + 3 + 1 - 3 - 3 * 9 = 1;

SELECT * FROM DT.DB1
ORDER by 1;


--}}}
--{{{ DELETE from a tables that are involved in FK relationship

-- CREATE TABLE DT.DIMENSION1 (x integer, y computed ( x + 1), f float,
--                                                      pk1 integer, pk2 integer, un integer unique,
--                                                      primary key (pk1, pk2))
-- ;

-- CREATE TABLE DT.FACT1 (x integer, y integer, 
--                                        foreign key (x, y) references DT.DIMENSION1(pk1, pk2))
-- ;

-- CREATE TABLE DT.FACT2 (x integer references DT.DIMENSION1(un))
-- ;

-- -- DELETE from FACT table (requires NO special validation)
-- DELETE FROM DT.FACT1
-- ;

-- DELETE FROM DT.FACT2
-- ;

-- -- DELETE from DIMENSION table (requires special validation)
-- DELETE FROM DT.DIMENSION1
-- ;

--}}}
--{{{ DELETE with bind variables and FK relationships

-- we will reuse tables from above
 
--{{{ populate the tables

-- EXECUTE SCRIPT as
-- int i = 0;
-- int j = 0;
-- for ( i = 0 ; i <= 9 ; i++ )
-- {
--      for ( j = 0 ; j <= 1 ; j++ )
--      {
--              INSERT INTO DT.DIMENSION1 VALUES (0, 0, ?i, ?j, ?i * 2 + ?j);
--      }
-- }
-- ;

-- -- x, y pairs where x is even
-- EXECUTE SCRIPT as
-- int i = 0;
-- int j = 0;
-- for ( i = 0 ; i <= 9 ; i += 2 )
-- {
--      for ( j = 0 ; j <= 1 ; j++ )
--      {
--              INSERT INTO DT.FACT1 VALUES (?i, ?j);
--      }
-- }
-- ;

-- -- 5 to 14
-- EXECUTE SCRIPT as
-- int i = 0;
-- for ( i = 5 ; i <= 14 ; i++ )
-- {
--              INSERT INTO DT.FACT2 VALUES (?i);
-- }
-- ;

-- SELECT * FROM DT.DIMENSION1
-- ;

-- SELECT * FROM DT.FACT1
-- ;

-- SELECT * FROM DT.FACT2
-- ;

-- --}}}

-- -- DELETE from FACT table (requires NO special validation)
-- DELETE FROM DT.FACT2 WHERE x > 9
-- ;

-- SELECT * FROM DT.FACT2
-- ;

-- -- DELETE from DIMENSION table (requires special validation)
-- -- this one should succeed
-- DELETE  FROM DT.DIMENSION1 WHERE  un > 9
--              AND pk1 - CAST (pk1 ; 2 as integer) * 2  != 0
-- ;

-- SELECT * FROM DT.DIMENSION1
-- ;

-- -- this one should fail
-- DELETE FROM DT.DIMENSION1 WHERE un > 4
--              AND pk1 - CAST (pk1 / 2 as integer) * 2  != 0           
-- ;

-- SELECT * FROM DT.DIMENSION1
-- ;

-- -- all of these should succeed
-- EXECUTE SCRIPT as
-- int i = 0;
-- for ( i = 1 ; i <= 9 ; i += 2 )
-- {
--     int j = 0;
-- //   SELECT * FROM DT.DIMENSION1 WHERE un < 5 AND pk1 = ?i AND ?j = 0;
--      DELETE FROM DT.DIMENSION1 WHERE un < 5 AND pk1 = ?i AND ?j = 0;
-- }
-- ;

-- SELECT * FROM DT.DIMENSION1
-- ;

-- -- this should have nothing to delete (so it should succeed)
-- DELETE FROM DT.DIMENSION1 WHERE un < 5 AND pk1 = 1
-- ;

-- SELECT * FROM DT.DIMENSION1
-- ;

-- --}}}
