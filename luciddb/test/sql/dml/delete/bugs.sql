------------------------------------------------------------------------------
-----
-- Tests covering bug fixes made to DELETE code
-----
------------------------------------------------------------------------------

-- --{{{ Bug 7905
-- 
-- -- UPDATE with where clause would miscalculate the number of bind
-- -- variables and crash

CREATE SCHEMA BUG7905
;

-- setup
CREATE TABLE BUG7905.T1(c1 int,c2 char(20))
;
TRUNCATE TABLE BUG7905.T1
;
INSERT INTO BUG7905.T1 VALUES(1,'one')
;
INSERT INTO BUG7905.T1 VALUES(2,'two')
;

SELECT * FROM BUG7905.T1
;

-- -- this used to crash
-- EXECUTE SCRIPT as
-- int i = 0;
-- int j = 0;
-- for ( i = 1 ; i <= 2 ; i++ )
-- {
-- j = i + 1;
-- UPDATE BUG7905.T1 SET c1=?j WHERE c1=?i;
-- }
-- /
-- 
-- SELECT * FROM BUG7905.T1
-- /
-- 
-- --}}}
-- 
-- --{{{ Bug 9354
-- 
-- -- parser was setting the parse region incorrectly for the right hand side of
-- -- SET clause (it did not include '()')
-- 
-- CREATE SCHEMA BUG9354
-- /
-- 
-- CREATE TABLE BUG9354.X (A int)
-- /
-- 
-- CREATE MACRO BUG9354.M ( ) as 1
-- /
-- 
-- UPDATE BUG9354.X SET a = BUG9354.M ( )
-- /
-- 
-- --}}}
-- 
-- 

-- bug 14556
--alter system set optimizertrace=off;
-- create schema archana;
-- create table archana.dim(di1 int primary key);
-- create table archana.fact1(f1 int references archana.dim(di1));
-- create table archana.fact2(f2 int references archana.dim(di1));
-- create table archana.fact3(f3 int references archana.dim(di1));
-- insert into archana.dim values(1);
-- insert into archana.fact1 values(1);
-- insert into archana.fact2 values(1);
-- insert into archana.fact3 values(1);
-- -- alter system set optimizertrace=on;
-- delete from archana.dim where di1 < 0;
