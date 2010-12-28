--
-- calc/inList.sql
-- $Id$
--
-- Test IN LIST and NOT IN LIST function/constructs
--
-- Author: Kirk
-- Date: 19 March, 1999
--

--{{{ Bug 3881 (0.0 vs -0.0 for inList)

-- Abstract: select stmt produces incorrect results


CREATE SCHEMA BUG3881;
create table BUG3881.qrysrcdec (d1 decimal ( 2, 0),
                                d2 decimal ( 5, 2), 
                                d3 decimal (12, 7),
                                d4 decimal (15, 6));
INSERT INTO BUG3881.qrysrcdec VALUES ( 1, 123.45, 123.456789, 123456789.123456 );
INSERT INTO BUG3881.qrysrcdec VALUES ( 2, 22.5, 374.2597, 12.75 );
INSERT INTO BUG3881.qrysrcdec VALUES ( 3, 132.75, 2099.999, -333.333 );
INSERT INTO BUG3881.qrysrcdec VALUES ( 4, 73.734, -21367.45666, 888.98989898 );
--INSERT INTO BUG3881.qrysrcdec VALUES ( -12, 153.153, -1010.9999999, -115.000001);
INSERT INTO BUG3881.qrysrcdec VALUES ( 5, 20.00 , 300.00, 5000000.000000 );
INSERT INTO BUG3881.qrysrcdec VALUES ( 6, null, null, null);
INSERT INTO BUG3881.qrysrcdec VALUES ( 7, 10.00 , 300.00, 50000.000000 );
INSERT INTO BUG3881.qrysrcdec VALUES ( 6, -3.14, null, 2.718281);
INSERT INTO BUG3881.qrysrcdec VALUES ( 7, 10.00 , 300.00, 50000.000000 );
INSERT INTO BUG3881.qrysrcdec VALUES ( null, 0, 0, -1.1111);
INSERT INTO BUG3881.qrysrcdec VALUES ( null, null, null, null);
INSERT INTO BUG3881.qrysrcdec VALUES ( null, null, null, null);
SELECT * FROM BUG3881.qrysrcdec WHERE -ABS(LN(d1)) = (0.0000)  ;
SELECT * FROM BUG3881.qrysrcdec WHERE ABS(LN(d1)) IN (0.0000)  ;
SELECT * FROM BUG3881.qrysrcdec WHERE -ABS(LN(d1)) IN (0.0000)  ;

drop table BUG3881.qrysrcdec;
drop schema BUG3881;

--}}}

--{{{ Bug 3893 (IN list with strings)

-- Abstract: inconsistent evalutation of IN with character data
-- Strings with trailing spaces should be treated specially in SQL
--

CREATE SCHEMA BUG3893;
CREATE TABLE BUG3893.T(c char(10));
INSERT INTO BUG3893.T VALUES ('abc');

-- force usage of a filter
-- the following query should return 1 row
--SELECT * FROM (SELECT * FROM BUG3893.T ORDER BY 1) WHERE c IN ('abc');
SELECT * FROM (SELECT * FROM BUG3893.T) WHERE c IN ('abc');

-- the following query should return zero rows
SELECT * FROM BUG3893.T WHERE NOT (c IN ('abc'));
-- this should also return no rows
-- SELECT * FROM BUG3893.T WHERE c NOT IN ('abc');
DROP TABLE BUG3893.T;
DROP SCHEMA BUG3893;

--}}}

--{{{ Bug 3935 (null with IN lists)

-- Abstract: select stmt with "IN" in where clause produces incorrect results
--

CREATE SCHEMA SBUG3935;
SET SCHEMA 'SBUG3935';
create table bug3935 (ti        tinyint, 
                      si        smallint,
                      ii        integer,
                      bi        bigint);
insert into bug3935 values ( 1, 10, 10000, 1000000000 );
insert into bug3935 values ( 2, 27, 80974, 23 );
insert into bug3935 values ( 3, 11, 2500000, 123456789 );
insert into bug3935 values ( 2, -27, -80974, -233 );
insert into bug3935 values ( 8, 6, 18, 54);
insert into bug3935 values ( null, null, null, null);
insert into bug3935 values ( 4, 999, 99999, 999999999);
insert into bug3935 values ( null, -238, -456778, null);
insert into bug3935 values ( 5, 67, 893256, 2000000000 );
insert into bug3935 values ( 4, 0, null, 0 );
insert into bug3935 values ( 6, 187, 76542, 1234567890 );
insert into bug3935 values ( 7, null, 22, 37);
insert into bug3935 values ( 8, 6, 18, 54);
insert into bug3935 values (null, null, null, null);
SELECT   *   FROM  bug3935  WHERE   ti*si IN (-54, 48, -1, 54, 333, 0);
SELECT * FROM bug3935 WHERE NOT ((ii*100) IN (-1800, 7654200, 1801, -45677800, 45677800)) ;
SELECT * FROM bug3935 WHERE (ii*100) NOT IN (-1800, 7654200, 1801, -45677800, 45677800) ;

-- tests with NULL in IN List (TODO: FRG-224)
select * from bug3935 where ti IN (null) order by 1,2,3,4;
select * from bug3935 where si IN (null, 6) order by 1,2,3,4;
select * from bug3935 where ti NOT IN (null) order by 1,2,3,4;
select * from bug3935 where si NOT IN (null, 6) order by 1,2,3,4;

DROP TABLE BUG3935;
DROP SCHEMA SBUG3935;

--}}}

--{{{ Bug 7819 (to_Number and an inList together)

-- Based on bug 7819 and simplified from there
-- when using to_Number() and an inList in the
-- same expression, the server crashed.
--
--
-- to_number is not supported; use cast instead
-- NOT IN not worked
CREATE SCHEMA SB7819;
SET SCHEMA 'SB7819';
 
CREATE TABLE bug7819
   ( id INTEGER,
     non_id VARCHAR(256)
     );
INSERT INTO bug7819 VALUES (1, '12345');
INSERT INTO bug7819 VALUES (2, '23456');
SELECT * FROM bug7819
--WHERE to_number(non_id) IN (12345);
WHERE CAST(non_id AS INTEGER) IN (12345);
SELECT * FROM bug7819
--WHERE to_number(non_id) NOT IN (12345);
WHERE NOT (CAST(non_id AS INTEGER) IN (12345));

--}}}

--{{{ Bug 13201 (empty strings don't work with "IN" list)

-- this should return false
values ('' in ('abc'));
-- these should return true
values ('' in ('','abc'));
values ('' in (''));

DROP TABLE bug7819;
DROP SCHEMA sb7819;

--}}}

--
-- End test calc;inList.sql
