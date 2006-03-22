-- test NULL situations
-- Was calc1.sql

set schema 's';

insert into boris (n1,n2,n3,s1,s2,s3) values(3, 4.5, 6, 'string1', 'string2', '   string3  ')
;
select
substring('julian', 1, 3),
substring('tai', 1, 0),
'V' || substring('meeten', 1, 0) || 'W',
'X' || snull || 'Y',
char_length(snull),
substring(snull, 1),
substring(snull, 3),
substring(snull, 2, 10),
trim(snull),
-- rpad(snull, 5, 'a'),
-- lpad(snull, 5, 'b'),
upper(snull),
-- nvl(snull, s1),
case when (snull like '%') then 1 else 0 end,
case when (snull like '%GOP') then 1 else 0 end,
case when (snull like '') then 1 else 0 end,
case when ('bunny' like '') then 1 else 0 end,
nnull + 1,
nnull + 2.5,
-- nvl(nnull, n1),
1 / nnull
from boris order by 1
;
-- Okay, that old query was just lame. Rewrite it into several statements,
-- pulling from multiple tables.

SELECT c1, SUBSTRING(c1, 1, 0) FROM TEST_CHAR_TABLE ORDER BY c1;
SELECT c2, SUBSTRING(c2, 1, 0) FROM TEST_CHAR_TABLE ORDER BY c2;
SELECT c3, SUBSTRING(c3, 1, 0) FROM TEST_CHAR_TABLE ORDER BY c3;
SELECT c4, SUBSTRING(c4, 1, 0) FROM TEST_CHAR_TABLE ORDER BY c4;
SELECT c5, SUBSTRING(c5, 1, 0) FROM TEST_CHAR_TABLE ORDER BY c5;
SELECT s1, SUBSTRING(s1, 1, 0) FROM TEST_VARCHAR_TABLE ORDER BY s1;
SELECT s2, SUBSTRING(s2, 1, 0) FROM TEST_VARCHAR_TABLE ORDER BY s2;
SELECT s3, SUBSTRING(s3, 1, 0) FROM TEST_VARCHAR_TABLE ORDER BY s3;
SELECT s4, SUBSTRING(s4, 1, 0) FROM TEST_VARCHAR_TABLE ORDER BY s4;
SELECT s5, SUBSTRING(s5, 1, 0) FROM TEST_VARCHAR_TABLE ORDER BY s5;
SELECT c1, c1 || SUBSTRING(c1, 1, 0) || c1 FROM TEST_CHAR_TABLE ORDER BY c1;
SELECT c2, c2 || SUBSTRING(c2, 1, 0) || c2 FROM TEST_CHAR_TABLE ORDER BY c2;
SELECT c3, c3 || SUBSTRING(c3, 1, 0) || c3 FROM TEST_CHAR_TABLE ORDER BY c3;
SELECT s1, s1 || SUBSTRING(s1, 1, 0) || s1 FROM TEST_VARCHAR_TABLE ORDER BY s1;
SELECT s2, s2 || SUBSTRING(s2, 1, 0) || s2 FROM TEST_VARCHAR_TABLE ORDER BY s2;
SELECT s3, s3 || SUBSTRING(s3, 1, 0) || s3 FROM TEST_VARCHAR_TABLE ORDER BY s3;
-- SELECT n1, NVL(n1, 0) FROM TEST_INTEGER_TABLE ORDER BY *;
-- SELECT n1, NVL(n1, 0) FROM TEST_NUMERIC_TABLE ORDER BY *;
-- SELECT n1, NVL(n1, 0) FROM TEST_REAL_TABLE ORDER BY *;
SELECT n1 + 1 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 + 1 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 + 1 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 + 1.5 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 + 1.5 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 + 1.5 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 * 1 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 * 1 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 * 1 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 * 1.5 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 * 1.5 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 * 1.5 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 * 2 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 * 2 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 * 2 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 * 2.5 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 * 2.5 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 * 2.5 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 / 2 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 / 2 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 / 2 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT n1 / 2.5 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT n1 / 2.5 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT n1 / 2.5 FROM TEST_REAL_TABLE WHERE n1 IS NULL;
SELECT 1 / n1 FROM TEST_INTEGER_TABLE WHERE n1 IS NULL;
SELECT 1 / n1 FROM TEST_NUMERIC_TABLE WHERE n1 IS NULL;
SELECT 1 / n1 FROM TEST_REAL_TABLE WHERE n1 IS NULL;

-- bug 15100 (dealing with types of NVL arguments)
-- select nvl(1234.56, cast(-1 as float)) from onerow;
-- select nvl(cast(null as decimal(18,2)), cast(-1 as float)) from onerow;
-- select nvl(null, cast(-1 as float)) from onerow;
-- select nvl(null, null) from onerow;

-- bug 15606 (another bug re: types of NVL arguments)
create table bug15606(i int);
insert into bug15606 values (null);
-- this select used to output nothing
-- select nvl(i,-99) from  bug15606;

-- bug 10532 (another bug re: types of NVL arguments)
CREATE TABLE bug10532 (c1 VARCHAR(10),c2 VARCHAR(64));
INSERT INTO bug10532 VALUES (NULL,'This is a very long string as you can see');
-- CREATE VIEW view10532 AS SELECT
-- NVL(c1,c2) as Newcol FROM SYSTEM.bug10532;
-- this select used to output nothing
-- SELECT * from view10532;

-- End of test
