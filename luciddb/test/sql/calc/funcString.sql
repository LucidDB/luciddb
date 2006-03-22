-- test STRING builtin functions

set schema 's';

select s1 || '-' || s2, upper(s1),
lower(s2), trim(s3), substring(s2 from 4 for 3),
position('RI' in s2), position('ri' in s2), char_length(s2)
from boris
;

select position('ing' in s2), position('' in s2), position('ing' in snull)
from boris
;
select snull || snull || 'Boris', 'Boris ' || s2 from boris
;
select lower(upper(lower(upper(s2)))), upper(s2), upper(snull), lower('B')
from boris
;
select substring(snull from 4 for 3), substring(s2 from 1 for 3),
substring(s2 from 1 for 0), substring(s2 from 2 for 1) from boris
;
select char_length(snull), char_length('Boris'), char_length(s2) from boris
;
select initcap('see spot, see Spot run'), initcap(snull) from boris
;
-- select lpad(s2, 10), rpad(s2, 10), lpad(snull, 2), rpad(s2, nnull) from boris
-- ;
DROP TABLE FOO;
create table foo(cola double, str varchar(20));
insert into foo values (1.12345, 'boy');
insert into foo values (21.12345, 'girl');
-- select cola, trunc(cola,4), round(cola,4) from foo;
select substring(str from 2 for 3) from foo;
select substring(str from 2 for -2) from foo;
DROP TABLE FOO;

-- New versions of statements

-- Check upper vs lower

-- This statement should not return any rows
SELECT A.c1, B.c1 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c1 = B.c1
  AND UPPER(A.c1) <> UPPER(LOWER(UPPER(B.c1)));
-- This statement should not return any rows
SELECT A.c3, B.c3 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c3 = B.c3
  AND UPPER(A.c3) <> UPPER(LOWER(UPPER(B.c3)));
-- This statement should not return any rows
SELECT A.s1, B.s1 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s1 = B.s1
  AND UPPER(A.s1) <> UPPER(LOWER(UPPER(B.s1)));
-- This statement should not return any rows
SELECT A.s3, B.s3 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s3 = B.s3
  AND UPPER(A.s3) <> UPPER(LOWER(UPPER(B.s3)));

-- Check substring and trimming on greater sizes than the width of the
-- column
-- This statement should not return any rows
SELECT A.c1, B.c1 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c1 = B.c1
  AND A.c1 <> SUBSTRING(B.c1, 1, 10);
-- This statement should not return any rows
SELECT A.s1, B.s1 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s1 = B.s1
  AND A.s1 <> SUBSTRING(B.s1, 1, 10);
-- This statement should not return any rows
SELECT A.c1, B.c1 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c1 = B.c1
  AND A.c1 <> LEFTN(B.c1, 10);
-- This statement should not return any rows
SELECT A.s1, B.s1 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s1 = B.s1
  AND A.s1 <> LEFTN(B.s1, 10);

-- Note that LTRIM on a given character should not remove
-- any characters that don't match.
-- This statement should not return any rows
SELECT A.c1, B.c1 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c1 = B.c1
  AND A.c1 <> LTRIM(B.c1, '@');
-- This statement should not return any rows
SELECT A.s1, B.s1 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s1 = B.s1
  AND A.s1 <> LTRIM(B.s1, '@');

-- Just check that our substringing and padding, etc. returns
-- the proper number of columns and that CHAR_LENGTH checks it properly.
-- This statement should not return any rows
SELECT A.c3, B.c3 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c3 = B.c3
  AND CHAR_LENGTH(A.c3) <> CHAR_LENGTH(LPAD(RTRIM(RPAD(SUBSTRING(B.c3, 2, 2), 100, '#'), '#'), CHAR_LENGTH(B.c3)));
-- This statement should not return any rows
SELECT A.s3, B.s3 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s3 = B.s3
  AND CHAR_LENGTH(A.s3) <> CHAR_LENGTH(LPAD(RTRIM(RPAD(SUBSTRING(B.s3, 2, 2), 100, '#'), '#'), CHAR_LENGTH(B.s3)));

-- Translate is semi random, but we should check it intelligently.
-- This statement should not return any rows
SELECT A.c1, B.c1 FROM TEST_CHAR_TABLE A, TEST_CHAR_TABLE B
WHERE A.c1 = B.c1
  AND A.c1 <> TRANSLATE(B.c1, 'abcdefgkirkKIRK12345678x', 'abcdefgkirkKIRK12345678x');
-- This statement should not return any rows
SELECT A.s1, B.s1 FROM TEST_VARCHAR_TABLE A, TEST_VARCHAR_TABLE B
WHERE A.s1 = B.s1
  AND A.s1 <> TRANSLATE(B.s1, 'abcdefgkirkKIRK12345678x', 'abcdefgkirkKIRK12345678x');

-- Check INITCAP on combinations of alpha numeric words
values(INITCAP('  123a3b3v3c  gff78f 4 4'));


-- Check the results of this sequence manually.
DROP TABLE FOO;
CREATE TABLE FOO (c CHAR(10));
INSERT INTO FOO VALUES ('1');
INSERT INTO FOO VALUES (' 1');
INSERT INTO FOO VALUES ('  1');
INSERT INTO FOO VALUES (' 1x');
INSERT INTO FOO VALUES (' 1 1');
INSERT INTO FOO VALUES ('1 1');
INSERT INTO FOO VALUES ('11');
INSERT INTO FOO VALUES ('1234567890');
INSERT INTO FOO VALUES ('123.456');
SELECT c, ISINTEGER(c) FROM FOO ORDER BY c;

values (TOKEN('1 2 3', 3));
values (TOKEN('1 2  3', 3));
values (TOKEN('   1 2 3', 4));
values (TOKEN(null, 3));
values (TOKEN('1 2 3', 4));
values (TOKEN('   1  2    3  ', 3));
values (TOKEN('   11  222    33333  ', 4));

-- Check title capping
values (TITLE('This is a title of a book'));
values (TITLE('This IS a tiTLe of a book'));
values (TITLE('This is a TITLE Of A Book'));
values (TITLE('This is a title of (a) book'));
values (TITLE('This <is> [a] {title}+^#$&@%~~;:.,.,.,'' of a book'));

-- Check concatenation precision
-- TODO: not sure what is System.Columns
DROP TABLE BUG8574;
CREATE TABLE BUG8574
(
    C1 CHAR(20),
    C2 CHAR(10),
    C3 COMPUTED (C1 || C2),
    V1 VARCHAR(20),
    V2 VARCHAR(10),
    V3 COMPUTED (V1 || V2)
    );
SELECT SCHEMA_NAME,
   TABLE_NAME,
   COLUMN_NAME,
   SQL_TYPE,
   SQL_Q1,
   SQL_Q2
FROM SYSTEM.COLUMNS
WHERE TABLE_NAME = 'BUG8574'
ORDER BY COLUMN_NAME;

-- Check precision of to_char
CREATE TABLE bug9210
   (
    i1 TINYINT,
    i2 SMALLINT,
    i3 INTEGER,
    i4 BIGINT,
    i5 DECIMAL(15, 4),
    i6 DECIMAL(5,3),
    i7 DECIMAL(5,0)
    );
CREATE OR REPLACE VIEW
   bug9210_view
   (c1, c2, c3, c4, c5, c6, c7)
AS
   SELECT
      to_char(i1),
      to_char(i2),
      to_char(i3),
      to_char(i4),
      to_char(i5),
      to_char(i6),
      to_char(i7)
   FROM bug9210;
SELECT table_name, column_name, sql_type, sql_Q1, sql_Q2
FROM system.COLUMNS
WHERE table_name = 'BUG9210_VIEW'
ORDER BY *;


-- End test
