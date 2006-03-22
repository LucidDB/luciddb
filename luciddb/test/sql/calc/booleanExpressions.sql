-- booleanExpressions.sql
-- Tests boolean expressions, both in filters as well as in select statements.

-- Although IDBA may show that many very very small floats and doubles
-- are equal to 0, they're not, so check that we have the right number
-- coming out.
-- This test should return 0, since none of the values are actually EQUAL
-- to zero.

set schema 's';

SELECT COUNT(*)
FROM TEST_REAL_TABLE
WHERE n1 = 0.0;

-- End of test
