-- $Id$
-- Test ROW constructor

-- NOTE:  sales.depts is used because optimizer fails
-- when onerow is replaced with values().  
-- Should change once optimizer is fixed.

-- NOTE:  field names of ROW constructors are implementation-defined.
-- These tests rely on Farrago implementation specifics.  Once
-- supported, should use ROW CAST in most cases to assign names.

-- test row single field access
select t.r."EXPR$0"
from (select row(1,2) r from sales.depts) t;

-- test row multiple field access
select t.r."EXPR$1", t.r."EXPR$0"
from (select row(1,2) r from sales.depts) t;

-- test without ROW noiseword
select t.r."EXPR$1", t.r."EXPR$0"
from (select (1,2) r from sales.depts) t;

-- something deeper
select t.r."EXPR$1"."EXPR$2"
from (select ((1,2),(3,4,5)) r from sales.depts) t;

-- test whether optimizer expands expressions redundantly
!set outputformat csv
explain plan for
select t.r."EXPR$1", t.r."EXPR$0"
from (select row(1,2) r from sales.depts) t;

-- aliased columns
select a + c as d
from (values (1, 2, 3), (4, 5, 6)) as t(a, b, c)
where b > 0
order by a;

-- End row.sql
