-- $Id$
-- Test optimization rule to elimnate the use of UNION 
-- when a UNION call only has one input

-- should not contain any calls to any union xo
explain plan for select multiset[622] from (values(true));
