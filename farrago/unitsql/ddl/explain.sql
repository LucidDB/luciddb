-- $Id$
-- Test EXPLAIN command.

set schema 'sales';

-- Explain in XML format.
explain plan as xml for select 1 from emps;
