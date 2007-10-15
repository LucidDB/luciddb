-- $Id$
-- Test EXPLAIN command.

set schema 'sales';
!set outputformat csv

-- Explain logical plan brief
explain plan excluding attributes without implementation for 
select 1 from depts;

-- Explain logical plan normal
explain plan including attributes without implementation for 
select 1 from depts;

-- Explain logical plan verbose
explain plan including all attributes without implementation for
select 1 from depts;

-- Explain physical plan brief
explain plan excluding attributes for
select 1 from depts;

-- Explain physical plan normal
explain plan including attributes for
select 1 from depts;

-- Explain physical plan verbose
explain plan including all attributes for
select 1 from depts;

-- Explain physical plan in XML format.
explain plan as xml for 
select 1 from depts;

-- Explain type
explain plan with type for
select 1 from depts;

-- Explain type as XML (ignores XML flag)
explain plan with type as xml for
select 1 from depts;

-- Verify rendering of literals in plans
explain plan for
values (1, -1.25, 1.0e50, true, 'bonjour', _ISO-8859-1'bonjour', 
_US-ASCII'bonjour', date '2006-11-08', time '15:05:05', 
timestamp '2006-11-08 15:05:05', X'CAFEBABE');

-- Apply a tweak to the session personality and verify that
-- the costing changes accordingly.

create schema s;
set schema 's';

-- first explain without tweak
explain plan including all attributes for
select count(*) from (values(0));

create jar test_personality_plugin 
library 'class net.sf.farrago.test.FarragoTestPersonalityFactory' 
options(0);

alter session implementation set jar test_personality_plugin;

-- explain again with tweak
explain plan including all attributes for
select count(*) from (values(0));


-- End explain.sql
