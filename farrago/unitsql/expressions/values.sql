-- $Id$
-- Test queries with VALUES

-- single-row
select * from (values ('hi',2,'bye'));

-- top-level
values ('hi',2,'bye');

-- multi-row
select * from (values ('hi',2,'bye'), ('foo',3,'bar')) order by 1;

-- top-level ordered
values ('hi',2,'bye'), ('foo',3,'bar') order by 1;

-- values with expressions
values (1+2);

-- bad:  identifiers in values
values (x,2);

-- bad:  expressions on identifiers in values
values (x+1,2);

-- test SESSION_USER
values (session_user);

-- test CURRENT_USER (should be same as SESSION_USER)
values (current_user);

-- test USER (should be same as CURRENT_USER)
values (user);

-- test SYSTEM_USER (but don't actually execute since it's context-dependent)
-- (zfong 5/9/07) - Commented out test since we now convert system_user and
-- other context dependent functions to constants during optimization
-- explain plan for values (system_user);

-- Janino had problems with this one (fixed in their 2.0.5 release)
values true and true;

-- test empty CURRENT_PATH
values current_path;

-- test single-schema CURRENT_PATH
set path 'sales';
values current_path;

-- test two-schema CURRENT_PATH
set path 'sys_boot.jdbc_metadata, sales';
values current_path;

-- test no-op SET PATH
set path current_path;
values current_path;

-- test complex SET PATH
set path current_path || ', sys_cwm."Relational"';
values current_path;

-- test CURRENT_CATALOG and CURRENT_SCHEMA from SQL:2008
values current_catalog;
values current_schema;

set catalog 'sys_boot';
values current_catalog;
values current_schema;

set schema 'localdb.sales';
values current_catalog;
values current_schema;

-- note that changing the catalog does not affect the schema
set catalog 'sys_boot';
values current_catalog;
values current_schema;

-- test LucidDB's standard-bending for 
-- SQL:2003 Part 2 Section 9.3 Syntax Rule 3.a.iii.3
-- (see http://sf.net/mailarchive/message.php?msg_id=13337379)
-- and some numeric type derivation

values (1/3);

-- if type union results in loss of digits, verify that rounding occurs
values 100000.0, 0.555555555555555555;

!set outputformat csv

values ('no'), ('yes'), ('maybe');

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

values (1/7);

values ('no'), ('si'), ('es posible');
