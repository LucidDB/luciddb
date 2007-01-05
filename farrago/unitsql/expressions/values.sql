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
explain plan for values (system_user);

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

-- test LucidDB's standard-bending for 
-- SQL:2003 Part 2 Section 9.3 Syntax Rule 3.a.iii.3
-- (see http://sf.net/mailarchive/message.php?msg_id=13337379)

!set outputformat csv

values ('no'), ('yes'), ('maybe');

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

values ('no'), ('si'), ('es possible');
