-- $Id$

-- These procedures are for internal use by tests only, so
-- we put them in a hidden catalog.  They can be dropped for
-- production instances.

create or replace procedure sys_boot.sys_boot.save_test_parameters()
language java
contains sql
external name 
'class org.luciddb.test.LucidDbTestCleanup.saveTestParameters';

create or replace procedure sys_boot.sys_boot.clean_test()
language java
contains sql
external name 
'class org.luciddb.test.LucidDbTestCleanup.cleanTest';
