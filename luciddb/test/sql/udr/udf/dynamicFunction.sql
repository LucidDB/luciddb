-- $Id$
-- Test script for dynamic_function using application variables UDF/UDP
create or replace schema functest;
set schema 'functest';
set path 'functest';

create or replace jar functest.myApplib
library 'file:${FARRAGO_HOME}/plugin/eigenbase-applib.jar'
options(0);

create or replace function functest.get_var(
    context_id varchar(128), 
    var_id varchar(128)) 
returns varchar(65535)
language java
deterministic
not dynamic_function
no sql
external name 'functest.myApplib:org.eigenbase.applib.variable.AppVarApi.executeGet';

-- use xmlattr format so we can distinguish nulls from blanks
!set outputformat xmlattr

-- for this test, enable code cache to make sure that changes
-- in variables do not get ignored due to stale plan cache
alter system set "codeCacheMaxBytes" = max;

-- create a context
call applib.create_var('ctx', null, 'very explicit');

-- create a variable in that context
call applib.create_var('ctx', 'var1', 'rather moody');

-- default value should be null
values (functest.get_var('ctx', 'var1'));

-- set a new value and verify value gotten from cache
call applib.set_var('ctx', 'var1', 'foo');
values (functest.get_var('ctx', 'var1'));

-- get cached value again
call applib.set_var('ctx', 'var1', 'bar');
values (functest.get_var('ctx', 'var1'));

create or replace function functest.get_var(
    context_id varchar(128), 
    var_id varchar(128)) 
returns varchar(65535)
language java
deterministic
dynamic_function
no sql
external name 'functest.myApplib:org.eigenbase.applib.variable.AppVarApi.executeGet';

-- get values again with dynamic_function specified
values (functest.get_var('ctx', 'var1'));

-- set a new value and verify value is not retrieved from cache
call applib.set_var('ctx', 'var1', 'foo');
values (functest.get_var('ctx', 'var1'));

-- set a new value and verify value is not retrieved from cache
call applib.set_var('ctx', 'var1', 'bar');
values (functest.get_var('ctx', 'var1'));

call applib.create_var('ctx2', null, 'uncomfortably implicit');
call applib.create_var('ctx2', 'var2', 'uncomfortably implicit');

-- default value should be null
values (functest.get_var('ctx2', 'var2'));

-- cleanup
drop schema functest cascade;
