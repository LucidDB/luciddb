-- $Id$
-- Test script for application variables UDF/UDP
set schema 'udftest';
set path 'udftest';

-- use xmlattr format so we can distinguish nulls from blanks
!set outputformat xmlattr

-- create a context
call applib.create_var('context1', null, 'very explicit');

-- create a variable in that context
call applib.create_var('context1', 'var1', 'rather moody');

-- default value should be null
values (applib.get_var('context1', 'var1'));

-- set a new value and verify that value was updated
call applib.set_var('context1', 'var1', 'foo');
values (applib.get_var('context1', 'var1'));

-- and again
call applib.set_var('context1', 'var1', 'bar');
values (applib.get_var('context1', 'var1'));

-- delete variable
call applib.delete_var('context1', 'var1');

-- should fail:  no longer exists
values (applib.get_var('context1', 'var1'));

-- should fail:  attempt to access a variable we never even created
values (applib.get_var('context1', 'var0'));

-- create a variable, implicitly creating its context
call applib.create_var('context2', 'var2', 'uncomfortably implicit');

-- default value should be null
values (applib.get_var('context2', 'var2'));

-- delete context, which should implicitly delete variable
call applib.delete_var('context2', null);

-- should fail:  no longer exists
values (applib.get_var('context2', 'var2'));
