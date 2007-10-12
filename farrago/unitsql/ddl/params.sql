-- $Id$
-- Test system parameters

-- should work
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- should work
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- should fail:  bad enum value
alter system set "calcVirtualMachine" = 'turing';

-- should work
alter system set "calcVirtualMachine" = 'CALCVM_AUTO';

-- should work
alter system set "cachePagesMax" = 1001;

-- should fail: invalid param
alter system set "cachePagesMax" = 0;

-- should fail: invalid param
alter system set "cachePagesMax" = -1;

-- should fail: type mismatch
alter system set "cachePagesMax" = 4294967295;

-- should fail:  type mismatch
alter system set "cachePagesMax" = 'a bunch';

-- should fail:  unknown parameter
alter system set "charlie" = 'horse';

-- should work
alter system set "cachePagesInit" = 10;

-- should work -- set it back to original value of 1000
alter system set "cachePagesInit" = 1000;

-- should fail
alter system set "cachePagesInit" = 0;

-- should fail
alter system set "cachePagesInit" = -1;

-- should fail -- cachePagesMax is 1000
alter system set "cachePagesInit" = 100001;

-- should fail
alter system set "cachePagesInit" = 'abc';

-- should work
alter system set "expectedConcurrentStatements" = 10;
alter system set "expectedConcurrentStatements" = 4;

-- should fail
alter system set "expectedConcurrentStatements" = 201;

-- should work
alter system set "cacheReservePercentage" = 10;
alter system set "cacheReservePercentage" = 5;

-- should fail
alter system set "cacheReservePercentage" = 100;

-- Test session parameters

-- should work
select * from sys_boot.mgmt.session_parameters_view
  where param_name in 
    ('catalogName', 'schemaName', 'sessionUserName', 'squeezeJdbcNumeric')
  order by 1;

-- should fail (farrago does not have this parameter)
alter session set "logDir" = 'testlog';

-- should fail
alter session set "squeezeJdbcNumeric" = 1;
alter session set "squeezeJdbcNumeric" = 'or anything besides a boolean';

-- should work
alter session set "squeezeJdbcNumeric" = false;

select * from sys_boot.mgmt.session_parameters_view
  where param_name = 'squeezeJdbcNumeric';

-- Test LucidDb session parameters

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- should work (luciddb inherits from farrago)
alter session set "squeezeJdbcNumeric" = 'true';

-- should work
alter session set "logDir" = 'testlog';
alter session set "etlProcessId" = 1234;
alter session set "etlActionId" = 'LoadAccount';
alter session set "errorMax" = 1000;
alter session set "errorLogMax" = 1000;

-- should fail
alter session set "logDir" = 'foobar';
alter session set "logDir" = 'README';
alter session set "logDir" = null;
alter session set "errorMax" = true;
alter session set "errorLogMax" = 101.51;
alter session set "errorMax" = 9876543210;
alter session set "errorLogMax" = -1;

-- should work
select * from sys_boot.mgmt.session_parameters_view
  where param_name in 
    ('logDir', 'etlProcessId', 'etlActionId', 'errorMax', 'errorLogMax')
  order by 1;

-- should work
alter session set "etlActionId" = null;
