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
alter system set "cachePagesInit" = 1001;

-- should fail
alter system set "cachePagesInit" = 'abc';
