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

-- should fail:  immutable
alter system set "fennelDisabled" = true;

-- should work
alter system set "cachePagesMax" = 1001;

-- should fail:  type mismatch
alter system set "cachePagesMax" = 'a bunch';

-- should fail:  unknown parameter
alter system set "charlie" = 'horse';
