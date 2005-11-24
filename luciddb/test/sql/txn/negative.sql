-- $Id$
-- Negative tests for LucidDB transaction support

-- should fail:  we don't support transactions
!autocommit off

-- should fail:  still in autocommit mode
commit;

-- should fail:  still in autocommit mode
rollback;
