-- $Id$
-- This script switches the default row store to mock storage
-- for use on systems without Fennel support.

!set verbose true
!autocommit off

drop server sys_rowstore;

create server sys_rowstore
local data wrapper sys_mock;

commit;
