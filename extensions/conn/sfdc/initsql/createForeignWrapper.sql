--
-- This script creates the built-in foreign data wrappers
--

!set verbose true

-- Change this to whereever for testing
-- defaulted to something that runs in plain "farrago"
set schema 'LOCALDB.SALES';

--------------
-- SFDC Jar --
--------------

create or replace jar sfdcJar
library 'file:plugin/conn-sfdc-complete.jar'
options(1);
