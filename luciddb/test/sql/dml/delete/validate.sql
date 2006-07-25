------------------------------------------------------------------------------
-----
--  pure validation
-----
------------------------------------------------------------------------------


--{{{ Validate the table we are DELETING

-- DELETE code needs to validate whether the table exists, whether it
-- is a normal table (not a view, system-table, etc), whether user
-- has delete permission on this table

-- -- DELETE on a table that does not exist (should fail)
DELETE FROM DT.TableThatDoesNotExist
;

-- -- DELETE on a system table (should fail)
-- DELETE FROM system.columns
-- ;

-- -- DELETE on something in the catalog (should fail)
-- commented out: error message is not deterministic
-- delete from sys_fem.med."StoredColumn";

-- -- DELETE on a view (should fail)
--CREATE VIEW DT.onerowView AS SELECT * FROM system.onerow
--;

-- DELETE FROM DT.onerowView
-- ;
-- commented out: error message is not deterministic
--CREATE VIEW DT.onerowView as values(1, 'one', 1.00);

--DELETE from DT.onerowView;

-- -- DELETE on a table which we don't have DELETE permission for (should fail)
CREATE TABLE DT.permissionCheck (x integer)
;

-- CREATE USER aUser IDENTIFIED BY aUser
-- ;

CREATE USER AUSER AUTHORIZATION 'Unknown';

CREATE ROLE aRole
;

-- -- we grant all the permissions but the one that is actually necessary
-- GRANT ALL ON DT.permissionCheck TO aRole
-- ;

-- REVOKE DELETE ON DT.permissionCheck FROM aRole
-- ;
GRANT SELECT, INSERT, UPDATE on DT.permissionCheck TO AUSER;

GRANT ROLE aRole TO aUser
;


!closeall
!connect jdbc:luciddb: AUSER ""

DELETE FROM DT.permissionCheck
;

!closeall
!connect jdbc:luciddb: sa ""

DELETE FROM DT.permissionCheck
;

-- DELETE via a synonym (should succeed)
-- CREATE TABLE DT.tableForSynonym (x integer, y integer)
-- ;

-- INSERT INTO DT.tableForSynonym VALUES (1, 2)
-- ;

-- CREATE SYNONYM DT.tableSynonym FOR DT.tableForSynonym
-- ;

-- DELETE FROM DT.tableSynonym
-- ;

-- SELECT * FROM DT.tableSynonym
-- ;

--
-- Validate the WHERE clause

-- need to validate the where clause


--}}}
--{{{ Validate special permission case

-- -- create users

-- CREATE SCHEMA schemaA
-- ;
-- -- create user userA identified by userA default schema schemaA
-- -- ;
-- create user USERA authorization 'Unknown' default SCHEMA schemaA;

-- create role userA
-- ;
-- grant userA to userA
-- ;

-- CREATE SCHEMA schemaB
-- ;
-- -- create user userB identified by userB default schema schemaB
-- -- ;
-- create user USERB authorization 'Unknown' default SCHEMA schemaB;
-- create role userB
-- ;
-- grant role userB to userB
-- ;

-- GRANT ALL on schemaA.* TO userA
-- ;

-- GRANT ALL ON schemaB.* TO userB
-- ;

-- -- user A
-- connect userA userA

-- CREATE TABLE DIM (x integer primary key)
-- ;

-- GRANT SELECT ON DIM TO PUBLIC
-- ;

-- -- user B
-- connect userB userB

-- CREATE TABLE FACT (y integer references schemaA.DIM(x))
-- ;

-- -- user A again

-- connect userA userA

-- -- the following should work even though userA can't read FACT table
-- DELETE FROM DIM where x = 0
-- ;

-- -- this should fail
-- SELECT * FROM schemaB.fact
-- ;

-- --}}}
