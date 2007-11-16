-- $Id$
-- Test effect on view's stored original text when a dependency is re-created.

!set outputformat csv
!set force true
CREATE OR REPLACE SCHEMA dtbug1173;
SET SCHEMA 'dtbug1173';
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
CREATE OR REPLACE VIEW v2 AS SELECT *, 999 AS k FROM v;
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
-- expect original view text to be unchanged
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;
CREATE OR REPLACE VIEW v AS SELECT 1 AS i, 2 AS j FROM (VALUES (TRUE));
-- must change V2's text because V's columns have changed
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;
-- End replaceView.sql
