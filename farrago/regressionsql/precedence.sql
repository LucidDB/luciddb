-- $Id$
-- Full vertical system testing of precedence tests 

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

-- If a test fails it should have a sql comment preceding it saying it should fail. Oterwise it should pass.

-- should pass but doesnt
-- select true AND 'x' LIKE 'x' from values(1);

-- OK
select ('x' LIKE 'x') or false from values(1);
select false AND true OR NOT false AND true from values(1);
select not not false from values(1);
select 1+2*3*10/2-3*2 from values(1);
select (1+2)*3 from values(1);
select 2.0*pow(2.0, 2.0)+3.0*pow(2.0, 2.0) from values(1);
select 2.0*{fn pow(2.0, 2.0)}+3.0*{fn pow(2.0, 2.0)} from values(1);

select true is unknown is not unknown from values(1);
select not true is unknown is not unknown from values(1);
select true and 3=3 from values(1);
select true and 3<>3 from values(1);
select true and 3>=3 from values(1);
select true and 3<=3 from values(1);
select true and 3>3 from values(1);
select true and 3<3 from values(1);

select true and 1 between 2 and 3 from values(1);

