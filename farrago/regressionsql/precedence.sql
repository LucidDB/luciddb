-- $Id$
-- Full vertical system testing of precedence tests 

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

-- If a test fails it should have a sql comment preceding it saying it should fail. Oterwise it should pass.

-- should pass but doesnt
-- values true AND 'x' LIKE 'x';

-- OK
values (('x' LIKE 'x') or false);
values (false AND true OR NOT false AND true);
values (not not false);
values (1+2*3*10/2-3*2);
values ((1+2)*3);
values (2.0*power(2.0, 2.0)+3.0*power(2.0, 2.0));
values (2.0*{fn pow(2.0, 2.0)}+3.0*{fn pow(2.0, 2.0)});

values (true is unknown is not unknown);
values (not true is unknown is not unknown);
values (true and 3=3);
values (true and 3<>3);
values (true and 3>=3);
values (true and 3<=3);
values (true and 3>3);
values (true and 3<3);

values (true and 1 between 2 and 3);

