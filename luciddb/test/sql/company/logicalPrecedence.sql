set schema 's';

create table booltest (bt boolean, bf boolean);
insert into booltest values(true, false);

--------------------------------------------
-- minimum: NOT vs AND; NOT vs OR; AND vs OR
--------------------------------------------

-- NOT vs AND: not x and y = (not x) and y
-- not false and false = (not false) and false = false
-- right: count = 0
select count(*) from booltest where not bf and bf;
select count(*) from booltest where (not bf) and bf;
-- wrong: count = 1
select count(*) from booltest where not (bf and bf);
-- right: result = false
select not bf and bf from booltest;
select (not bf) and bf from booltest;
-- wrong: result = true
select not (bf and bf) from booltest;

-- NOT vs OR: not x or y = (not x) or y
-- not true or true = (not true) or true = true
-- right: count = 1
select count(*) from booltest where not bt or bt;
select count(*) from booltest where (not bt) or bt;
-- wrong: count = 0
select count(*) from booltest where not (bt or bt);

-- AND vs OR: x or y and z = x or (y and z)
-- true or true and false = true or (true and false) = true
-- right: count = 1
select count(*) from booltest where bt or bt and bf;
select count(*) from booltest where bt or (bt and bf);
-- wrong: count = 0
select count(*) from booltest where (bt or bt) and bf;
-- AND vs OR: x and y or z = (x and y) or z
-- false and true or true = (false and true) or true = true
-- right: count = 1
select count(*) from booltest where bf and bt or bt;
select count(*) from booltest where (bf and bt) or bt;
-- wrong: count = 0
select count(*) from booltest where bf and (bt or bt);


-----------
-- extra
-----------

-- x and not y and z 
-- true and not false and false = true and (not false) and false = false
-- right: count = 0
select count(*) from booltest where bt and not bf and bf;
select count(*) from booltest where bt and (not bf) and bf;
-- wrong: count = 1
select count(*) from booltest where bt and (not (bf and bf));


-- not x and y and z
-- not false and false and true = (not false) and false and true = false
-- rigth: count = 0
select count(*) from booltest where not bf and bf and bt;
select count(*) from booltest where ((not bf) and bf) and bt;
-- wrong: count = 1 
select count(*) from booltest where not (bf and bf) and bt;
select count(*) from booltest where not (bf and bf and bt);


-- x or not y or z
-- false or not true or true = false or (not true) or true = true
-- right: count = 1
select count(*) from booltest where bf or not bt or bt;
select count(*) from booltest where bf or (not bt) or bt;
--wrong: count = 0
select count(*) from booltest where bf or (not (bt or bt));

-- not x or y or z
-- not true or true or false = (not true) or true or false = true
--right: count = 1
select count(*) from booltest where not bt or bt or bf;
select count(*) from booltest where (not bt) or bt or bf;
--wrong: count = 0
select count(*) from booltest where not (bt or bt) or bf;
select count(*) from booltest where not (bt or bt or bf);
