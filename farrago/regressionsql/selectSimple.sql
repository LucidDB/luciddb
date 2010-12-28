-- $Id$
-- Full vertical system testing of non advanced select statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select empno from sales.emps order by empno asc;
select empno+1 from sales.emps order by 1;
select empno-1 from sales.emps order by 1;
select empno*2 from sales.emps order by 1;
select empno/2 from sales.emps order by 1;

select empno from sales.emps where empno=110;
select empno from sales.emps where empno>=110 order by 1;
select empno from sales.emps where empno>110;
select empno from sales.emps where empno<=110 order by 1;
select empno from sales.emps where empno<110;
select empno from sales.emps where empno<>110 order by 1;

select empno from sales.emps where empno=99999;
select empno from sales.emps where empno>=99999;
select empno from sales.emps where empno<1;
select empno from sales.emps where empno<=1;
select empno from sales.emps where empno>99999;
select empno from sales.emps where empno<>99999 order by 1;

select empno+1 from sales.emps where empno=110;
select empno+1 from sales.emps where empno>=110 order by 1;
select empno+1 from sales.emps where empno>110;
select empno+1 from sales.emps where empno<110;

select empno+1, empno/2 from sales.emps order by 1;

select * from sales.emps where name = 'Wilma';
select * from sales.emps where name = 'wilma';
select empno, empno from sales.emps order by 1;
--select empno,*,empno from sales.emps;
select 1 as apa, age as apa, 3 as APA, 4 as "APA" from sales.emps order by 2;
select 1+2 as empno, empno as empno, age as empno, 1+2 as empno from sales.emps order by 3;

select age from (select emps.* from sales.emps) order by 1;

--Fails
SELECT 0.5e1.1 from sales.emps;

--OK 
--values 0.5e-3*1e3;
--values 0.5e-3*1.1e3;

values true>=true;
values true>=false;
values true>=unknown;
values false>=true;
values false>=false;
values false>=unknown;
values unknown>=true;
values unknown>=false;
values unknown>=unknown;
values true<=true;
values true<=false;
values true<=unknown;
values false<=true;
values false<=false;
values false<=unknown;
values unknown<=true;
values unknown<=false;
values unknown<=unknown;

-- SQL2003 Part 2 Section 4.3.2 and
-- SQL2003 Part 2 Section 8.2 General Rule 4 state
-- that CLOBS (ex. BINARY/VARBINARY) may only be compared for
-- equality. Currently allow the other (>,>=, etc.) comparisons
-- as an extension.
-- tests that return true:
values   x'ff' =  x'ff';
values   x'ff' <= x'ff';
values   x'ff' >= x'ff';
values   x'00' =  x'00';
values x'00ff' =  x'00ff';
values   x'ff' >  x'00';
values x'00ff' >  x'0000';
values x'0000' >  x'00';
values   x'ff' >= x'00';
values x'00ff' >= x'0000';
values x'0000' >= x'00';
values   x'00' <  x'ff';
values   x'00' <  x'0000';
values x'0000' <  x'00ff';
values   x'00' <= x'ff';
values   x'00' <= x'0000';
values x'0000' <= x'00ff';
values   x'00' <> x'ff';
values   x'ff' <> x'00';
values x'0000' <> x'0001';
values x'0000' <> x'00';
values   x'00' <> x'0000';

-- symmetric (inverted) tests return false:
values   x'ff' <>  x'ff';
values   x'00' <>  x'00';
values x'00ff' <>  x'00ff';
values   x'ff' <  x'00';
values x'00ff' <  x'0000';
values x'0000' <  x'00';
values   x'ff' <= x'00';
values x'00ff' <= x'0000';
values x'0000' <= x'00';
values   x'00' >  x'ff';
values   x'00' >  x'0000';
values x'0000' >  x'00ff';
values   x'00' >= x'ff';
values   x'00' >= x'0000';
values x'0000' >= x'00ff';
values   x'00' =  x'ff';
values   x'ff' =  x'00';
values x'0000' =  x'0001';
values x'0000' =  x'00';
values   x'00' =  x'0000';


values 'a' is distinct from 'a';
values 'a' is distinct from 'aa';
values 'a' is distinct from 'b';
values 1 is distinct from cast(null as integer);
values cast(null as integer) is distinct from cast(null as integer);

-- a few boundary cases for decimal precision 19
values cast (0.1234567890123456789 as decimal(10,0));
values cast (0.8876543210987654321 as decimal(10,0));
values cast (-0.8876543210987654321 as decimal(10,0));
values cast (1e-5 as decimal(19,19));
values 1 + 0.1234567890123456789;
values floor(0.8876543210987654321);
values floor(0.0000000000000000001);
values floor(-0.0000000000000000001);
values floor(-0.8876543210987654321);
values ceil(0.8876543210987654321);
values ceil(0.0000000000000000001);
values ceil(-0.0000000000000000001);
values ceil(-0.8876543210987654321);

-- char to date conversions
-- these fail, as expected
values cast('1997-01-00' as date);
values cast('1997-02-29' as date);
values cast('1997-00-01' as date);
values cast('1997-13-01' as date);
-- this works
values cast('9999-01-01' as date);
-- FIXME: these work on Java calc, not Fennel calc (LER-2866)
-- values cast('997-01-01' as date);
-- values cast('97-01-01' as date);
-- FIXME: this fails on both calcs (LER-2866)
-- values cast('10000-01-01' as date);

-- NOTE: the rest of this file runs as luciddb
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- decimal multiplication, luciddb semantics

-- fewer than 19 digits, keep fractional digits
values 1.000000 * 1.000000;
-- greater than 19 digits, take off a few fractional digits
values 123456789.000000 * 1.000000;
-- many integer digits, limit fractional digits to 6 digits
values 123456789.000 
  * cast(1.000000 as decimal(18,6))
  * cast(1.000000 as decimal(18,6));
-- check large value
values cast(123456789.000000 * 10000.000000 as decimal(18,3));

-- decimal division, luciddb semantics

-- luciddb preserves the desired scale, but caps it at 6 to preserve the 
-- integral part and avoid subsequent overflows
values (
  cast(158229.4028 as decimal(19,4)) 
  / cast(5523083.9328 as decimal(19,4)));
values (
  cast (123456789012 as decimal(19,4))
  / cast (1000000000 as decimal(19,0)));
values (cast((1.0/1000000.0) + 123456789012 as decimal(18,3)));
values (
  cast(158229.4028 as decimal(19,8)) 
  / cast(5523083.9328 as decimal(19,8)));
values (
  cast(1234567 as decimal(19,0)) 
  / cast(0.000001 as decimal(19,8)));
-- we lose very small values
values 1.0/10000000.0;
-- detect overflow errors
values (
  cast(12345678 as decimal(19,0)) 
  / cast(0.000001 as decimal(19,8)));

-- end selectSimple.sql
