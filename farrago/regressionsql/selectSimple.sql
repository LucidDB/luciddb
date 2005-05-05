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

values x'ff'=x'ff';
values X'f2'<>x'ff';
values x'11'>x'01';
values x'11'<x'aa';
values x'0a'<=x'0a';
values x'0a'<=x'10';
values x'20'<=x'10';
values x'0001'>=x'100000';

values 'a' is distinct from 'a';
values 'a' is distinct from 'aa';
values 'a' is distinct from 'b';
values 1 is distinct from cast(null as integer);
values cast(null as integer) is distinct from cast(null as integer);

-- end selectSimple.sql
