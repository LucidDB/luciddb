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
--select 0.5e-3*1e3 from values(1);
--select 0.5e-3*1.1e3 from values(1);

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

select x'ff'=x'ff' from values(1);
select X'f2'<>x'ff' from values(1);
select x'11'>x'01' from values(1);
select x'11'<x'aa' from values(1);
select x'0a'<=x'0a' from values(1);
select x'0a'<=x'10' from values(1);
select x'20'<=x'10' from values(1);
select x'0001'>=x'100000' from values(1);

select x'f'=x'f' from values(1);
select X'2'<>x'f' from values(1);
--select x'1'>x'01' from values(1);
select x'1'<x'a' from values(1);
select x'a'<=x'a' from values(1);
select x'a'<=x'1' from values(1);
select x'0'<=x'1' from values(1);
select x'001'>=x'10000' from values(1);

--select b'10'=b'010' from values(1);
select b'1001'=b'1001' from values(1);
--select b'1'<>b'001' from values(1);
--select b'111'>b'01' from values(1);
select b'11'<b'11' from values(1);
select b'01'<=b'01' from values(1);
select b'01'<=b'10' from values(1);
select b'10'<=b'100' from values(1);
select B'0001'>=b'100000' from values(1);
