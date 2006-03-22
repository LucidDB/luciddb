set schema 's';
--
-- stats.sql - test statistic gathering
--

--set echo=on
--set plan=both
--alter session set explainplandetail=3;

--analyze table emp estimate statistics for all columns sample 100 percent;
--select * from histogram_bars where table_name='EMP' and column_name='EMPNO';

select count(*) from emp where empno=105;
select count(*) from emp where empno<105;
select count(*) from emp where empno=101 or empno=105 or empno=107;
--select count(*) from emp where empno in (101, 105, 107);

--analyze table sales estimate statistics for all columns sample 100 percent;
--select * from histogram_bars where table_name='SALES' and column_name='PRICE';
--select * from histogram_bars where table_name='SALES' and column_name='PRODID';

select count(*) from sales where price<1;
select count(*) from sales where prodid=10010;
--select count(*) from emp,dept where emp.deptno=dept.deptno and dname='Marketing';

--analyze table sales estimate statistics for all columns sample 10 percent;
--select * from histogram_bars where table_name='SALES' and column_name='PRICE';
--select * from histogram_bars where table_name='SALES' and column_name='PRODID';

select count(*) from sales where price<1;
select count(*) from sales where prodid=10010;
--select count(*) from emp,dept where emp.deptno=dept.deptno and dname='Marketing';

--analyze table sales estimate statistics for all columns sample 1 percent;
--select * from histogram_bars where table_name='SALES' and column_name='PRICE';
--select * from histogram_bars where table_name='SALES' and column_name='PRODID';

select count(*) from sales where price<1;
select count(*) from sales where prodid=10010;
--select count(*) from emp,dept where emp.deptno=dept.deptno and dname='Marketing';
