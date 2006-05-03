
--
-- null tests: test queries on columns with NULLS
--

set schema 's';

-- single table

select LNAME from EMP where COMMISSION is NULL order by 1;

select LNAME, COMMISSION from EMP where COMMISSION is NOT NULL order by 1,2;

select LNAME, COMMISSION from EMP where COMMISSION=10 order by 1,2;

select LNAME, COMMISSION from EMP where COMMISSION<>10 order by 1,2;

select LNAME, COMMISSION from EMP where NOT (COMMISSION=10) order by 1,2;

select LNAME, COMMISSION from EMP where COMMISSION < 10 order by 1,2;

select LNAME, COMMISSION from EMP where COMMISSION > 5 order by 1,2;

-- join on nulls
select L.LNAME, R.LNAME, L.COMMISSION
from EMP L, EMP R
where L.COMMISSION = R.COMMISSION
order by 1,2,3;

select L.LNAME, R.LNAME, L.COMMISSION
from EMP L, EMP R
where NOT (L.COMMISSION = R.COMMISSION)
order by 1,2,3;

select L.LNAME, R.LNAME, L.COMMISSION
from EMP L, EMP R
where L.COMMISSION <> R.COMMISSION
order by 1,2,3;

select L.LNAME, R.LNAME, L.COMMISSION
from EMP L, EMP R
where L.COMMISSION < R.COMMISSION
order by 1,2,3;

select L.LNAME, R.LNAME, L.COMMISSION
from EMP L, EMP R
where L.COMMISSION >= R.COMMISSION
order by 1,2,3;

-- count (should not count null rows, except for count(*))

select count(*) from EMP;

select count(COMMISSION) from EMP;

-- order by (puts nulls first)
select lname, commission from emp order by 2,1;
--select lname, commission from emp order by 2 DESC,1 DESC;

-- Aggregates on empty tables should return 0 or NULL depending on the aggregate and group by ( Bugid 1578 )

select count(*), 1+count(*), sum(emp.deptno), count(emp.deptno)
 from emp,dept where emp.deptno=dept.deptno and emp.empno<0 and dept.locid='zz';

select count(*), 1+count(*), sum(emp.deptno), count(emp.deptno)
 from emp,dept where emp.deptno=dept.deptno and emp.empno<0 and dept.locid='zz' group by emp.deptno;
