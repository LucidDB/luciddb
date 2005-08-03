-- $Id$
-- Full vertical system testing of the case function
-- 
alter system set "calcVirtualMachine"='CALCVM_JAVA';
-- 
-- The result is not primitive

-- condition is not nullable, list all possible cases, with else.
select manager, case manager when true then 'Yes' when false then 'No' else 'Other' end from sales.emps;

-- condition is not nullable, list one case, with else.
select name, case name when 'Fred' then 'Yes' else 'Other' end from sales.emps;

-- condition is not nullable, list one case, without else.
select deptno, case deptno when 10 then 'Yes' end from sales.emps;

-- condition is nullable, list all possible cases, with else.
select slacker, case slacker when true then 'yes' when false then 'no' else 'other' end from sales.emps;

-- condition is nullable, list two case, with else.
select age, case age when 50 then 'fifty' when 25 then 'twenty-five' end from sales.emps;

-- condition is nullable, list one case, without else.
select gender, case gender when 'M' then 'Yes' end from sales.emps;

-- The result is primitive

-- condition is not nullable, list all possible cases, with else.
select manager, case manager when true then 1 when false then 2 else 3 end from sales.emps;

-- condition is not nullable, list all possible cases, without else.
select name, case name when 'Fred' then 1 when 'Eric' then 2  when 'Wilma' then 3 when 'John' then 4 end from sales.emps;

-- condition is not nullable, list one case, with else.
select empno, case empno when 120 then 1 else 2 end from sales.emps;
-- condition is not nullable, list one case, without else.
select deptno, case deptno when 20 then 1 end from sales.emps;

-- condition is nullable, list all possible cases, with else. 
select deptno, case deptno when 10 then 1 when 20 then 2 when 40 then 3 else 4 end from sales.emps;
-- condition is nullable, list all possible cases, without else.
select slacker, case slacker when true then 1 when false then 2 end from sales.emps;
-- condition is nullable, list one case, with else.
select slacker, case slacker when true then 1 else 2 end from sales.emps;
-- condition is nullable, list one case, without else.
select slacker, case slacker when true then 1 end from sales.emps;

