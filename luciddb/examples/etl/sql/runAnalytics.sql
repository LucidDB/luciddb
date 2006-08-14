set schema 'warehouse';

-- average hours worked by employee
select ename, avg(hours_worked) as avg_hours
from timesheet_fact t, employee_dimension e
where t.emp_key=e.emp_key
group by ename;

-- average hours worked by job
select job, avg(hours_worked) as avg_hours
from timesheet_fact t, employee_dimension e
where t.emp_key=e.emp_key
group by job;

-- who works on weekends?
select distinct ename, job
from timesheet_fact t, employee_dimension e, calendar_dimension c
where t.emp_key=e.emp_key
and t.workday_key=c.calendar_key
and c.is_weekend;
