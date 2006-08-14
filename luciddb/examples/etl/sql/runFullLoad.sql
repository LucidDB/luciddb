set schema 'warehouse';

insert into employee_dimension(empno, ename, dname, job)
select * from transform_schema.emp_view;

insert into calendar_dimension(calendar_date, is_weekend)
select * from transform_schema.calendar_view;

insert into timesheet_fact(emp_key, workday_key, hours_worked)
select 
    e.emp_key,
    c.calendar_key,
    t.hours_worked
from 
    transform_schema.timesheet_view t,
    employee_dimension e,
    calendar_dimension c
where
    t.empno=e.empno and t.workday=c.calendar_date;

analyze table employee_dimension compute statistics for all columns;
analyze table calendar_dimension compute statistics for all columns;
analyze table timesheet_fact compute statistics for all columns;

select count(*) from employee_dimension;

select count(*) from calendar_dimension;

select count(*) from timesheet_fact;

select * from employee_dimension;
