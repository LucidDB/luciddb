create schema transform_schema;
set schema 'transform_schema';

create view timesheet_view as
select * from extraction_schema.timesheet;

create view emp_view as
select e.empno, e.ename, d.dname, e.job
from extraction_schema.emp e inner join extraction_schema.dept d
on e.deptno=d.deptno;

create view calendar_view as
select 
    time_key as calendar_date, 
    case when weekend='Y' then true else false end as is_weekend
from table(applib.time_dimension(2005, 1, 1, 2005, 12, 31));

select count(*) from timesheet_view;

select count(*) from emp_view;

select count(*) from calendar_view;

select * from emp_view;
