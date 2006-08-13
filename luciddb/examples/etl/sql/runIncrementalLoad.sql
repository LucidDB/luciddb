select * from extraction_schema.emp2;

create view transform_schema.emp2_view as
select e.empno, e.ename, d.dname, e.job
from extraction_schema.emp2 e inner join extraction_schema.dept d
on e.deptno=d.deptno;

merge into warehouse.employee_dimension tgt
using transform_schema.emp2_view src
on src.empno=tgt.empno
when matched then
update set ename=src.ename, dname=src.dname, job=src.job
when not matched then
insert(empno, ename, dname, job)
values(src.empno, src.ename, src.dname, src.job);

select * from warehouse.employee_dimension;
