--
-- joinFilt.sql - join Filter tests
--

set schema 's';

--alter session set optimizerjoinfilterthreshold=2;
 
-- Standard join filter case
select lname,dname from emp,dept
where emp.deptno=dept.deptno and dept.dname='Marketing';

select lname,dname from emp,dept
where emp.deptno=dept.deptno and dept.dname<'Development'
order by 1;

-- multiple dimension filter conditions
--select emp.lname, emp.fname, dname from emp,dept
--where emp.deptno=dept.deptno and dept.dname='Accounting' and dept.locid in ('HQ','SF')
--order by 1,2;
select emp.lname, emp.fname, dname 
from emp, dept
where emp.deptno=dept.deptno and dept.dname='Accounting' 
  and (dept.locid='HQ' or dept.locid='SF')
order by 1,2;



-- don't reference dept in the select list, should drop out
-- of select list
select 1 from emp,dept
where emp.deptno=dept.deptno and dept.deptno=20
order by 1;

select emp.fname from emp,dept
where emp.deptno=dept.deptno and dept.deptno<20
order by 1;

-- multiple dimension tables, filters on both
select customers.lname, products.name, sales.price
--from sales, products, customers
from sales, customers, products
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and products.name >= 'Soap'
order by 1,2,3;

-- multiple dimension tables but filter on only one
select customers.lname, products.name, sales.price
--from sales, products, customers
from sales, customers, products
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname = 'Andrews'
order by 1,2,3;


-- multiple dimension tables, multiple filters
select customers.lname, products.name, sales.price
--from sales, products, customers
from sales, customers, products
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and customers.fname > 'S'
order by 1,2,3;

select customers.lname, products.name, sales.price
--from sales, products, customers
from sales, customers, products
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and customers.fname > 'S'
and sales.prodid < 10009
--and products.name IN ('Soap', 'Juice', 'Soup', 'Microwave', 'Soda')
and (products.name='Soap' or products.name='Juice' or products.name='Microwave' or products.name='Soda')
and products.price < 5.00
order by 1,2,3;

-- dimension tables not referenced in select list, should drop
-- out of join
select sum(sales.price)
--from sales, products, customers
from sales, customers, products
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and customers.fname > 'S'
and sales.prodid < 10009
--and products.name IN ('Soap', 'Juice', 'Soup', 'Microwave', 'Soda')
and (products.name='Soap' or products.name='Juice' or products.name='Microwave' or products.name='Soda')
and products.price < 5.00;

--select sum(sales.price)
--from sales
--where custid in (select custid from customers where lname<'F' and fname>'D')
--and prodid in (select prodid from products where name
--IN ('Soap', 'Juice', 'Soup', 'Microwave', 'Soda'));
