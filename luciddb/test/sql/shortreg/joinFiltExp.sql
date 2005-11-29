--
-- joinFilt.sql - join Filter tests
--

-- Standard join filter case
explain plan for select lname,dname from emp,dept
where emp.deptno=dept.deptno and dept.dname='Marketing';

explain plan for select lname,dname from emp,dept
where emp.deptno=dept.deptno and dept.dname<'Development'
order by 1;

-- multiple dimension filter conditions
explain plan for select emp.lname, emp.fname, dname from emp,dept
where emp.deptno=dept.deptno and dept.dname='Accounting' and dept.locid in ('HQ','SF')
order by 1,2;

-- don't reference dept in the select list, should drop out
-- of select list
explain plan for select 1 from emp,dept
where emp.deptno=dept.deptno and dept.deptno=20
order by 1;

explain plan for select emp.fname from emp,dept
where emp.deptno=dept.deptno and dept.deptno<20
order by 1;

-- multiple dimension tables, filters on both
explain plan for select customers.lname, products.name, sales.price
from sales, products, customers
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and products.name >= 'Soap'
order by 1,2,3;

-- multiple dimension tables but filter on only one
explain plan for select customers.lname, products.name, sales.price
from sales, products, customers
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname = 'Andrews'
order by 1,2,3;


-- multiple dimension tables, multiple filters
explain plan for select customers.lname, products.name, sales.price
from sales, products, customers
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and customers.fname > 'S'
order by 1,2,3;

explain plan for select customers.lname, products.name, sales.price
from sales, products, customers
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and customers.fname > 'S'
and sales.prodid < 10009
and products.name IN ('Soap', 'Juice', 'Soup', 'Microwave', 'Soda')
and products.price < 5.00
order by 1,2,3;

-- dimension tables not referenced in select list, should drop
-- out of join
explain plan for select sum(sales.price)
from sales, products, customers
where customers.custid=sales.custid
and sales.prodid = products.prodid
and customers.lname < 'C'
and customers.fname > 'S'
and sales.prodid < 10009
and products.name IN ('Soap', 'Juice', 'Soup', 'Microwave', 'Soda')
and products.price < 5.00;

explain plan for select sum(sales.price)
from sales
where custid in (select custid from customers where lname<'F' and fname>'D')
and prodid in (select prodid from products where name
	IN ('Soap', 'Juice', 'Soup', 'Microwave', 'Soda'));
