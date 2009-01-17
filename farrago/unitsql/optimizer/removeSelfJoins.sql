-- $Id$
-- Exercise queries where self-joins can be removed

create schema rsj;
set schema 'rsj';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table sales(
    sid int unique not null, product_id int, salesperson int, customer int,
    quantity int);
create index i_sales_pid on sales(product_id);
create index i_sales_sp on sales(salesperson);
create index i_sales_cust on sales(customer);

create table product(
    id int unique not null, name char(20), color char(10), size char(1));
create table salesperson(id int unique not null, name char(20), age int);
create table customer(
    id int unique not null, company char(20), city char(20) not null);
create table state(city char(20) unique not null, state char(20));

create index i_product_color on product(color);
create index i_product_size on product(size);
create index i_customer_city on customer(city);

insert into product values(1, 'radio', 'black', 'S');
insert into product values(2, 'phone', 'white', 'M');
insert into salesperson values(1, 'XYZ', 30);
insert into salesperson values(2, 'UVW', 40);
insert into customer values(1, 'ABC', 'San Mateo');
insert into customer values(2, 'DEF', 'New York City');
insert into state values('San Mateo', 'CA');
insert into state values('New York City', 'New York');
insert into sales values(1, 1, 1, 1, 10);
insert into sales values(2, 1, 1, 2, 20);
insert into sales values(3, 1, 2, 1, 30);
insert into sales values(4, 1, 2, 2, 40);
insert into sales values(5, 2, 1, 1, 50);
insert into sales values(6, 2, 1, 2, 60);
insert into sales values(7, 2, 2, 1, 70);
insert into sales values(8, 2, 2, 2, 80);

-- fake stats to force usage of semijoins; note that the predicates chosen in
-- the actual queries aren't necessarily selective in reality but the stats
-- make the optimizer think they are

call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RSJ', 'SALES', 100000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RSJ', 'PRODUCT', 20);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RSJ', 'SALESPERSON', 10);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RSJ', 'CUSTOMER', 100);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RSJ', 'STATE', 5);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'SALES', 'PRODUCT_ID', 20, 100, 20, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'SALES', 'SALESPERSON', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'SALES', 'CUSTOMER', 100, 100, 100, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'PRODUCT', 'ID', 20, 100, 20, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'SALESPERSON', 'ID', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'CUSTOMER', 'ID', 100, 100, 100, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'CUSTOMER', 'CITY', 5, 100, 5, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'RSJ', 'STATE', 'CITY', 5, 100, 5, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

!set outputformat csv

--------------------------------------------------------------------------------
-- Exercise various scenarios including combinations of the following:
-- a) projections on the self-join tables
-- b) filtering on the self-join tables
-- c) semijoin filtering on the self-join tables
-- d) semijoin filtering on the self-join tables where the joins to the
--    semijoin dimension table can also be removed
-- e) different orderings of tables in the FROM clause
--------------------------------------------------------------------------------

explain plan for
select s1.quantity, s2.* from sales s1, sales s2 where s1.sid = s2.sid and
    s2.quantity > 50;
explain plan for
select s1.*, s2.quantity from sales s1, sales s2 where s1.sid = s2.sid and
    s1.quantity > 50;
explain plan for
select s1.*, s2.* from sales s1, sales s2 where s1.sid = s2.sid and
    s1.quantity > 50 and s2.quantity > 60;
explain plan for
select s1.*, s2.* from sales s1, sales s2, product p
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M';
explain plan for
select s1.quantity, s2.quantity, sp.* from sales s1, sales s2, salesperson sp
    where s1.sid = s2.sid and s2.salesperson = sp.id and sp.age >= 40;

explain plan for
select s1.*, s2.* from sales s1, sales s2, product p, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40;
explain plan for
select s1.quantity, s2.* from sales s1, product p, sales s2, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40;
explain plan for
select s1.quantity, s2.* from sales s1, product p, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s1.quantity > 50;

explain plan for 
select s1.*, s2.quantity from product p, sales s1, sales s2, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40;
explain plan for 
select s1.quantity, s2.quantity
    from product p, sales s1, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40;
explain plan for 
select s1.quantity, s2.quantity
    from product p, salesperson sp, sales s1, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s1.quantity > 50;

explain plan for 
select s1.*, s2.quantity, p.*, sp.*
from sales s1, product p, sales s2, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s2.quantity > 50;
explain plan for 
select s1.quantity, s2.quantity, p.*, sp.*
from sales s1, product p, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s2.quantity > 50;
explain plan for 
select s1.quantity, s2.quantity, p.*, sp.*
from product p, sales s1, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s2.quantity > 50 and
        s1.quantity > 60;

!set outputformat table

select s1.quantity, s2.* from sales s1, sales s2 where s1.sid = s2.sid and
    s2.quantity > 50
    order by s2.sid;
select s1.*, s2.quantity from sales s1, sales s2 where s1.sid = s2.sid and
    s1.quantity > 50
    order by s1.sid;
select s1.*, s2.* from sales s1, sales s2 where s1.sid = s2.sid and
    s1.quantity > 50 and s2.quantity > 60
    order by s1.sid;
select s1.*, s2.* from sales s1, sales s2, product p
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M'
    order by s1.sid;
select s1.quantity, s2.quantity, sp.* from sales s1, sales s2, salesperson sp
    where s1.sid = s2.sid and s2.salesperson = sp.id and sp.age >= 40
    order by s1.quantity;
select s1.*, s2.* from sales s1, sales s2, product p, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40
    order by s1.sid;
select s1.quantity, s2.* from sales s1, product p, sales s2, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40
    order by s1.sid;
select s1.quantity, s2.* from sales s1, product p, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s1.quantity > 50
    order by s2.sid;
select s1.*, s2.quantity from product p, sales s1, sales s2, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40
    order by s1.sid;
select s1.quantity, s2.quantity
    from product p, sales s1, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40
    order by s1.quantity;
select s1.quantity, s2.quantity
    from product p, salesperson sp, sales s1, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s1.quantity > 50
    order by s1.quantity;
select s1.*, s2.quantity, p.*, sp.*
from sales s1, product p, sales s2, salesperson sp
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s2.quantity > 50
    order by s1.sid;
select s1.quantity, s2.quantity, p.*, sp.*
from sales s1, product p, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s2.quantity > 50
    order by s1.quantity;
select s1.quantity, s2.quantity, p.*, sp.*
from product p, sales s1, salesperson sp, sales s2
    where s1.sid = s2.sid and s1.product_id = p.id and p.size >= 'M' and
        s2.salesperson = sp.id and sp.age >= 40 and s2.quantity > 50 and
        s1.quantity > 60
    order by s1.quantity;

---------------------------------------------
-- Cases where the self-join can't be removed
---------------------------------------------

!set outputformat csv

-- outer joins

explain plan for select * from sales s1 left outer join sales s2
    on s1.sid = s2.sid and s2.quantity > 50;
explain plan for select * from sales s1 right outer join sales s2
    on s1.sid = s2.sid and s1.quantity > 50;
explain plan for select * from 
    (select * from sales where quantity > 50) s1
    full outer join 
    (select * from sales where quantity > 60) s2
    on s1.sid = s2.sid;

-- one of the self-join inputs is removed by a semijoin

explain plan for select s.* from sales s, product p1, product p2
    where s.product_id = p1.id and p1.size >= 'M' and p1.id = p2.id;

-- join inputs are not simple tables

explain plan for select * from
    (select sid from sales group by sid) s1, sales s2 where s1.sid = s2.sid;
explain plan for select * from
    sales s1,
    (select s.* from sales s full outer join product p on s.product_id = p.id)
        s2
    where s1.sid = s2.sid;

-- join keys are not unique

explain plan for select * from sales s1, sales s2
    where s1.product_id = s2.product_id;

-- join keys are derived
explain plan for select * from sales s1, sales s2
    where abs(s1.sid) = abs(s2.sid);
