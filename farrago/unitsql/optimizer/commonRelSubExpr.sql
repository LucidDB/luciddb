-- $Id$
-- Exercise materialization of common relational subexpressions.  Many of the
-- testcases in here are nonsense queries that are intended to exercise the
-- corner cases described at
-- http://pub.eigenbase.org/wiki/LucidDbCommonRelationalSubExpressionMaterialization

create schema crse;
set schema 'crse';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

create table sales(
    sid int primary key, product_id int, salesperson int, customer int,
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

-- more faking of stats; note also that the predicates chosen in the
-- actual queries aren't necessarily selective in reality but the stats
-- make the optimizer think they are

call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'CRSE', 'SALES', 100000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'CRSE', 'PRODUCT', 20);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'CRSE', 'SALESPERSON', 10);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'CRSE', 'CUSTOMER', 100);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'CRSE', 'STATE', 5);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'SALES', 'PRODUCT_ID', 20, 100, 20, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'SALES', 'SALESPERSON', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'SALES', 'CUSTOMER', 100, 100, 100, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'PRODUCT', 'ID', 20, 100, 20, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'SALESPERSON', 'ID', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'CUSTOMER', 'ID', 100, 100, 100, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'CUSTOMER', 'CITY', 5, 100, 5, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'CRSE', 'STATE', 'CITY', 5, 100, 5, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

create view v as
select sid, p.name as pname, p.color, p.size, sp.name as spname, sp.age,
    c.company, c.city, st.state, quantity
    from sales s left outer join product p on s.product_id = p.id
        left outer join salesperson sp on s.salesperson = sp.id
        left outer join customer c on s.customer = c.id
        left outer join state st on c.city = st.city;

!set outputformat csv

-- Buffering is not used here because it's not cost-effective

explain plan for
select count(distinct product_id), sum(quantity) from sales;

-- But it is used here

explain plan for
select count(distinct product_id), sum(quantity) from sales
    where salesperson < 2;

-- Multiple count-distinct's with another aggregate

explain plan for
select count(distinct pname), count(distinct company), sum(quantity) from v
    where size >= 'M' and city >= 'N';
explain plan for
select city, count(distinct pname), count(distinct company), min(quantity),
    max(quantity)
    from v 
    where size >= 'M' and city >= 'N'
    group by city;

-- Chained semijoin -- buffering should be done on the scans of both
-- STATE and CUSTOMER
explain plan for
select s.sid, s.quantity, c.city, st.state, c.company
    from sales s, state st, customer c
    where s.customer = c.id and (c.company like 'A%' or c.company like 'D%')
        and c.city = st.city and (st.state like 'New%');

-- This is intended to test a scenario similar to the query tree shown below,
-- which is referenced in the design writeup.
--
--         Z
--        / \
--       Y  B1
--      / \
--     B2 B2
--     |   |
--     X   X
--     |   |
--     B1 B1

create view v1 as
    select s.sid, p.name from sales s, product p
        where s.product_id = p.id;
create view v2 as
    select * from v1 union all select c.id, c.company from customer c;

explain plan for
(select * from v2 where name like 'r%'
    union all select * from v2 where name like 'r%')
union all
    select * from v1 where name like 'r%';

-- This is intended to test a scenario similar to the query tree shown below,
-- which is referenced in the design writeup.
--
--        -- X --
--       /       \
--       B        Y
--       |      /   \
--      IFC1  IFC2   Z
--              |
--              B
--              |
--             IFC1

create view v3 as
    select s.sid, p.name from sales s left outer join 
    (select id, name from product p where name like 'r%') as p
        on s.product_id = p.id;
explain plan for
select id, name from product where name like 'r%' union all
    (select * from v3 where name like 'ra%' union all 
        select id, company from customer);

-- Avoid buffering a common relational subexpression that will be removed
-- by join removal.  The first query can use buffering because the self-join
-- on the view is not removed.  But the second cannot be buffered because the
-- self-join is removed.

explain plan for
select * from v3 x, v3 y where x.sid = y.sid;
explain plan for
select x.* from v3 x, v3 y where x.sid = y.sid;

-- If the buffering rule is applied too early, the following queries will use
-- buffering, resulting in excessive joins.  The correct plans will not buffer.

explain plan for
select sid from sales s
    where not (quantity in (select id from product))
        and exists(select * from product where id = s.product_id);
explain plan for
select * from product p
    where (select min(quantity + product_id) from sales
            where product_id = p.id)
    < (select max(quantity + product_id) from sales
            where product_id = p.id);

-- Buffering is in a dataflow that is the input into a UDX that is on the RHS
-- of a hash join.  Buffering is done on both inputs into the hash join.
-- Therefore, if the scheduler incorrectly schedules the left input of the
-- hash join rather than the right, to consume the buffered data, a hang will
-- occur.

set path 'crse';
create function stringify(c cursor, delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringify';
explain plan for
select * from
    table(stringify(cursor(select * from product where name like 'p%'), '|'))
        as x,
    (select cast(id as varchar(10)) || '|' || name || '|' || color || '|'
            || size as v
        from product where name like 'p%') as y
    where x.v = y.v;

-- Buffering occurs in the RHS of a cartesian join, without a FennelBufferRel
-- in between the FennelMultiUseBufferRel and the cartesian join.  This
-- exercises executing SegBufferReaderExecStream in open-restart mode.

explain plan for
select c1.company, c2.city from
    (select * from customer where id = 1) as c1, 
    (select * from customer where id = 1) as c2;

!set outputformat table
-- Run the above queries

select count(distinct product_id), sum(quantity) from sales;
select count(distinct product_id), sum(quantity) from sales
    where salesperson < 2;
-- execute the above query a second time to ensure proper handling when
-- executing a previously closed stream graph
select count(distinct product_id), sum(quantity) from sales
    where salesperson < 2;
select count(distinct pname), count(distinct company), sum(quantity) from v
    where size >= 'M' and city >= 'N';
select city, count(distinct pname), count(distinct company), min(quantity),
    max(quantity)
    from v 
    where size >= 'M' and city >= 'N'
    group by city
    order by city;
(select * from v2 where name like 'r%'
        union all select * from v2 where name like 'r%')
    union all
        select * from v1 where name like 'r%'
    order by sid, name;
select id, name from product where name like 'r%' union all
    (select * from v3 where name like 'ra%' union all 
        select id, company from customer)
    order by id, name;
select * from v3 x, v3 y where x.sid = y.sid order by sid;
select x.* from v3 x, v3 y where x.sid = y.sid order by sid;
select sid from sales s
    where not (quantity in (select id from product))
        and exists(select * from product where id = s.product_id)
    order by sid;
select * from product p
    where (select min(quantity + product_id) from sales
            where product_id = p.id)
    < (select max(quantity + product_id) from sales
            where product_id = p.id)
    order by id;
select * from
    table(stringify(cursor(select * from product where name like 'p%'), '|'))
        as x,
    (select cast(id as varchar(10)) || '|' || name || '|' || color || '|'
            || size as v
        from product where name like 'p%') as y
    where x.v = y.v;
select c1.company, c2.city from
    (select * from customer where id = 1) as c1, 
    (select * from customer where id = 1) as c2;

alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';
