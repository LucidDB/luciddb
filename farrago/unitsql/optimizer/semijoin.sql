-- $Id$
-- Tests semijoin transformations

create schema sj;
set schema 'sj';

-- set session personality to LucidDB so all tables
-- will be column-store by default
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table t(
    a int, b char(20), c varchar(20) not null,
    d varchar(128) not null);
create index it_d on t(d);
create index it_bd on t(b, d);

insert into t values(1, 'abcdef', 'no match', 'this is row 1');
insert into t values(2, 'abcdef', 'ghijkl', 'this is row 2');
insert into t values(3, 'abcdef', 'ghijkl', 'this is row 3');
insert into t values(4, null, 'ghijkl', 'this is row 4');
insert into t values(5, null, 'ghijkl', 'no match');

-- although this table has the same number of rows as t, we will force this
-- to be the dimension table in the semijoin by putting a dummy filter on
-- the table

create table smalltable(
    s1 varchar(128) not null, s2 int, s3 varchar(128) not null,
        s4 varchar(128) not null);
insert into smalltable values('this is row 1', 1, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 2', 2, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 3', 3, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 4', 4, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 5', 5, 'abcdef', 'ghijkl');

-- Create fake statistics.  The stats do not match the actual data in the
-- tables and are meant to force the optimizer to choose semijoins

call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'T', 10000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'SMALLTABLE', 10);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'T', 'B', 10, 100, 10, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'T', 'D', 10, 100, 10, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'SMALLTABLE', 'S1', 10, 100, 10, 1,
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'SMALLTABLE', 'S3', 10, 100, 10, 1,
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

-- explain plan tests

!set outputformat csv

----------------------
-- single column joins
----------------------

explain plan for select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by a;

explain plan for select *
    from t inner join smalltable s
    on t.d = s.s1 where s.s2 > 0 order by a;

-----------------
-- negative cases
-----------------
-- no index available to process join
explain plan for select *
    from t inner join smalltable s
    on t.c = s.s4 where s.s2 > 0;

-- filter not of the form cola = colb
explain plan for select *
    from t inner join smalltable s
    on t.d = upper(s.s1) where s.s2 > 0;

-- no filter on dimension table, so not worthwhile to do a semijoin
explain plan for select *
    from t inner join smalltable s
    on t.d = s.s1;

-- outer join
-- FIXME: Cartesian product is picked because join types do not match; however
-- cartesian product + outer join is not feasible.
-- explain plan for
-- select *
-- from t left outer join smalltable s
-- on t.b = s.s3 and s.s1 = 'this is row 1' 
-- order by a;

-- this outer join uses HashJoin since join types match
explain plan for
select *
from t full outer join smalltable s
on t.d = s.s1 where s.s2 > 0
order by a;

select *
from t full outer join smalltable s
on t.d = s.s1 where s.s2 > 0
order by a;

---------------------
-- multi-column joins
---------------------

explain plan for select *
    from t inner join smalltable s
    on t.d = s.s1 and t.b = s.s3 where s.s2 > 0 order by a;

-- same as above except join columns are reversed
explain plan for select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = t.d where s.s2 > 0 order by a;

-- join on 3 columns but index only on 2 columns
explain plan for select *
    from t inner join smalltable s
    on s.s1 = t.d and s.s3 = t.b and t.c = s.s4 where s.s2 > 0
    order by a;

-- same query but filters juggled around
explain plan for select *
    from t inner join smalltable s
    on s.s4 = t.c and s.s1 = t.d and s.s3 = t.b where s.s2 > 0
    order by a;

---------------------------------------
-- run queries above that use semijoins
---------------------------------------

!set outputformat table

select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' order by a;

select *
    from t inner join smalltable s
    on t.d = s.s1 where s.s2 > 0 order by a;

select *
    from t inner join smalltable s
    on t.d = s.s1 and t.b = s.s3 where s.s2 > 0 order by a;

select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = t.d where s.s2 > 0 order by a;

select *
    from t inner join smalltable s
    on s.s1 = t.d and s.s3 = t.b and t.c = s.s4 where s.s2 > 0
    order by a;

select *
    from t inner join smalltable s
    on s.s4 = t.c and s.s1 = t.d and s.s3 = t.b where s.s2 > 0
    order by a;

------------------
-- n-way semijoins
------------------

create table sales(
    sid int, product_id int, salesperson int, customer int, quantity int);
create index i_sales_pid on sales(product_id);
create index i_sales_sp on sales(salesperson);
create index i_sales_cust on sales(customer);

create table product(id int, name char(20), color char(10), size char(1));
create table salesperson(id int, name char(20), age int);
create table customer(id int, company char(20), city char(20));
create table state(city char(20), state char(20));

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

call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'SALES', 100000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'PRODUCT', 20);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'SALESPERSON', 10);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'CUSTOMER', 100);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'STATE', 5);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'SALES', 'PRODUCT_ID', 20, 100, 20, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'SALES', 'SALESPERSON', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'SALES', 'CUSTOMER', 100, 100, 100, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'PRODUCT', 'ID', 20, 100, 20, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'SALESPERSON', 'ID', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'CUSTOMER', 'ID', 100, 100, 100, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'CUSTOMER', 'CITY', 5, 100, 5, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'STATE', 'CITY', 5, 100, 5, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

!set outputformat csv

explain plan for
    select sid, p.name, p.color, p.size, s.quantity
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S'
        order by sid;

-- push semijoin past filter
explain plan for
    select sid, p.name, p.color, p.size, s.quantity
        from product p, sales s
        where
            s.product_id = p.id and p.size = 'S' and sid > 2
        order by sid;

-- push semijoin past joins
explain plan for
    select sid, p.name, p.color, p.size, sp.name, s.quantity
        from sales s, product p, salesperson sp
        where
            s.product_id = p.id and
            s.salesperson = sp.id and
            p.size >= 'M' and sp.age >= 30
        order by sid;

explain plan for
    select sid, p.name, p.color, p.size, sp.name, c.company
        from customer c, salesperson sp, product p, sales s
        where
            s.product_id = p.id and
            s.salesperson = sp.id and 
            s.customer = c.id and
            p.size >= 'M' and sp.age >= 30 and c.city >= 'N'
        order by sid;

-- push semijoin past filter and join
explain plan for
    select sid, p.name, p.color, p.size, sp.name, s.quantity
        from product p, sales s, salesperson sp
        where
            s.sid < 3 and
            s.product_id = p.id and
            s.salesperson = sp.id and
            p.size >= 'M' and sp.age >= 30
        order by sid;

-- chained join
explain plan for
    select sid, c.company, c.city, st.state
        from sales s, state st, customer c
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York'
        order by sid;

-- index can be used for both semijoins and table filtering

explain plan for
    select sid, p.name, p.color, p.size, s.quantity
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S' and
            s.salesperson > 0
        order by sid;

explain plan for
    select sid, p.name, p.color, p.size, s.quantity
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S' and
            s.salesperson = 1
        order by sid;

explain plan for
    select sid, p.name, p.color, p.size, s.quantity
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S' and
            s.salesperson > 0 and s.customer > 0
        order by sid;

-- semijoin used for IN clause; customer column has 100 distinct values so
-- the semijoin should be worthwhile
explain plan for
    select * from sales where customer in
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
explain plan for
    select * from sales where customer in
        (select id from customer where id < 10);

-- semijoin that needs to be removed

explain plan for
    select s.product_id from
        (select sum(quantity), product_id from sales group by product_id) s,
        product p
        where
            s.product_id = p.id and p.size = 'S'
        order by 1;

------------------
-- run the queries
------------------
!set outputformat table

select sid, p.name, p.color, p.size, s.quantity
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S'
    order by sid;

select sid, p.name, p.color, p.size, s.quantity
    from product p, sales s
    where
        s.product_id = p.id and p.size = 'S' and sid > 2
    order by sid;

select sid, p.name, p.color, p.size, sp.name, s.quantity
    from sales s, product p, salesperson sp
    where
        s.product_id = p.id and
        s.salesperson = sp.id and
        p.size >= 'M' and sp.age >= 30
    order by sid;

select sid, p.name, p.color, p.size, sp.name, c.company
    from customer c, salesperson sp, product p, sales s
    where
        s.product_id = p.id and
        s.salesperson = sp.id and 
        s.customer = c.id and
        p.size >= 'M' and sp.age >= 30 and c.city >= 'N'
    order by sid;

select sid, p.name, p.color, p.size, sp.name, s.quantity
    from product p, sales s, salesperson sp
    where
        s.sid < 3 and
        s.product_id = p.id and
        s.salesperson = sp.id and
        p.size >= 'M' and sp.age >= 30
    order by sid;

select sid, c.company, c.city, st.state
    from sales s, state st, customer c
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York'
        order by sid;

select sid, p.name, p.color, p.size, s.quantity
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S' and
        s.salesperson > 0
    order by sid;

select sid, p.name, p.color, p.size, s.quantity
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S' and
        s.salesperson = 1
    order by sid;

select sid, p.name, p.color, p.size, s.quantity
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S' and
        s.salesperson > 0 and s.customer > 0
    order by sid;

select * from sales where customer in
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    order by sid;

select * from sales where customer in
    (select id from customer where id < 10)
    order by sid;
