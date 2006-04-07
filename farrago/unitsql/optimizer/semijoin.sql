-- $Id$
-- Tests semijoin transformations

create schema sj;
set schema 'sj';

-- set session personality to LucidDB so all tables
-- will be column-store by default

-- fake jar since we don't actually build a separate jar for LucidDB yet
create jar luciddb_plugin 
library 'class com.lucidera.farrago.LucidDbSessionFactory' 
options(0);

alter session implementation set jar luciddb_plugin;

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

-- outer join
-- TODO - add a testcase for outer joins once we support these on lcs tables

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
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S' and sid > 2
        order by sid;

-- push semijoin past joins
explain plan for
    select sid, p.name, p.color, p.size, sp.name, s.quantity
        from sales s, product p, salesperson sp
        where
            s.product_id = p.id and
            s.salesperson = sp.id
        order by sid;

explain plan for
    select sid, p.name, p.color, p.size, sp.name, c.company
        from sales s, product p, salesperson sp, customer c
        where
            s.product_id = p.id and
            s.salesperson = sp.id and 
            s.customer = c.id
        order by sid;

-- push semijoin past filter and join
explain plan for
    select sid, p.name, p.color, p.size, sp.name, s.quantity
        from sales s, product p, salesperson sp
        where
            s.sid < 3 and
            s.product_id = p.id and
            s.salesperson = sp.id
        order by sid;

-- chained join
explain plan for
    select sid, c.company, c.city, st.state
        from sales s, customer c, state st
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York'
        order by sid;

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
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S' and sid > 2
    order by sid;

select sid, p.name, p.color, p.size, sp.name, s.quantity
    from sales s, product p, salesperson sp
    where
        s.product_id = p.id and
        s.salesperson = sp.id
    order by sid;

select sid, p.name, p.color, p.size, sp.name, c.company
    from sales s, product p, salesperson sp, customer c
    where
        s.product_id = p.id and
        s.salesperson = sp.id and 
        s.customer = c.id
    order by sid;

select sid, p.name, p.color, p.size, sp.name, s.quantity
    from sales s, product p, salesperson sp
    where
        s.sid < 3 and
        s.product_id = p.id and
        s.salesperson = sp.id
    order by sid;

select sid, c.company, c.city, st.state
    from sales s, customer c, state st
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York'
        order by sid;

