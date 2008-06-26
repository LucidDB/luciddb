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
    s1 varchar(128) not null, s2 int, s3 varchar(128),
        s4 varchar(128) not null);
insert into smalltable values('this is row 1', 1, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 2', 2, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 3', 3, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 4', 4, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 5', 5, 'abcdef', 'ghijkl');
insert into smalltable values('this is row 6', 6, null, 'ghijkl');

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

-- outer joins -- should not use semijoin
explain plan for
select *
from t left outer join smalltable s
on t.b = s.s3 and s.s1 = 'this is row 1';
explain plan for
select *
from t right outer join smalltable s
on t.b = s.s3 and s.s1 = 'this is row 1';
explain plan for
select *
from t full outer join smalltable s
on t.b = s.s3 and s.s1 = 'this is row 1';

-- this outer join uses HashJoin since join types match
explain plan for
select *
from t full outer join smalltable s
on t.d = s.s1 where s.s2 > 0
order by a;

!set outputformat table
select *
from t full outer join smalltable s
on t.d = s.s1 where s.s2 > 0
order by a, s1;

---------------------
-- multi-column joins
---------------------
!set outputformat csv
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

explain plan for
    select sid, p.name, p.color, p.size, s.quantity
        from product p, sales s
        where
            s.product_id = p.id
            and p.size = 'S' and p.color = 'white'
            and sid > 2
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

-- cartesian product join where the RHS of the cartesian product join contains
-- a semijoin where the dimension table is processed using index intersection;
-- this testcase ensures that things behave correctly if an early close is done
-- on the bitmap intersect
explain plan for
    select a, sid, name, color, size, quantity
        from t left outer join
        (select * from  sales s, product p
            where
                s.product_id = p.id and p.size = 'S' and p.color = 'black')
        on true
    order by a, sid;

-- similar to the above query except the dimension table is processed using
-- a bitmap merge
explain plan for
    select a, sid, name, color, size, quantity
        from t left outer join
        (select * from  sales s, product p
            where
                s.product_id = p.id and p.color > 'w')
        on true
    order by a, sid;

-- semijoin can't be used here because we don't push semijoins past aggregates
explain plan for
    select s.product_id from
        (select sum(quantity), product_id from sales group by product_id) s,
        product p
        where
            s.product_id = p.id and p.size = 'S'
        order by 1;

-----------------------------
-- run the n-way join queries
-----------------------------
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

-- run the queries below twice to ensure proper handling when executing a
-- previously closed stream graph
select a, sid, name, color, size, quantity
    from t left outer join
    (select * from  sales s, product p
        where
            s.product_id = p.id and p.size = 'S' and p.color = 'black')
    on true
order by a, sid;
select a, sid, name, color, size, quantity
    from t left outer join
    (select * from  sales s, product p
        where
            s.product_id = p.id and p.size = 'S' and p.color = 'black')
    on true
order by a, sid;

select a, sid, name, color, size, quantity
    from t left outer join
    (select * from  sales s, product p
        where
            s.product_id = p.id and p.color > 'w')
    on true
order by a, sid;
select a, sid, name, color, size, quantity
    from t left outer join
    (select * from  sales s, product p
        where
            s.product_id = p.id and p.color > 'w')
    on true
order by a, sid;

--------------------------------------------------------------------------
-- semijoin used for IN clause; customer column has 100 distinct values so
-- the semijoin should be worthwhile
--------------------------------------------------------------------------
!set outputformat csv
explain plan for
    select * from sales where customer in
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
explain plan for
    select * from sales where customer in
        (select id from customer where id < 10);

!set outputformat table
select * from sales where customer in
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    order by sid;

select * from sales where customer in
    (select id from customer where id < 10)
    order by sid;

--------------------------
-- test semijoins on views
--------------------------
create view vt(vc, vd, vb, vbd) as
    select upper(c), trim(d), b, b || d from t;
select * from vt;
!set outputformat csv

-- semijoin should be usable in these two cases; for the second and third,
-- should only use the index on column b
explain plan for
    select * from vt, smalltable s
        where vt.vb = s.s3 and s.s1 = 'this is row 1';
explain plan for
    select * from vt, smalltable s
        where vt.vb = s.s3 and vt.vbd = s.s1 and s.s2 > 0;
explain plan for
    select * from vt, smalltable s
        where vt.vb = s.s3 and vt.vd = s.s1 and s.s2 > 0;

-- but not in these cases
explain plan for
    select * from vt, smalltable s where vt.vd = s.s1 and s.s2 > 0;
explain plan for
    select * from vt, smalltable s
        where vt.vbd = s.s3 and s.s1 = 'this is row 1';

-- run the queries corresponding to the cases where semijoins can be used
!set outputformat table
select * from vt, smalltable s
    where vt.vb = s.s3 and s.s1 = 'this is row 1' order by vc;
select * from vt, smalltable s
    where vt.vb = s.s3 and vt.vbd = s.s1 and s.s2 > 0 order by vc;
select * from vt, smalltable s
    where vt.vb = s.s3 and vt.vd = s.s1 and s.s2 > 0 order by vc;

-------------------------------------------------------
-- cases where join with dimension table can be removed
-------------------------------------------------------
!set outputformat csv
explain plan for
    select sid, p.id, s.quantity
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S'
        order by sid;
explain plan for
    select sid
        from product p, sales s
        where
            s.product_id = p.id and p.size = 'S' and sid > 2
        order by sid;
explain plan for
    select sid, p.id as pid, sp.id spid, s.quantity
        from sales s, product p, salesperson sp
        where
            p.id = s.product_id and
            s.salesperson = sp.id and
            p.size = 'M' and sp.age = 30
        order by sid;
explain plan for
    select sid
        from customer c, salesperson sp, product p, sales s
        where
            s.product_id = p.id and
            s.salesperson = sp.id and 
            s.customer = c.id and
            p.size = 'S' and sp.age = 40 and c.city >= 'N'
        order by sid;
explain plan for
    select sid, s.quantity
        from product p, sales s, salesperson sp
        where
            s.sid < 3 and
            s.product_id = p.id and
            s.salesperson = sp.id and
            p.size >= 'M' and sp.age >= 30
        order by sid;
explain plan for
    select sid, c.id
        from sales s, state st, customer c
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York'
        order by sid;
-- shuffle the order of the tables in the from clause
explain plan for
    select sid, c.id
        from customer c, sales s, state st
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York'
        order by sid;

-- product can be removed because its join key with t can be obtained from
-- sales
explain plan for
    select sid, s.quantity, t.c
        from sales s, product p, t
        where
            s.product_id = p.id and p.size = 'S' and
            p.id = t.a
        order by sid;

-- LER-6330
explain plan for
    select s1.sid, s2.sid
        from product p1, sales s1, product p2, sales s2
        where s1.product_id = p1.id and p1.size = 'S' and
            s2.product_id = p2.id and p2.size = 'S';
explain plan for
    select s1.sid
        from sales s1, customer c1, sales s2, customer c2
        where s1.customer = c1.id and s2.customer = c2.id and c2.company = 'DEF'
            and c1.id = c2.id;

-- run the queries
!set outputformat table
select sid, p.id, s.quantity
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S'
    order by sid;
select sid
    from product p, sales s
    where
        s.product_id = p.id and p.size = 'S' and sid > 2
    order by sid;
select sid, p.id as pid, sp.id spid, s.quantity
    from sales s, product p, salesperson sp
    where
        p.id = s.product_id and
        s.salesperson = sp.id and
        p.size = 'M' and sp.age = 30
    order by sid;
select sid
    from customer c, salesperson sp, product p, sales s
    where
        s.product_id = p.id and
        s.salesperson = sp.id and 
        s.customer = c.id and
        p.size = 'S' and sp.age = 40 and c.city >= 'N'
    order by sid;
select sid, s.quantity
    from product p, sales s, salesperson sp
    where
        s.sid < 3 and
        s.product_id = p.id and
        s.salesperson = sp.id and
        p.size >= 'M' and sp.age >= 30
    order by sid;
select sid, c.id
    from sales s, state st, customer c
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York'
    order by sid;
select sid, c.id
    from customer c, sales s, state st
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York'
    order by sid;
select sid, s.quantity, t.c
    from sales s, product p, t
    where
        s.product_id = p.id and p.size = 'S' and
        p.id = t.a
    order by sid;
select s1.sid s1id, s2.sid s2id
    from product p1, sales s1, product p2, sales s2
    where s1.product_id = p1.id and p1.size = 'S' and
        s2.product_id = p2.id and p2.size = 'S'
    order by s1id, s2id;
select s1.sid
    from sales s1, customer c1, sales s2, customer c2
    where s1.customer = c1.id and s2.customer = c2.id and c2.company = 'DEF'
        and c1.id = c2.id
    order by sid;

-----------------------------------------
-- cases where the join cannot be removed
-----------------------------------------
!set outputformat csv

-- extra join filters
explain plan for select t.*
    from t inner join smalltable s
    on s.s1 = t.d and s.s3 = t.b and t.c = s.s4 where s.s2 > 0
    order by a;

-- semijoin keys not unique
explain plan for select t.*
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by a;

-- references to non-semijoin key column in projection list
explain plan for
    select *
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S'
        order by sid;
explain plan for
    select sid, p.name, s.quantity
        from sales s, product p
        where
            s.product_id = p.id and p.size = 'S'
        order by sid;

-- state can be removed because city can be retrieved from customer; note that
-- we can't remove customer instead of state because if we were to retrieve
-- city from state instead of customer, we'd also need the city column from
-- customer to join with state, resulting in neither join being removed
explain plan for
    select sid, st.city
        from sales s, state st, customer c
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York'
        order by sid;

-- neither customer or state can be removed because state is referenced in the
-- projection list; therefore customer needs to be joined with state in order
-- to retrieve the state column
explain plan for
    select sid, st.state
        from sales s, state st, customer c
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York'
        order by sid;

-- although customer doesn't need to be joined with either state or sales
-- because of semijoins with those two tables, it needs to be joined with t;
-- so its join can't be removed
explain plan for
    select sid
        from sales s, state st, customer c, t
        where
            s.customer = c.id and
            c.city = st.city and st.state = 'New York' and
            c.city = t.c
        order by sid;

!set outputformat table
select t.*
    from t inner join smalltable s
    on s.s1 = t.d and s.s3 = t.b and t.c = s.s4 where s.s2 > 0
    order by a;
select t.*
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by a;
select *
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S'
    order by sid;
select sid, p.name, s.quantity
    from sales s, product p
    where
        s.product_id = p.id and p.size = 'S'
    order by sid;
select sid, st.city
    from sales s, state st, customer c
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York'
    order by sid;
select sid, st.state
    from sales s, state st, customer c
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York'
    order by sid;
select sid
    from sales s, state st, customer c, t
    where
        s.customer = c.id and
        c.city = st.city and st.state = 'New York' and
        c.city = t.c
    order by sid;

------------
-- Misc Bugs
------------

-- LER-6865 -- a semijoin should not be used
create table le_org_party(org_party_key int primary key, dummy int);
create table le_sales_ordlns(cust_sold_to_key int, dummy int);
create index i_sales_ordlns on le_sales_ordlns(cust_sold_to_key);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'LE_ORG_PARTY', 6353);
call sys_boot.mgmt.stat_set_row_count(
    'LOCALDB', 'SJ', 'LE_SALES_ORDLNS', 2641498);
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'LE_ORG_PARTY', 'ORG_PARTY_KEY', 6353, 100, 6353, 0,
    '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'SJ', 'LE_SALES_ORDLNS', 'CUST_SOLD_TO_KEY', 5614, 100, 5614, 0,
    '0123456789');
!set outputformat csv
explain plan for
    select count(*) from le_sales_ordlns s, le_org_party o
    where s.cust_sold_to_key = o.org_party_key;

-- LER-6916 -- aggregation should be performed on SUPPLIER(S_NATIONKEY)
CREATE TABLE TPCHCUSTOMER ( C_CUSTKEY     INTEGER PRIMARY KEY,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       VARCHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  VARCHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);
CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER PRIMARY KEY,
                             S_NAME        VARCHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       VARCHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);
CREATE INDEX C_NATIONKEY_IDX ON TPCHCUSTOMER(C_NATIONKEY);
CREATE INDEX S_NATIONKEY_IDX ON SUPPLIER(S_NATIONKEY);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'TPCHCUSTOMER', 150000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'SUPPLIER', 10000);
explain plan for
    select * from
        tpchcustomer c, supplier s where
            c.c_nationkey = s.s_nationkey and
            s.s_name = 'foo';

-- LER-8251 -- In the resulting query plan, you should NOT see a cast of a
-- NULL literal to a NOT NULL type
CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER PRIMARY KEY,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    VARCHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  VARCHAR(15) NOT NULL,
                           O_CLERK          VARCHAR(15) NOT NULL,
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER PRIMARY KEY,
                            N_NAME       VARCHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));
CREATE TABLE REGION  ( R_REGIONKEY  INTEGER PRIMARY KEY,
                            R_NAME       VARCHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));
CREATE TABLE LINEITEM (
L_ORDERKEY    INTEGER,
L_PARTKEY     INTEGER NOT NULL,
L_SUPPKEY     INTEGER NOT NULL,
L_LINENUMBER  INTEGER,
L_QUANTITY    DECIMAL(15,2) NOT NULL,
L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
L_DISCOUNT    DECIMAL(15,2) NOT NULL,
L_TAX         DECIMAL(15,2) NOT NULL,
L_RETURNFLAG  VARCHAR(1) NOT NULL,
L_LINESTATUS  VARCHAR(1) NOT NULL,
L_SHIPDATE    DATE NOT NULL,
L_COMMITDATE  DATE NOT NULL,
L_RECEIPTDATE DATE NOT NULL,
L_SHIPINSTRUCT VARCHAR(25) NOT NULL, 
L_SHIPMODE    VARCHAR(10) NOT NULL,
L_COMMENT      VARCHAR(44) NOT NULL,
PRIMARY KEY(L_ORDERKEY, L_LINENUMBER)
);
CREATE TABLE PART  ( P_PARTKEY     INTEGER PRIMARY KEY,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        VARCHAR(25) NOT NULL,
                          P_BRAND       VARCHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   VARCHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );
create index O_CUSTKEY_IDX on ORDERS(O_CUSTKEY);
create index N_REGIONKEY_IDX on NATION(N_REGIONKEY);
create index L_PARTKEY_IDX on LINEITEM(L_PARTKEY);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'ORDERS', 1500000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'NATION', 250);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'REGION', 50);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'LINEITEM', 6000000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'SJ', 'PART', 200000);
explain plan for 
select count(*) from orders, tpchcustomer, nation, region, lineitem, part
    where o_custkey = c_custkey and c_nationkey = n_nationkey and
        n_regionkey = r_regionkey and r_name = 'AMERICA' and
        O_ORDERDATE BETWEEN DATE'1995-01-01' AND DATE'1996-12-31' and 
        l_partkey = p_partkey and p_type = 'ECONOMY ANODIZED STEEL';
