-- $Id$
-- Tests for Firewater remote query optimization

create server remote_node1
foreign data wrapper sys_firewater_remote_wrapper
options(
user_name 'sa',
url 'jdbc:firewater_storage:remote:rmi://localhost:5434'
);

create server remote_node2
foreign data wrapper sys_firewater_remote_wrapper
options(
user_name 'sa',
url 'jdbc:firewater_storage:remote:rmi://localhost:5434'
);

create partition rp1 on (remote_node1);

create partition rp2 on (remote_node2);

create schema m;

create table m.t1(i int, j int);

!set outputformat csv

-- test basic table access
explain plan for select * from m.t1;

-- test projection pushdown through union
explain plan for select i from m.t1;

-- test filter pushdown through union
explain plan for select i from m.t1 where j > 3;

-- test GROUP BY pushdown through union
explain plan for select i,sum(j),count(*) from m.t1 group by i;

-- test GROUP BY with AVG
explain plan for select i,avg(j) from m.t1 group by i;

-- test GROUP BY with DISTINCT COUNT
explain plan for select i,count(distinct j), sum(j) from m.t1 group by i;

-- test pushdown of GROUP BY with filter
explain plan for select i,sum(j) from m.t1 where i > 100 group by i;
