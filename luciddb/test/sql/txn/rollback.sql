-- $Id$
-- Tests LucidDB support for implicit rollback on error

create schema s;
create table s.t(i int not null);

create foreign table s.mock_fennel_table(
    id int not null)
server sys_mock_foreign_data_server
options (executor_impl 'FENNEL', row_count '3000');

-- run a load which will fail on the last row
-- due to division by zero
insert into s.t
select * from s.mock_fennel_table
union all
values (1/0);

-- verify that table contents were rolled back
select count(*) from s.t;

-- run a load which should succeed
insert into s.t
select * from s.mock_fennel_table;

-- verify that table contents were updated
select count(*) from s.t;

-- run an incremental load which will fail on the last row
-- due to division by zero
insert into s.t
select * from s.mock_fennel_table
union all
values (1/0);

-- verify that table contents were rolled back
select count(*) from s.t;
