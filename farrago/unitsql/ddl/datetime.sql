-- $Id$
-- test datetime ddl.

create schema tempo;
set schema 'tempo';

create table datetime1(
    col1 int not null primary key,
    datecol date default date '2002-05-23',
    timecol time default time '12:09:22',
    timestampcol timestamp default timestamp '2002-05-23 12:09:22');

-- real values
insert into datetime1(col1,datecol,timecol,timestampcol) 
values(0, DATE '2004-12-21', TIME '12:22:33', TIMESTAMP '2004-12-21 12:22:33');

-- defaults
insert into datetime1(col1) values(1);

select * from datetime1 order by col1;

delete from datetime1;

select * from datetime1;

