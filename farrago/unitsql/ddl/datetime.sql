-- $Id$
-- test datetime ddl.
set schema sales;

insert into datetime1(col1,datecol,timecol,timestampcol) values(0, DATE '2004-12-21', TIME '12:22:33', TIMESTAMP '2004-12-21 12:22:33');

select * from datetime1;

delete from datetime1;

