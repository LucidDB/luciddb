--
-- Bug 1843
-- Owner: Jonathan
-- Abstract: The third insert to the below table gives an internal error at "xovibuilder".
--
create schema bugs;
set schema 'bugs';

CREATE TABLE WORKS
(EMPNUM   CHAR(3) NOT NULL,
PNUM     CHAR(3) NOT NULL,
HOURS    DECIMAL(5),
UNIQUE(EMPNUM,PNUM));

insert into works values('E9', 'P9', NULL);
select * from works;

delete from works;
select * from works;

insert into works values('E9', 'P9', NULL);
select * from works;

delete from works;
select * from works;

insert into works values('E9', 'P9', NULL);
select * from works;
