-- Test for WriteRowsToFileUDX.java
create schema wrtfudx;
set schema 'wrtfudx';

create table MyTestTb(

id int,
name varchar(255),
birthday date,
is_married boolean

);

insert into wrtfudx.MyTestTb values(1,'ray',DATE'1983-12-25',FALSE);
insert into wrtfudx.MyTestTb values(2,'john',DATE'1983-1-17',FALSE);
insert into wrtfudx.MyTestTb values(3,'mike',DATE'1983-1-17',TRUE);
insert into wrtfudx.MyTestTb values(4,'tom',DATE'1983-1-17',FALSE);
insert into wrtfudx.MyTestTb values(5,'jim',DATE'1983-1-17',FALSE);

select count(1) from wrtfudx.MyTestTb;
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb),'file:///tmp/GZIP.wrtfudx.MyTestTb.dat',TRUE));
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb),'file:///tmp/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE));

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb),'classpath:///com/lucidera/GZIP.wrtfudx.MyTestTb.dat',TRUE));
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb),'classpath:///com/lucidera/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE));

--Verify a zero row file is written, and reads properly with READ_ROWS_FROM_FILE 

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb where 1=2),'file:///tmp/GZIP.wrtfudx.MyTestTb.dat',TRUE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/GZIP.wrtfudx.MyTestTb.dat',TRUE)
);

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb where 1=2),'file:///tmp/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE)
);

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb where 1=2),'classpath:///com/lucidera/GZIP.wrtfudx.MyTestTb.dat',TRUE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'classpath:///com/lucidera/GZIP.wrtfudx.MyTestTb.dat',TRUE)
);

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb where 1=2),'classpath:///com/lucidera/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'classpath:///com/lucidera/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE)
);


--Verify that writing to a malformed URL throws an exception (bad file location) 

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb),'file:///tmp1/GZIP.wrtfudx.MyTestTb.dat',TRUE));
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyTestTb),'classpath:///com/lucidera1/GZIP.wrtfudx.MyTestTb.dat',TRUE));

--Test creating a large, big file. Consider using fiscal time dimension applib extension as source.

create table MyFiscalTimeTb(

TIME_KEY_SEQ int,
TIME_KEY date

);

insert into wrtfudx.MyFiscalTimeTb
select TIME_KEY_SEQ ,TIME_KEY from table(APPLIB.FISCAL_TIME_DIMENSION(2010,1,1,2050,12,31,1));

insert into wrtfudx.MyFiscalTimeTb
select * from wrtfudx.MyFiscalTimeTb;

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyFiscalTimeTb),'file:///tmp/GZIP.wrtfudx.MyTestTb.dat',TRUE));

select count(1) from wrtfudx.MyFiscalTimeTb;

select count(1) from table(
  APPLIB.READ_ROWS_FROM_FILE(
    cursor(
      select cast(null as int) as TIME_KEY_SEQ, 
      cast(null as date) as TIME_KEY 
      from (values(0)
  )
  ),'file:///tmp/GZIP.wrtfudx.MyTestTb.dat',TRUE)
);

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from wrtfudx.MyFiscalTimeTb),'classpath:///com/lucidera/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE));
select count(1) from table(
  APPLIB.READ_ROWS_FROM_FILE(
    cursor(
      select cast(null as int) as TIME_KEY_SEQ, 
      cast(null as date) as TIME_KEY 
      from (values(0)
  )
  ),'classpath:///com/lucidera/NON_GZIP.wrtfudx.MyTestTb.dat',FALSE)
);

drop table MyFiscalTimeTb cascade;
drop table MyTestTb cascade;
drop schema wrtfudx cascade;


