--Test ReadRowsFromFileUDX

--Test case.

create schema rrffudx;
set schema 'rrffudx';

create table MyTestTb(

id int,
name varchar(255),
birthday date,
is_married boolean


);

insert into rrffudx.MyTestTb values(1,'ray',DATE'1983-12-25',FALSE);
insert into rrffudx.MyTestTb values(2,'john',DATE'1983-1-17',FALSE);
insert into rrffudx.MyTestTb values(3,'mike',DATE'1983-1-17',TRUE);
insert into rrffudx.MyTestTb values(4,'tom',DATE'1983-1-17',FALSE);
insert into rrffudx.MyTestTb values(5,'jim',DATE'1983-1-17',FALSE);

select * from rrffudx.MyTestTb;
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyTestTb),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE));
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyTestTb),'file:///tmp/NON_GZIP.rrffudx.MyTestTb.dat',FALSE));

--Test header/cursor mismatches (verify checking) 
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE)
);

select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(50)) as name,
     cast(null as date) as birthday, cast(null as int) as is_married
     from (values(0))
    ),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE)
);
--Verify proper errors (and reporting) from abrupt end of file.
--Create file on classpath (ie, in a jar) and verify you're able to read from a classpath resource file in addition to a File URL 
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyTestTb),'classpath:///com/GZIP.rrffudx.MyTestTb.dat',TRUE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'classpath:///com/GZIP.rrffudx.MyTestTb.dat',TRUE)
);

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyTestTb),'classpath:///com/NON_GZIP.rrffudx.MyTestTb.dat',FALSE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'classpath:///com/NON_GZIP.rrffudx.MyTestTb.dat',FALSE)
);

--Verify reading from a file with 0 row works (ie, produces 0 rows without an error) 
select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyTestTb where 1=2),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE)
);

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyTestTb where 1=2),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',FALSE));
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',FALSE)
);

--Test reading using gzip true to ungzipped file gives error. 
select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/NON_GZIP.rrffudx.MyTestTb.dat',TRUE)
);

select * from table(
  APPLIB.READ_ROWS_FROM_FILE(cursor(
     select cast(null as int) as id, cast(null as varchar(255)) as name,
     cast(null as date) as birthday, cast(null as boolean) as is_married
     from (values(0))
    ),'file:///tmp/NON_GZIP.rrffudx.MyTestTb.dat',FALSE)
);

--Test read performance of larger dataset (200k+ rows) 
create table MyFiscalTimeTb(

TIME_KEY_SEQ int,
TIME_KEY date

);

insert into rrffudx.MyFiscalTimeTb
select TIME_KEY_SEQ ,TIME_KEY from table(APPLIB.FISCAL_TIME_DIMENSION(2010,1,1,2050,12,31,1));

insert into rrffudx.MyFiscalTimeTb
select * from rrffudx.MyFiscalTimeTb;

select * from table(APPLIB.WRITE_ROWS_TO_FILE(cursor(select * from rrffudx.MyFiscalTimeTb),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE));

select count(1) from rrffudx.MyFiscalTimeTb;

select count(1) from table(
  APPLIB.READ_ROWS_FROM_FILE(
    cursor(
      select cast(null as int) as TIME_KEY_SEQ, 
      cast(null as date) as TIME_KEY 
      from (values(0)
  )
  ),'file:///tmp/GZIP.rrffudx.MyTestTb.dat',TRUE)
);

drop table MyFiscalTimeTb cascade;
drop table MyTestTb cascade;
drop schema rrffudx cascade;


