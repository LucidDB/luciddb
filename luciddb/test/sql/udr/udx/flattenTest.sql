create schema flatten;

set schema 'flatten';

--------------------
-- positive tests --
--------------------

-- simple tree (no multiple path) ------------------------------------

create table tree1(parent varchar(128), child varchar(128));

insert into tree1 values
    ('01','02'),
    ('01','03'),
    ('01','04'),
    ('02','05'),
    ('02','06'),
    ('07','08'),
    ('08','09'),
    ('08','10'),
    ('09','11'),
    ('11','12'),
    ('11','13')
;

select * from table(applib.flatten(cursor(select * from tree1)));


-- graph with multiple paths -----------------------------------------

create table mgraph(parent varchar(128), child varchar(128));

insert into mgraph values
    ('01','02'),
    ('01','03'),
    ('02','04'),
    ('03','04'),
    ('04','05'),
    ('04','06'),
    ('05','07'),
    ('06','07'),
    ('01','07'),
    ('01','08')
;

select * from table(applib.flatten(cursor(select * from mgraph)));

truncate table mgraph;

insert into mgraph values
    ('01','04'),
    ('00','04'),
    ('01','02'),
    ('02','03'),
    ('03','04'),
    ('04','05'),
    ('06','04'),
    ('02','06'),
    ('01','07'),
    ('07','08'),
    ('07','09'),
    ('05','10'),
    ('05','11'),
    ('09','11'),
    ('11','12'),
    ('11','13'),
    ('10','12'),
    ('12','14')
;

select * from table(applib.flatten(cursor(select * from mgraph)));


-- input table has null values at col 1 for root nodes----------------

create table nullval(c1 varchar(10), c2 varchar(10));

insert into nullval values
    (null, '00'),
    (null, '01'),
    ('02', '03'),
    (null, '02'),
    ('02', '04'),
    ('03', '04'),
    ('04', '05'),
    ('04', '06'),
    ('02', '07'),
    ('07', '08'),
    ('08', '09'),
    ('09', '10'),
    ('07', '11'),
    ('11', '12')
;

select * from table (applib.flatten(cursor(select * from nullval)));

truncate table nullval;

insert into nullval values 
    ('01','04'),
    (null, '00'),
    ('00','04'),
    ('01','02'),
    ('02','03'),
    (null, '01'),
    ('03','04'),
    ('04','05'),
    ('06','04'),
    ('02','06'),
    ('01','07'),
    ('07','08'),
    ('07','09'),
    ('05','10'),
    ('05','11'),
    ('09','11'),
    ('11','12'),
    ('11','13'),
    ('10','12'),
    ('12','14')
;

select * from table (applib.flatten(cursor(select * from nullval)));

-- input table has non-string type -----------------------------------

create table typeint(parent int, child int);

insert into typeint values
    (null,1),
    (null,2),
    (2,3),
    (2,4),
    (2,5),
    (5,6),
    (2,6),
    (6,7)
;

select * from table (applib.flatten(cursor(select * from typeint)));

create table typedouble(parent double, child double);

insert into typedouble values
    (null,1.0),
    (null,2.0),
    (2.0,3.0),
    (2.0,4.0),
    (2.0,5.0),
    (5.0,6.0),
    (2.0,6.0),
    (6.0,7.0)
;

select * from table (applib.flatten(cursor(select * from typedouble)));

create table typetimestamp(parent timestamp, child timestamp);

insert into typetimestamp values
    (null,timestamp'2002-01-01 01:56:00'),
    (null,timestamp'2002-01-02 01:56:00'),
    (timestamp'2002-01-02 01:56:00',timestamp'2002-01-03 01:56:00'),
    (timestamp'2002-01-02 01:56:00',timestamp'2002-01-04 01:56:00'),
    (timestamp'2002-01-02 01:56:00',timestamp'2002-01-05 01:56:00'),
    (timestamp'2002-01-05 01:56:00',timestamp'2002-01-06 01:56:00'),
    (timestamp'2002-01-02 01:56:00',timestamp'2002-01-06 01:56:00'),
    (timestamp'2002-01-06 01:56:00',timestamp'2002-01-07 01:56:00')
;

select * from table (applib.flatten(cursor(select * from typetimestamp)));

-- clean up ----------------------------------------------------------

drop table tree1 cascade;
drop table mgraph cascade;
drop table typeint;
drop table typetimestamp;
drop table typedouble;


--------------------
-- negative tests --
--------------------

-- loop --------------------------------------------------------------

create table loop(parent varchar(128), child varchar(128));

insert into loop values 
    ('1','2'),
    ('2','1')
;

select * from table(applib.flatten(cursor(select * from loop)));

truncate table loop;

insert into loop values
    ('1','2'),
    ('2','3'),
    ('3','1')
;

select * from table(applib.flatten(cursor(select * from loop)));

truncate table loop;

insert into loop values
    ('1','2'),
    ('2','3'),
    ('3','2')
;

select * from table(applib.flatten(cursor(select * from loop)));


-- tree deeper than maxDepth------------------------------------------

create table deep(parent varchar(128), child varchar(128));

insert into deep values
    ('a','b'),
    ('a','c'),
    ('b','c'),
    ('1','2'),
    ('2','3'),
    ('3','4'),
    ('4','5'),
    ('5','6'),
    ('6','7'),
    ('7','8'),
    ('8','9'),
    ('9','10'),
    ('10','11'),
    ('11','12'),
    ('12','13'),
    ('13','14'),
    ('14','15'),
    ('15','16')
;

select * from table(applib.flatten(cursor(select * from deep)));

create view dpview as
    select * from table(applib.flatten(cursor(select * from deep)));
select * from dpview;


-- input table does not have 2 columns -------------------------------

create table threecol(c1 varchar(10), c2 varchar(10), c3 varchar(10));

select * from table (applib.flatten(cursor(select * from threecol)));


-- clean up ----------------------------------------------------------

drop table loop cascade;
drop table deep cascade;
drop table threecol cascade;