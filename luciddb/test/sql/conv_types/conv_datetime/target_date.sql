set schema 's'
;

create table target_date(coldate date)
;

--
-- BAD
--

-- all numeric should FAIL
insert into target_date 
 select colbit from datatype_source where colname = 'BAD'
;
insert into target_date
 select coltiny from datatype_source where colname = 'BAD'
;
insert into target_date
 select colsmall from datatype_source where colname = 'BAD'
;
insert into target_date
 select colint from datatype_source where colname = 'BAD'
;
insert into target_date
 select colbig from datatype_source where colname = 'BAD'
;
insert into target_date
 select coldec from datatype_source where colname = 'BAD'
;
insert into target_date
 select colnum from datatype_source where colname = 'BAD'
;

-- all floating point should FAIL
insert into target_date
 select coldouble from datatype_source where colname = 'BAD'
;
insert into target_date
 select colfloat from datatype_source where colname = 'BAD'
;
insert into target_date
 select colreal from datatype_source where colname = 'BAD'
;

-- all char/binary should FAIL
insert into target_date
 select colchar from datatype_source where colname = 'BAD'
;
insert into target_date
 select colvchar from datatype_source where colname = 'BAD'
;
--insert into target_date
-- select colbin from datatype_source where colname = 'BAD'
--;
insert into target_date
 select colvbin from datatype_source where colname = 'BAD'
;

-- only the first should FAIL
insert into target_date
 select coltime from datatype_source where colname = 'BAD'
;
insert into target_date
 select coldate from datatype_source where colname = 'BAD'
;
insert into target_date
 select cast (coltmstamp as date) from datatype_source where colname = 'BAD'
;


--
-- GOOD
--

-- should be OK
-- FRG-20
insert into target_date
 select cast (cast (colchar as timestamp) as date) from datatype_source where colname = 'DATE'
;
insert into target_date
 select cast (cast (colvchar as timestamp) as date) from datatype_source where colname = 'DATE'
;


-- should be OK
insert into target_date
 select coldate from datatype_source where colname = 'DATE'
;
-- FRG-20
insert into target_date
 select cast (coltmstamp as date) from datatype_source where colname = 'DATE'
;

select * from target_date order by 1
;
