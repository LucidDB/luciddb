set schema 's'
;

create table target_timestamp(coltimestamp timestamp)
;

--
-- BAD
--

-- all numeric should FAIL
insert into target_timestamp
 select colbit from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select coltiny from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colsmall from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colint from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colbig from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select coldec from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colnum from datatype_source where colname = 'BAD'
;

-- all floating point should FAIL
insert into target_timestamp
 select coldouble from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colfloat from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colreal from datatype_source where colname = 'BAD'
;

-- all char/binary should FAIL
insert into target_timestamp
 select colchar from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select colvchar from datatype_source where colname = 'BAD'
;
--insert into target_timestamp
-- select colbin from datatype_source where colname = 'BAD'
--;
insert into target_timestamp
 select colvbin from datatype_source where colname = 'BAD'
;

-- all should be OK
insert into target_timestamp
 select coltime from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select coldate from datatype_source where colname = 'BAD'
;
insert into target_timestamp
 select coltmstamp from datatype_source where colname = 'BAD'
;


--
-- GOOD
--

-- should be FAIL
insert into target_timestamp
 select colchar from datatype_source where colname = 'TIMESTAMP'
;
insert into target_timestamp
 select colvchar from datatype_source where colname = 'TIMESTAMP'
;


-- should be OK
-- FRG-20
insert into target_timestamp
 select cast (coltime as timestamp) from datatype_source where colname = 'TIMESTAMP'
;
-- FRG-20
insert into target_timestamp
 select cast (coldate as timestamp) from datatype_source where colname = 'TIMESTAMP'
;
insert into target_timestamp
 select coltmstamp from datatype_source where colname = 'TIMESTAMP'
;


select * from target_timestamp order by 1
;
