set schema 's'
;

create table target_time(coltime time primary key)
;

--
-- BAD
--

-- all numeric should FAIL
insert into target_time
 select colbit from datatype_source where colname = 'BAD'
;
insert into target_time
 select coltiny from datatype_source where colname = 'BAD'
;
insert into target_time
 select colsmall from datatype_source where colname = 'BAD'
;
insert into target_time
 select colint from datatype_source where colname = 'BAD'
;
insert into target_time
 select colbig from datatype_source where colname = 'BAD'
;
insert into target_time
 select coldec from datatype_source where colname = 'BAD'
;
insert into target_time
 select colnum from datatype_source where colname = 'BAD'
;

-- all floating point should FAIL
insert into target_time
 select coldouble from datatype_source where colname = 'BAD'
;
insert into target_time
 select colfloat from datatype_source where colname = 'BAD'
;
insert into target_time
 select colreal from datatype_source where colname = 'BAD'
;

-- all char/binary should FAIL
insert into target_time
 select colchar from datatype_source where colname = 'BAD'
;
insert into target_time
 select colvchar from datatype_source where colname = 'BAD'
;
insert into target_time
 select colbin from datatype_source where colname = 'BAD'
;
insert into target_time
 select colvbin from datatype_source where colname = 'BAD'
;

-- only the second should FAIL
insert into target_time
 select coltime from datatype_source where colname = 'BAD'
;
insert into target_time
 select coldate from datatype_source where colname = 'BAD'
;
insert into target_time
 select coltmstamp from datatype_source where colname = 'BAD'
;


--
-- GOOD
--

-- should be OK
insert into target_time
 select colchar from datatype_source where colname = 'TIME'
;
insert into target_time
 select colvchar from datatype_source where colname = 'TIME'
;


-- should be OK
insert into target_time
 select coltime from datatype_source where colname = 'TIME'
;
insert into target_time
 select coltmstamp from datatype_source where colname = 'TIME'
;

select * from target_time
;
