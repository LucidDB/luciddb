set schema 's'
;

create table target_time(coltime time)
;

--
-- BAD
--

-- all numeric should FAIL
--insert into target_time
-- select colbit from datatype_source where colname = 'BAD'
--;
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
--insert into target_time
-- select coldec from datatype_source where colname = 'BAD'
--;
--insert into target_time
-- select colnum from datatype_source where colname = 'BAD'
--;

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
-- will be fixed with FRG-22
--insert into target_time
-- select colchar from datatype_source where colname = 'BAD'
--;
--insert into target_time
-- select colvchar from datatype_source where colname = 'BAD'
--;
--insert into target_time
-- select colbin from datatype_source where colname = 'BAD'
--;
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
-- will be fixed by FRG-20
insert into target_time
 select coltmstamp from datatype_source where colname = 'BAD'
;

-- all should FAIL
-- implicit converstion fron a timestamp string not supported
insert into target_time
 select colchar from datatype_source where colname = 'TIME'
;
insert into target_time
 select colvchar from datatype_source where colname = 'TIME'
;

--
-- GOOD
--

-- should be OK
insert into target_time
 select coltime from datatype_source where colname = 'TIME'
;
-- will be fixed in FRG-20
--insert into target_time
-- select coltmstamp from datatype_source where colname = 'TIME'
--;

select * from target_time order by 1
;
