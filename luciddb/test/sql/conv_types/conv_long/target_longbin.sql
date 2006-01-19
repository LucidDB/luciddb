set schema 's'
;

create table tlongb (l long varbinary)
;

-- all numeric datatypes should FAIL
-- insert into tlongb select colbit from datatype_source
-- ;
insert into tlongb select coltiny from datatype_source
;
insert into tlongb select colsmall from datatype_source
;
insert into tlongb select colint from datatype_source
;
insert into tlongb select colbig from datatype_source
;
-- insert into tlongb select coldec from datatype_source
-- ;
-- insert into tlongb select colnum from datatype_source
-- ;
insert into tlongb select coldouble from datatype_source
;
insert into tlongb select colfloat from datatype_source
;
insert into tlongb select colreal from datatype_source
;

-- char datatypes should produce warnings, LONG VARCHAR should FAIL
insert into tlongb REPORT AT MOST 0 FAILURES
 select colchar from datatype_source
;
insert into tlongb REPORT AT MOST 0 FAILURES
 select colvchar from datatype_source
;
insert into tlongb REPORT AT MOST 0 FAILURES
 select collchar from datatype_source
;

-- all binary datatypes should be OK
insert into tlongb select colbin from datatype_source
;
insert into tlongb select colvbin from datatype_source
;
insert into tlongb select collbin from datatype_source
;

select count(*) from tlongb
;
