set schema 's'
;

create table tlongc (l long varchar)
;

-- all numeric datatypes should be OK
-- insert into tlongc select colbit from datatype_source
-- ;
insert into tlongc select coltiny from datatype_source
;
insert into tlongc select colsmall from datatype_source
;
insert into tlongc select colint from datatype_source
;
insert into tlongc select colbig from datatype_source
;
-- insert into tlongc select coldec from datatype_source
-- ;
-- insert into tlongc select colnum from datatype_source
-- ;
insert into tlongc select coldouble from datatype_source
;
insert into tlongc select colfloat from datatype_source
;
insert into tlongc select colreal from datatype_source
;

-- all char datatypes should be OK
insert into tlongc select colchar from datatype_source
;
insert into tlongc select colvchar from datatype_source
;
insert into tlongc select collchar from datatype_source
;

-- all binary datatypes except for LONG VARBINARY should be OK
insert into tlongc select colbin from datatype_source
;
insert into tlongc select colvbin from datatype_source
;
insert into tlongc select collbin from datatype_source
;

select * from tlongc
;
