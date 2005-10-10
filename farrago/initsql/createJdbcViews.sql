-- $Id$
-- This script creates a view schema used by JDBC metadata calls

!set verbose true
!autocommit off

-- create views in system-owned schema sys_boot.jdbc_metadata
create schema sys_boot.jdbc_metadata;
set schema 'sys_boot.jdbc_metadata';
set path 'sys_boot.jdbc_metadata';

create function null_identifier()
returns varchar(128)
contains sql
deterministic
return cast(null as varchar(128));

create function null_remarks()
returns varchar(1024)
contains sql
deterministic
return cast(null as varchar(1024));

create function convert_cwm_nullable_to_int(cwm_nullability varchar(20))
returns integer
contains sql
deterministic
return case
when cwm_nullability='columnNoNulls' then 0
when cwm_nullability='columnNullable' then 1
else 2
end;

create function convert_cwm_nullable_to_string(cwm_nullability varchar(20))
returns varchar(3)
contains sql
deterministic
return case
when cwm_nullability='columnNoNulls' then 'NO'
when cwm_nullability='columnNullable' then 'YES'
else ''
end;

create function convert_cwm_param_kind_to_int(cwm_param_kind varchar(10))
returns smallint
contains sql
deterministic
return case
when cwm_param_kind='pdk_in' then 1
when cwm_param_kind='pdk_inout' then 2
when cwm_param_kind='pdk_out' then 4
when cwm_param_kind='pdk_return' then 5
else 0
end;

create function convert_cwm_typename_to_literal_prefix(typename varchar(128))
returns varchar(128)
contains sql
deterministic
return case
when typename='VARCHAR' then trim('''')
when typename='CHAR' then ''''
when typename='VARBINARY' then 'X'''
when typename='BINARY' then 'X'''
when typename='DATE' then 'DATE '''
when typename='TIME' then 'TIME '''
when typename='TIMESTAMP' then 'TIMESTAMP '''
else cast(null as varchar(128))
end;

create function convert_cwm_typename_to_literal_suffix(typename varchar(128))
returns varchar(128)
contains sql
deterministic
return case
when typename='VARCHAR' then trim('''')
when typename='CHAR' then ''''
when typename='VARBINARY' then ''''
when typename='BINARY' then ''''
when typename='DATE' then ''''
when typename='TIME' then ''''
when typename='TIMESTAMP' then ''''
else cast(null as varchar(128))
end;

-- NOTE:  don't include ORDER BY in the view definitions

create view schemas_view_internal as
    select 
        c."name" as object_catalog,
        s."name" as object_schema,
        s."mofId"
    from 
        sys_cwm."Relational"."Catalog" c
    inner join
        sys_cwm."Relational"."Schema" s
    on
        c."mofId" = s."namespace"
;

create view tables_view_internal as
    select 
        s.object_catalog as table_cat,
        s.object_schema as table_schem,
        t."name" as table_name,
        null_remarks() as remarks,
        null_identifier() as type_cat,
        null_identifier() as type_schem,
        null_identifier() as type_name,
        null_identifier() as self_referencing_col_name,
        null_identifier() as ref_generation,
        t."mofId",
        t."mofClassName"
    from
        schemas_view_internal s
    inner join
        sys_cwm."Relational"."NamedColumnSet" t
    on
        s."mofId" = t."namespace"
;

create view catalogs_view as
    select 
        c."name" as table_cat
    from 
        sys_cwm."Relational"."Catalog" c
;
grant select on catalogs_view to public;

create view schemas_view as
    select 
        object_schema as table_schem,
        object_catalog as table_catalog
    from 
        schemas_view_internal
;
grant select on schemas_view to public;

-- TODO:  add 'GLOBAL TEMPORARY' and 'SYSTEM TABLE'
create view table_types_view_internal(table_type,uml_class_name) as
    values 
        (trim('FOREIGN TABLE'),trim('ForeignTable')),
        ('TABLE','LocalTable'),
        ('VIEW','LocalView')
;

create view tables_view as
    select 
        table_cat,
        table_schem,
        table_name,
        tt.table_type,
        remarks,
        type_cat,
        type_schem,
        type_name,
        self_referencing_col_name,
        ref_generation
    from
        tables_view_internal t,
        table_types_view_internal tt
    where
        t."mofClassName"=tt.uml_class_name
;
grant select on tables_view to public;

create view table_types_view as
    select distinct
        table_type
    from
        table_types_view_internal
;
grant select on table_types_view to public;

-- TODO: get column_def by left-outer-join to get default value
-- TODO: get source_data_type for distinct types
-- TODO: get predefined numericPrecision and numericPrecisionRadix
-- REVIEW: length/precision/scale/radix
-- also all of above for attributes and procedure_columns

create view columns_view_internal as
    select 
        t.table_cat,
        t.table_schem,
        t.table_name,
        c."name" as column_name,
        c."type",
        coalesce(c."length",c."precision") as column_size,
        0 as buffer_len,
        c."scale" as dec_digits,
        convert_cwm_nullable_to_int(c."isNullable") as nullable,
        c."length" as char_octet_length,
        c."ordinal" + 1 as ordinal_position,
        convert_cwm_nullable_to_string(c."isNullable") as is_nullable,
        c."mofId"
    from 
        tables_view_internal t 
    inner join 
        sys_fem."SQL2003"."AbstractColumn" c 
    on 
        t."mofId" = c."owner";

create view columns_view as
    select 
        c.table_cat,
        c.table_schem,
        c.table_name,
        c.column_name,
        t."typeNumber" as data_type,
        t."name" as type_name,
        c.column_size,
        c.buffer_len,
        c.dec_digits,
        cast(null as integer) as num_prec_radix,
        c.nullable,
        null_remarks() as remarks,
        null_remarks() as column_def,
        0 as sql_data_type,
        0 as sql_datetime_sub,
        c.char_octet_length,
        c.ordinal_position,
        c.is_nullable,
        null_identifier() as scope_catalog,
        null_identifier() as scope_schema,
        null_identifier() as scope_table,
        null_identifier() as source_data_type
    from 
        columns_view_internal c 
    inner join 
        sys_cwm."Relational"."SQLDataType" t 
    on 
        c."type" = t."mofId";
grant select on columns_view to public;

create view udts_view_internal as
    select
        s.object_catalog as type_cat,
        s.object_schema as type_schem,
        u."name" as type_name,
        u."typeNumber" as data_type,
        u."mofId"
    from
        schemas_view_internal s
    inner join
        sys_fem."SQL2003"."UserDefinedType" u
    on
        s."mofId" = u."namespace"
;

-- TODO:  base_type for distinct types

create view udts_view as
    select
        u.type_cat,
        u.type_schem,
        u.type_name,
        null_identifier() as class_name,
        u.data_type,
        null_remarks() as remarks,
        cast (null as smallint) as base_type
    from
        udts_view_internal u
;
grant select on udts_view to public;

create view attributes_view_internal as
    select
        u.type_cat,
        u.type_schem,
        u.type_name,
        a."name" as attr_name,
        coalesce(a."length",a."precision") as attr_size,
        a."scale" as decimal_digits,
        convert_cwm_nullable_to_int(a."isNullable") as nullable,
        a."length" as char_octet_length,
        a."ordinal" + 1 as ordinal_position,
        convert_cwm_nullable_to_string(a."isNullable") as is_nullable,
        a."type",
        a."mofId"
    from
        udts_view_internal u
    inner join
        sys_fem."SQL2003"."SQLTypeAttribute" a
    on
        u."mofId" = a."owner"
;

create view attributes_view as
    select
        a.type_cat,
        a.type_schem,
        a.type_name,
        a.attr_name,
        t."typeNumber" as data_type,
        t."name" as attr_type_name,
        a.attr_size,
        a.decimal_digits,
        cast(null as integer) as num_prec_radix,
        a.nullable,
        null_remarks() as remarks,
        null_remarks() as attr_def,
        0 as sql_data_type,
        0 as sql_datetime_sub,
        a.char_octet_length,
        a.ordinal_position,
        a.is_nullable,
        null_identifier() as scope_catalog,
        null_identifier() as scope_schema,
        null_identifier() as scope_table,
        null_identifier() as source_data_type
    from
        attributes_view_internal a
    inner join
        sys_cwm."Relational"."SQLDataType" t 
    on
        a."type" = t."mofId"
;
grant select on attributes_view to public;

-- TODO:  find out why replacing BehavioralFeature below with Method or
-- Routine doesn't work (causes assignment of null to NOT NULL).  Must be
-- a bug in Farrago's MDR namespace support.

create view procedures_view_internal as
    select
        s.object_catalog as procedure_cat,
        s.object_schema as procedure_schem,
        r."name" as procedure_name,
        r."mofId"
    from
        schemas_view_internal s
    inner join
        sys_cwm."Behavioral"."BehavioralFeature" r
    on
        s."mofId" = r."namespace"
;

-- TODO:  other values for procedure_type once we support procedures
-- that return result sets

create view procedures_view as
    select
        p.procedure_cat,
        p.procedure_schem,
        p.procedure_name,
        null_identifier() as reserved1,
        null_identifier() as reserved2,
        null_identifier() as reserved3,
        null_remarks() as remarks,
        cast(1 as smallint) as procedure_type
    from
        procedures_view_internal p
;
grant select on procedures_view to public;

create view procedure_columns_view_internal as
    select
        p.procedure_cat,
        p.procedure_schem,
        p.procedure_name,
        rp."name" as column_name,
        convert_cwm_param_kind_to_int(rp."kind") as column_type,
        rp."precision" as "PRECISION",
        rp."length" as length,
        rp."scale" as scale,
        case when rp."kind"='pdk_return' then 0
             else rp."ordinal" + 1 end as column_ordinal,
        rp."type",
        rp."mofId"
    from
        procedures_view_internal p
    inner join
        sys_fem."SQL2003"."RoutineParameter" rp
    on
        p."mofId" = rp."behavioralFeature"
;

create view procedure_columns_view as
    select
        pc.procedure_cat,
        pc.procedure_schem,
        pc.procedure_name,
        pc.column_name,
        pc.column_type,
        t."typeNumber" as data_type,
        t."name" as type_name,
        pc."PRECISION",
        pc.length,
        pc.scale,
        cast(null as integer) as radix,
        cast(1 as smallint) as nullable,
        null_remarks() as remarks,
        pc.column_ordinal
    from
        procedure_columns_view_internal pc
    inner join
        sys_cwm."Relational"."SQLDataType" t 
    on 
        pc."type" = t."mofId"
;
grant select on procedure_columns_view to public;

-- TODO:  refine precision, case_sensitive, searchable, minimum/maximum_scale
-- as we add LOB, NUMERIC, and UNICODE data types; unsigned should
-- be null for non-numerics; case_sensitive should be null for
-- non-character; support create_params

create view type_info_view as
    select
        t."name" as type_name,
        t."typeNumber" as data_type,
        coalesce(
            t."numericPrecision",
            t."characterMaximumLength",
            t."dateTimePrecision") as "PRECISION",
        convert_cwm_typename_to_literal_prefix(t."name") as literal_prefix,
        convert_cwm_typename_to_literal_suffix(t."name") as literal_suffix,
        null_identifier() as create_params,
        cast(1 as smallint) as nullable,
        true as case_sensitive,
        true as searchable,
        false as unsigned_attribute,
        false as fixed_prec_scale,
        false as auto_increment,
        null_identifier() as local_type_name,
        cast(null as smallint) as minimum_scale,
        cast(null as smallint) as maximum_scale,
        cast(null as integer) as sql_data_type,
        cast(null as integer) as sql_datetime_sub,
        "numericPrecisionRadix" as num_prec_radix
    from
        sys_cwm."Relational"."SQLSimpleType" t
;
grant select on type_info_view to public;

create view primary_keys_view_internal as
    select
        t.table_cat,
        t.table_schem,
        t.table_name,
        k."name" as pk_name,
        k."mofId"
    from
        tables_view_internal t
    inner join 
        sys_fem."SQL2003"."PrimaryKeyConstraint" k
    on
        t."mofId" = k."namespace"
;

create view primary_keys_view as
    select
        k.table_cat,
        k.table_schem,
        k.table_name,
        c."name" as column_name,
        c."ordinal" + 1 as key_seq,
        k.pk_name
    from
        primary_keys_view_internal k
    inner join
        sys_fem."SQL2003"."KeyComponent" c
    on
        k."mofId" = c."KeyConstraint"
;
grant select on primary_keys_view to public;
        
-- TODO:  all the rest

-- just a placeholder for now
create schema localdb.information_schema;

commit;
