-- $Id$
-- This script creates a view schema used by JDBC metadata calls

!set verbose true

-- create views in system-owned schema sys_boot.jdbc_metadata
create or replace schema sys_boot.jdbc_metadata;
set schema 'sys_boot.jdbc_metadata';
set path 'sys_boot.jdbc_metadata';

create or replace function null_identifier()
returns varchar(128)
contains sql
deterministic
return cast(null as varchar(128));

create or replace function null_remarks()
returns varchar(1024)
contains sql
deterministic
return cast(null as varchar(1024));

create or replace function convert_cwm_nullable_to_int(
  cwm_nullability varchar(20))
returns integer
contains sql
deterministic
return case
when cwm_nullability='columnNoNulls' then 0
when cwm_nullability='columnNullable' then 1
else 2
end;

create or replace function convert_cwm_nullable_to_string(
  cwm_nullability varchar(20))
returns varchar(3)
contains sql
deterministic
return case
when cwm_nullability='columnNoNulls' then 'NO'
when cwm_nullability='columnNullable' then 'YES'
else ''
end;

create or replace function convert_cwm_param_kind_to_int(
  cwm_param_kind varchar(10))
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

create or replace function convert_cwm_typename_to_literal_prefix(
  typename varchar(128))
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

create or replace function convert_cwm_typename_to_literal_suffix(
  typename varchar(128))
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

-- NOTE:  currently unused return codes are 0 for statistic, 2 for hashed
create or replace function convert_cwm_index_attributes_to_type(
  is_clustered boolean)
returns smallint
contains sql
deterministic
return case
when is_clustered then 1
else 3
end;

create or replace function convert_cwm_statistic_to_int(val numeric)
returns int
contains sql
deterministic
return case
when val is null then 0
else cast (val as int)
end;

create or replace function filter_user_visible_objects(
  input_set cursor,
  id_columns select from input_set)
returns table(input_set.*)
language java
parameter style system defined java
no sql
not deterministic
external name 
'class net.sf.farrago.syslib.FarragoManagementUDR.filterUserVisibleObjects';

create or replace function filter_user_visible_objects_typed(
  input_set cursor,
  class_name varchar(128),
  id_columns select from input_set)
returns table(input_set.*)
language java
parameter style system defined java
no sql
not deterministic
external name 
'class net.sf.farrago.syslib.FarragoManagementUDR.filterUserVisibleObjectsTyped';

-- NOTE:  don't include ORDER BY in the view definitions

create or replace view schemas_view_internal as
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

create or replace view tables_view_internal as
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

create or replace view catalogs_view as
    select 
        c."name" as table_cat
    from 
        sys_cwm."Relational"."Catalog" c
;
grant select on catalogs_view to public;

create or replace view schemas_view as
    select 
        object_schema as table_schem,
        object_catalog as table_catalog
    from 
        schemas_view_internal
;
grant select on schemas_view to public;

-- TODO:  add 'GLOBAL TEMPORARY' and 'SYSTEM TABLE'
create or replace view table_types_view_internal(table_type,uml_class_name) as
    values 
        (trim('FOREIGN TABLE'),trim('ForeignTable')),
        ('TABLE','LocalTable'),
        ('VIEW','LocalView')
;

create or replace view tables_view as
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
        table(filter_user_visible_objects(
          cursor(select * from tables_view_internal),
          row("mofId","mofClassName"))) t,
        table_types_view_internal tt
    where
        t."mofClassName"=tt.uml_class_name
;
grant select on tables_view to public;

create or replace view table_types_view as
    select distinct
        table_type
    from
        table_types_view_internal
;
grant select on table_types_view to public;

-- TODO: get source_data_type for distinct types
-- TODO: get predefined numericPrecision and numericPrecisionRadix
-- REVIEW: length/precision/scale/radix
-- also all of above for attributes and procedure_columns

create or replace view columns_view_internal as
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
        c."description" as remarks,
        c."mofId",
        c."lineageId",
        nullif(e."body",'NULL') as default_value,
        t."mofId" as table_mofid,
        t."mofClassName" as table_class_name
    from 
        tables_view_internal t 
    inner join 
        sys_fem."SQL2003"."AbstractColumn" c 
    on 
        t."mofId" = c."owner"
    left outer join
        sys_cwm."Core"."Expression" e
    on 
        e."mofId" = c."initialValue";

create or replace view columns_view as
    select 
        c.table_cat,
        c.table_schem,
        c.table_name,
        c.column_name,
        t."typeNumber" as data_type,
        t."name" as type_name,
        c.column_size,
        c.buffer_len,
        c.dec_digits as "DECIMAL_DIGITS",
        cast(null as integer) as num_prec_radix,
        c.nullable,
        null_remarks() as remarks,
        c.default_value as column_def,
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
        table(filter_user_visible_objects(
          cursor(select * from columns_view_internal),
          row(table_mofid,table_class_name))) c
    inner join 
        sys_cwm."Relational"."SQLDataType" t 
    on 
        c."type" = t."mofId";
grant select on columns_view to public;

create or replace view udts_view_internal as
    select
        s.object_catalog as type_cat,
        s.object_schema as type_schem,
        u."name" as type_name,
        u."typeNumber" as data_type,
        u."mofId",
        u."mofClassName"
    from
        schemas_view_internal s
    inner join
        sys_fem."SQL2003"."UserDefinedType" u
    on
        s."mofId" = u."namespace"
;

-- TODO:  base_type for distinct types

create or replace view udts_view as
    select
        u.type_cat,
        u.type_schem,
        u.type_name,
        null_identifier() as class_name,
        u.data_type,
        null_remarks() as remarks,
        cast (null as smallint) as base_type
    from
        table(filter_user_visible_objects(
          cursor(select * from udts_view_internal),
          row("mofId","mofClassName"))) u
;
grant select on udts_view to public;

create or replace view attributes_view_internal as
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
        a."mofId",
        u."mofId" as udt_mofid,
        u."mofClassName" as udt_class_name
    from
        udts_view_internal u
    inner join
        sys_fem."SQL2003"."SQLTypeAttribute" a
    on
        u."mofId" = a."owner"
;

create or replace view attributes_view as
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
        table(filter_user_visible_objects(
          cursor(select * from attributes_view_internal),
          row(udt_mofid,udt_class_name))) a
    inner join
        sys_cwm."Relational"."SQLDataType" t 
    on
        a."type" = t."mofId"
;
grant select on attributes_view to public;

-- TODO:  find out why replacing BehavioralFeature below with Method or
-- Routine doesn't work (causes assignment of null to NOT NULL).  Must be
-- a bug in Farrago's MDR namespace support.

create or replace view procedures_view_internal as
    select
        s.object_catalog as procedure_cat,
        s.object_schema as procedure_schem,
        r."name" as procedure_name,
        r."mofId",
        r."mofClassName"
    from
        schemas_view_internal s
    inner join
        sys_cwm."Behavioral"."BehavioralFeature" r
    on
        s."mofId" = r."namespace"
;

-- TODO:  other values for procedure_type once we support procedures
-- that return result sets

create or replace view procedures_view as
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
        table(filter_user_visible_objects(
          cursor(select * from procedures_view_internal),
          row("mofId", "mofClassName"))) p
;
grant select on procedures_view to public;

create or replace view procedure_columns_view_internal as
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
        rp."mofId",
        p."mofId" as procedure_mofid,
        p."mofClassName" as procedure_class_name
    from
        procedures_view_internal p
    inner join
        sys_fem."SQL2003"."RoutineParameter" rp
    on
        p."mofId" = rp."behavioralFeature"
;

create or replace view procedure_columns_view as
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
        table(filter_user_visible_objects(
          cursor(select * from procedure_columns_view_internal),
          row(procedure_mofid, procedure_class_name))) pc
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

create or replace view type_info_view as
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
        3 as searchable,
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

create or replace view primary_keys_view_internal as
    select
        t.table_cat,
        t.table_schem,
        t.table_name,
        k."name" as pk_name,
        k."mofId",
        t."mofId" as table_mofid,
        t."mofClassName" as table_class_name
    from
        tables_view_internal t
    inner join 
        sys_fem."SQL2003"."PrimaryKeyConstraint" k
    on
        t."mofId" = k."namespace"
;

create or replace view primary_keys_view as
    select
        k.table_cat,
        k.table_schem,
        k.table_name,
        c."name" as column_name,
        c."ordinal" + 1 as key_seq,
        k.pk_name
    from
        table(filter_user_visible_objects(
          cursor(select * from primary_keys_view_internal),
          row(table_mofid, table_class_name))) k
    inner join
        sys_fem."SQL2003"."KeyComponent" c
    on
        k."mofId" = c."KeyConstraint"
;
grant select on primary_keys_view to public;

-- TODO: use an outer join with histograms for index cardinality
--   or store that statistic in the catalog

create or replace view index_info_internal as
    select
        t.table_cat,
        t.table_schem,
        t.table_name,
        (not i."isUnique") as non_unique,
        t.table_cat as index_qualifier,
        i."name" as index_name,
        convert_cwm_index_attributes_to_type(i."isClustered") as type,
        0 as "CARDINALITY",
        i."pageCount" as pages,
        cast(null as varchar(128)) as filter_condition,
        i."mofId",
        t."mofId" as table_mofid,
        t."mofClassName" as table_class_name
    from 
        tables_view_internal t
    inner join
        sys_fem."MED"."LocalIndex" i
    on
        t."mofId" = i."spannedClass"
;

create or replace view table_row_counts_internal as
    select
        t.table_cat,
        t.table_schem,
        t.table_name,
        acs."rowCount" as "CARDINALITY",
        t."mofId",
        t."mofClassName"
    from 
        tables_view_internal t
    inner join
        sys_fem.sql2003."AbstractColumnSet" acs
    on
        t."mofId" = acs."mofId"
;

-- NOTE: would be cleaner to have a separate view for page counts 
--   but MedMdr joins work best when joining simple tables
create or replace view table_stats_internal as
    select
        t.table_cat,
        t.table_schem,
        t.table_name,
        t."CARDINALITY",
        sum(i."pageCount") as pages,
        t."mofId",
        t."mofClassName"
    from 
        table_row_counts_internal t
    inner join
        sys_fem.med."LocalIndex" i
    on
        t."mofId" = i."spannedClass"
    group by 
        t."mofId",
        t."mofClassName",
        t.table_cat,
        t.table_schem,
        t.table_name,
        t."CARDINALITY"
;

-- left outer join needed to handle cases where indexes are not associated
-- with any specific column
create or replace view index_info_view as
    select
        i.table_cat,
        i.table_schem,
        i.table_name,
        i.non_unique,
        i.table_cat as index_qualifier,
        i.index_name,
        i.type,
        c."ordinal" + 1 as ordinal_position,
        c."name" as column_name,
        'A' as asc_or_desc,
        i."CARDINALITY",
        convert_cwm_statistic_to_int(i.pages) as pages,
        i.filter_condition
    from 
        table(filter_user_visible_objects(
          cursor(select * from index_info_internal),
          row(table_mofid, table_class_name))) i
    left outer join
        sys_fem."MED"."LocalIndexColumn" c
    on
        i."mofId" = c."index"
union
    select 
        t.table_cat,
        t.table_schem,
        t.table_name,
        false as non_unique,
        null_identifier() as index_qualifier,
        null_identifier() as index_name,
        0 as type,
        0 as ordinal_position,
        null_identifier() as column_name,
        cast(null as varchar(128)) as asc_or_desc,
        convert_cwm_statistic_to_int(t."CARDINALITY") as "CARDINALITY",
        convert_cwm_statistic_to_int(t.pages) as pages,
        cast(null as varchar(128)) as filter_condition
    from
        table(filter_user_visible_objects(
          cursor(select * from table_stats_internal),
          row("mofId", "mofClassName"))) t
;
grant select on index_info_view to public;

create or replace view empty_view as
select * from (values(0)) where false;

create or replace view super_tables_view as
    select
        null_identifier() as table_cat,
        null_identifier() as table_schem,
        null_identifier() as table_name,
        null_identifier() as supertable_name
    from empty_view;
grant select on super_tables_view to public;
    
create or replace view super_types_view as
    select
        null_identifier() as type_cat,
        null_identifier() as type_schem,
        null_identifier() as type_name,
        null_identifier() as supertype_cat,
        null_identifier() as supertype_schem,
        null_identifier() as supertype_name
    from empty_view;
grant select on super_types_view to public;

create or replace view exported_keys_view as
    select
        null_identifier() as pktable_cat,
        null_identifier() as pktable_schem,
        null_identifier() as pktable_name,
        null_identifier() as pkcolumn_name,
        null_identifier() as fktable_cat,
        null_identifier() as fktable_schem,
        null_identifier() as fktable_name,
        null_identifier() as fkcolumn_name,
        cast(null as smallint) as key_seq,
        cast(null as smallint) as update_rule,
        cast(null as smallint) as delete_rule,
        null_identifier() as fk_name,
        null_identifier() as pk_name,
        cast(null as smallint) as deferrability
    from empty_view;
grant select on exported_keys_view to public;

create or replace view imported_keys_view as
    select
        null_identifier() as pktable_cat,
        null_identifier() as pktable_schem,
        null_identifier() as pktable_name,
        null_identifier() as pkcolumn_name,
        null_identifier() as fktable_cat,
        null_identifier() as fktable_schem,
        null_identifier() as fktable_name,
        null_identifier() as fkcolumn_name,
        cast(null as smallint) as key_seq,
        cast(null as smallint) as update_rule,
        cast(null as smallint) as delete_rule,
        null_identifier() as fk_name,
        null_identifier() as pk_name,
        cast(null as smallint) as deferrability
    from empty_view;
grant select on imported_keys_view to public;

create or replace view cross_reference_view as
    select
        null_identifier() as pktable_cat,
        null_identifier() as pktable_schem,
        null_identifier() as pktable_name,
        null_identifier() as pkcolumn_name,
        null_identifier() as fktable_cat,
        null_identifier() as fktable_schem,
        null_identifier() as fktable_name,
        null_identifier() as fkcolumn_name,
        cast(null as smallint) as key_seq,
        cast(null as smallint) as update_rule,
        cast(null as smallint) as delete_rule,
        null_identifier() as fk_name,
        null_identifier() as pk_name,
        cast(null as smallint) as deferrability
    from empty_view;
grant select on cross_reference_view to public;

-- TODO:  all the rest

-- just a placeholder for now
create or replace schema localdb.information_schema;
