-- $Id$
-- This script creates a view schema used by JDBC metadata calls

!set verbose true
!autocommit off

-- create views in system-owned schema sys_boot.jdbc_metadata
create schema sys_boot.jdbc_metadata;
set schema 'sys_boot.jdbc_metadata';

-- NOTE:  don't include ORDER BY in the view definitions

create view schemas_view_internal as
    select 
        c."name" as table_catalog,
        s."name" as table_schem,
        s."mofId"
    from 
        sys_cwm."Relational"."Catalog" c
    inner join
        sys_cwm."Relational"."Schema" s
    on
        c."mofId" = s."namespace"
;

-- TODO:  replace t."type" with cast(null as varchar(128))
-- FIXME:  need UPPER(t."mofClassName")

create view tables_view_internal as
    select 
        s.table_catalog as table_cat,
        s.table_schem,
        t."name" as table_name,
        t."mofClassName" as table_type,
        t."type" as remarks,
        t."type" as type_cat,
        t."type" as type_schem,
        t."type" as type_name,
        t."type" as self_referencing_col_name,
        t."type" as ref_generation,
        t."mofId"
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

create view schemas_view as
    select 
        table_schem,
        table_catalog
    from 
        schemas_view_internal
;

create view tables_view as
    select 
        table_cat,
        table_schem,
        table_name,
        table_type,
        remarks,
        type_cat,
        type_schem,
        type_name,
        self_referencing_col_name,
        ref_generation
    from
        tables_view_internal
;

-- TODO:  upper-case strings, and add 'GLOBAL TEMPORARY' and 'SYSTEM TABLE'
create view table_types_view(table_type) as
    select * 
    from values 
        ('ForeignTable'),
        ('LocalTable'),
        ('View')
;

--FIXME: c."length" can't be just column_size.
-- num_prec_radix is supposed to be 2/10 depending on binary/decimal
-- For char or date types this is the maximum number of characters,
-- for numeric or decimal types this is precision.
-- Both nullable and is_nullable require transformation.
-- Have to get column_def by left-outer-join to get default value.

create view tables_columns_view_internal as
    select 
        t.table_cat,
        t.table_schem,
        t.table_name,
        c."name" as column_name,
        c."type",
        c."length" as column_size,
        c."namespace" as buffer_len,
        c."scale" as dec_digits,
        c."scale" as num_prec_radix,
        c."isNullable" as nullable,
        c."namespace" as remarks,
        c."length" as char_octet_length,
        c."ordinal" + 1 as ordinal_position,
        c."isNullable" as is_nullable,
        c."namespace" as scope_catalog,
        c."namespace" as scope_schema,
        c."namespace" as scope_table,
        c."namespace" as source_data_type,
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
        c.num_prec_radix,
        c.nullable,
        c.remarks,
        t."name" as column_def,
        0 as sql_data_type,
        0 as sql_datetime_sub,
        c.char_octet_length,
        c.ordinal_position,
        c.is_nullable,
        c.scope_catalog,
        c.scope_schema,
        c.scope_table,
        c.source_data_type
    from 
        tables_columns_view_internal c 
    inner join 
        sys_cwm."Relational"."SQLDataType" t 
    on 
        c."type" = t."mofId";

-- TODO:  all the rest
         
commit;
