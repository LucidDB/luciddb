-- $Id$
-- This script creates a view schema used by JDBC metadata calls

!set verbose true
!autocommit off

-- create views in system-owned schema sys_boot.jdbc_metadata
create schema sys_boot.jdbc_metadata;
set schema sys_boot.jdbc_metadata;

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
        ('Table'),
        ('View')
;

-- TODO:  all the rest
         
commit;
