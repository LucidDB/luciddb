-- This script creates a view schema used for database management 
                                                                                
!set verbose true
                                                                                
-- create views in system-owned schema sys_boot.mgmt
create or replace schema sys_boot.mgmt;
set schema 'sys_boot.mgmt';
set path 'sys_boot.mgmt';

create or replace function repository_properties()
returns table(property_name varchar(255), property_value varchar(255))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.repositoryProperties';

create or replace view repository_properties_view as
  select * from table(repository_properties());

create or replace function repository_integrity_violations()
returns table(description varchar(65535), mof_id varchar(128))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.repositoryIntegrityViolations';

create or replace function statements()
returns table(id bigint, session_id bigint, sql_stmt varchar(1024), create_time timestamp, parameters varchar(1024))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.statements';

create or replace view statements_view as
  select * from table(statements());

-- todo:  grant this only to a privileged user
grant select on statements_view to public;

create or replace function sessions()
returns table(id int, url varchar(128), current_user_name varchar(128), current_role_name varchar(128), session_user_name varchar(128), system_user_name varchar(128), system_user_fullname varchar(128), session_name varchar(128), program_name varchar(128), process_id int, catalog_name varchar(128), schema_name varchar(128), is_closed boolean, is_auto_commit boolean, is_txn_in_progress boolean)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.sessions';

create or replace view sessions_view as
  select * from table(sessions());

-- todo:  grant this only to a privileged user
grant select on sessions_view to public;

create or replace function objects_in_use()
returns table(session_id bigint, stmt_id bigint, mof_id varchar(128))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.objectsInUse';

create or replace view objects_in_use_view as
  select * from table(objects_in_use());

-- TODO: grant this only to a privileged user
grant select on objects_in_use_view to public;

create or replace function threads()
returns table(
    thread_id bigint, thread_group_name varchar(128), thread_name varchar(128),
    thread_priority int, thread_state varchar(128), is_alive boolean,
    is_daemon boolean, is_interrupted boolean)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.threadList';

create or replace function thread_stack_entries()
returns table(
    thread_id bigint, stack_level int, entry_string varchar(1024),
    class_name varchar(128), method_name varchar(128), 
    file_name varchar(1024), line_num int, is_native boolean)
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.syslib.FarragoManagementUDR.threadStackEntries';

create or replace function system_info()
returns table(
    source_name varchar(128), 
    item_name varchar(1024), 
    item_units varchar(128),
    item_value varchar(65535))
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.syslib.FarragoManagementUDR.systemInfo';

create or replace function performance_counters()
returns table(
    source_name varchar(128), 
    counter_name varchar(1024), 
    counter_units varchar(128),
    counter_value varchar(1024))
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.syslib.FarragoManagementUDR.performanceCounters';

-- lie and say this is non-deterministic, since it's usually used
-- in cases where it would be annoying if it got optimized away
create or replace function sleep(millis bigint)
returns integer
language java
no sql
not deterministic
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.sleep';

-- flushes all entries from the global code cache
create or replace procedure flush_code_cache()
  language java
  parameter style java
  reads sql data
  external name 
  'class net.sf.farrago.syslib.FarragoManagementUDR.flushCodeCache';

-- lets an administrator kill a running session
-- TODO: grant this only to a privileged user
create or replace procedure kill_session(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killSession';

-- lets an administrator kill an executing statement
-- (like unix "kill -KILL")
-- param ID: globally-unique statement id
-- TODO: grant this only to a privileged user
create or replace procedure kill_statement(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killStatement';

-- kills all statements with SQL matching a given string
-- (like unix pkill)
-- Works around lack of scalar subqueries, whuch makes kill_statement(id) hard to use
-- param SQL: a string
-- TODO: grant this only to a privileged user
create or replace procedure kill_statement_match(in s varchar(256))
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killStatementMatch';

-- exports the catalog to an XMI file
create or replace procedure export_catalog_xmi(in filename varchar(65535))
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoManagementUDR.exportCatalog';

-- exports query results to a delimited file (optionally with BCP control file)
create or replace procedure export_query_to_file(
  in query_sql varchar(65535),
  in path_without_extension varchar(65535),
  in bcp boolean,
  in include_data boolean,
  in delete_failed_file boolean,
  in field_delimiter varchar(2),
  in file_extension varchar(5),
  in date_format varchar(128),
  in time_format varchar(128),
  in timestamp_format varchar(128))
language java
reads sql data
specific export_query_to_file_3
called on null input
external name 'class net.sf.farrago.syslib.FarragoExportSchemaUDR.exportQueryToFile';

-- Returns session parameters
create or replace function session_parameters ()
returns table(
  param_name varchar(128),
  param_value varchar(128))
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.syslib.FarragoManagementUDR.sessionParameters';

create or replace view session_parameters_view as
  select * from table(session_parameters());

-- todo:  grant this only to a privileged user
grant select on session_parameters_view to public;

--
-- Statistics
--

-- Set the row count of a table
create or replace procedure stat_set_row_count(
    in catalog_name varchar(2000),
    in schema_name varchar(2000),
    in table_name varchar(2000),
    in row_count bigint)
language java
contains sql
external name 'class net.sf.farrago.syslib.FarragoStatsUDR.set_row_count';

-- Set the page count of an index
create or replace procedure stat_set_page_count(
    in catalog_name varchar(2000),
    in schema_name varchar(2000),
    in index_name varchar(2000),
    in page_count bigint)
language java
contains sql
external name 'class net.sf.farrago.syslib.FarragoStatsUDR.set_page_count';

-- Generate a histogram for a column
--
-- distribution_type must be 0 for now
-- value_digits are characters to use for fake column values
create or replace procedure stat_set_column_histogram(
    in catalog_name varchar(2000),
    in schema_name varchar(2000),
    in table_name varchar(2000),
    in column_name varchar(2000),
    in distict_values bigint,
    in sample_percent int,
    in sample_distinct_values bigint,
    in distribution_type int,
    in value_digits varchar(2000))
language java
contains sql
external name 'class net.sf.farrago.syslib.FarragoStatsUDR.set_column_histogram';

-- Statistics views
create or replace view page_counts_view as
    select 
        i.table_cat,
        i.table_schem,
        i.table_name,
        i.index_name,
        i.pages
    from
        sys_boot.jdbc_metadata.index_info_internal i
    where
        i.pages is not null
;

create or replace view row_counts_view as
    select
        t.table_cat,
        t.table_schem,
        t.table_name,
        acs."rowCount" as row_count
    from 
        sys_boot.jdbc_metadata.tables_view_internal t
    inner join
        sys_fem."SQL2003"."AbstractColumnSet" acs
    on
        t."mofId" = acs."mofId"
    where 
        acs."rowCount" is not null
;

create or replace view histograms_view_internal as
    select 
        c.table_cat,
        c.table_schem,
        c.table_name,
        c.column_name,
        h."distinctValueCount" as "CARDINALITY",
        h."percentageSampled" as percent_sampled,
        h."barCount" as bar_count,
        h."rowsPerBar" as rows_per_bar,
        h."rowsLastBar" as rows_last_bar,
        h."mofId"
    from 
        sys_boot.jdbc_metadata.columns_view_internal c
    inner join
        sys_fem.med."ColumnHistogram" h
    on
        c."mofId" = h."Column"
;

create or replace view histograms_view as
    select
        h.table_cat,
        h.table_schem,
        h.table_name,
        h.column_name,
        h."CARDINALITY",
        h.percent_sampled,
        h.bar_count,
        h.rows_per_bar,
        h.rows_last_bar
    from
        histograms_view_internal h
;

create or replace view histogram_bars_view as
    select 
        h.table_cat,
        h.table_schem,
        h.table_name,
        h.column_name,
        b."ordinal" as ordinal,
        b."startingValue" as start_value,
        b."valueCount" as value_count
    from
        histograms_view_internal h
    inner join
        sys_fem.med."ColumnHistogramBar" b
    on
        h."mofId" = b."Histogram"
;

--
-- Sequences
--

create or replace view sequences_view as
    select
        c.table_cat,
        c.table_schem,
        c.table_name,
        c.column_name,
        s."baseValue",
        s."increment",
        s."minValue",
        s."maxValue",
        s."cycle",
        s."expired"
    from
        sys_boot.jdbc_metadata.columns_view_internal c
    inner join
        sys_fem."SQL2003"."SequenceGenerator" s
    on
        c."mofId" = s."Column"
;

--
-- Database admin internal views
--

create or replace view dba_schemas_internal1 as
  select
    c."name" as catalog_name,
    s."name" as schema_name,
    cast(s."creationTimestamp" as timestamp) as creation_timestamp,
    cast(s."modificationTimestamp" as timestamp) as last_modified_timestamp,
    s."description" as remarks,
    s."mofId",
    s."lineageId"
  from
    sys_fem."SQL2003"."LocalCatalog" c
  inner join
    sys_fem."SQL2003"."LocalSchema" s
  on
    c."mofId" = s."namespace"
;

create or replace view dba_schemas_internal2 as
  select
    catalog_name,
    schema_name,
    creation_timestamp,
    last_modified_timestamp,
    remarks,
    si."mofId",
    si."lineageId",
    g."Grantee"
  from
    dba_schemas_internal1 si
  inner join
    sys_fem."Security"."Grant" g
  on
   si."mofId" = g."Element"
  where
   g."action" = 'CREATION'
;

create or replace view dba_tables_internal1 as
  select 
    table_cat as catalog_name,
    table_schem as schema_name,
    table_name as table_name,
    t."mofClassName" as table_type,
    cast(ae."creationTimestamp" as timestamp) as creation_timestamp,
    cast(ae."modificationTimestamp" as timestamp) as last_modification_timestamp,
    "description" as remarks,
    ae."mofId",
    ae."lineageId"
  from
    sys_boot.jdbc_metadata.tables_view_internal t
  inner join 
    sys_fem."SQL2003"."AnnotatedElement" ae
  on
    t."mofId" = ae."mofId"
;

create or replace view dba_tables_internal2 as
  select
    catalog_name,
    schema_name,
    table_name,
    table_type,
    creation_timestamp,
    last_modification_timestamp,
    remarks,
    g."Grantee",
    dti."mofId",
    dti."lineageId"
  from
    dba_tables_internal1 dti
  inner join
    sys_fem."Security"."Grant" g    
  on
    dti."mofId" = g."Element"
  where
    g."action" = 'CREATION'
;

create or replace view dba_views_internal1 as
  select
    object_catalog as catalog_name,
    object_schema as schema_name,
    v."name" as view_name,
    cast("creationTimestamp" as timestamp) as creation_timestamp,
    cast("modificationTimestamp" as timestamp) as last_modification_timestamp,
    "originalDefinition" as original_text,
    "description" as remarks,
    v."mofId",
    v."lineageId"
  from
    sys_boot.jdbc_metadata.schemas_view_internal s
  inner join
    sys_fem."SQL2003"."LocalView" v
  on
    s."mofId" = v."namespace"
;


create or replace view dba_views_internal2 as
  select
    catalog_name,
    schema_name,
    view_name,
    creation_timestamp,
    last_modification_timestamp,
    original_text,
    remarks,
    vi."mofId",
    vi."lineageId",
    g."Grantee"
  from
    dba_views_internal1 vi
  inner join
    sys_fem."Security"."Grant" g
  on
    vi."mofId" = g."Element"
  where
    g."action" = 'CREATION'
;

create or replace view dba_stored_tables_internal1 as
  select
    object_catalog as catalog_name,
    object_schema as schema_name,
    t."name" as table_name,
    cast(t."creationTimestamp" as timestamp) as creation_timestamp,
    cast(t."modificationTimestamp" as timestamp) 
      as last_modification_timestamp,
    t."rowCount" as last_analyze_row_count,
    cast(t."analyzeTime" as timestamp) as last_analyze_timestamp,
    t."description" as remarks,
    t."lineageId",
    t."mofId"
  from
    sys_boot.jdbc_metadata.schemas_view_internal s
  inner join
    sys_fem.med."StoredTable" t
  on
    s."mofId" = t."namespace"
;

create or replace view dba_stored_tables_internal2 as
  select
    catalog_name,
    schema_name,
    table_name,
    creation_timestamp,
    last_modification_timestamp,
    last_analyze_row_count,
    last_analyze_timestamp,
    remarks,
    sti."lineageId",
    sti."mofId",
    g."Grantee"
  from
    dba_stored_tables_internal1 sti
  inner join
    sys_fem."Security"."Grant" g
  on
    sti."mofId" = g."Element"
  where
    g."action" = 'CREATION'
;

create or replace view dba_routines_internal1 as
  select
    s.object_catalog as catalog_name,
    s.object_schema as schema_name,
    r."invocationName" as invocation_name,
    r."name" as specific_name,
    r."externalName" as external_name,
    r."type" as routine_type,
    cast(r."creationTimestamp" as timestamp) as creation_timestamp,
    cast(r."modificationTimestamp" as timestamp) as last_modified_timestamp,
    r."isUdx" as is_table_function,
    r."parameterStyle" as parameter_style,
    r."deterministic" as is_deterministic,
    r."dataAccess" as data_access,
    r."description" as remarks,
    r."mofId",
    r."lineageId"
  from
    sys_boot.jdbc_metadata.schemas_view_internal s
  inner join
    sys_fem."SQL2003"."Routine" r
  on
    s."mofId" = r."namespace"
;
    
create or replace view dba_routines_internal2 as
  select 
    catalog_name,
    schema_name,
    invocation_name,
    specific_name,
    external_name,
    routine_type,
    creation_timestamp,
    last_modified_timestamp,
    is_table_function,
    parameter_style,
    is_deterministic,
    data_access,
    remarks,
    ri."mofId",
    ri."lineageId",
    g."Grantee"
  from
    dba_routines_internal1 ri
  inner join
    sys_fem."Security"."Grant" g
  on
    ri."mofId" = g."Element"
  where
    g."action" = 'CREATION'
;

create or replace view dba_routine_parameters_internal1 as
  select
    catalog_name,
    schema_name,
    specific_name as routine_specific_name,
    rp."name" as parameter_name,
    rp."ordinal" as ordinal,
    coalesce(rp."length", rp."precision") as "PRECISION",
    rp."scale" as dec_digits,
    rp."description" as remarks,
    rp."mofId",
    rp."lineageId",
    rp."type",
    ri.is_table_function
  from
    dba_routines_internal1 ri
  inner join
    sys_fem."SQL2003"."RoutineParameter" rp
  on
    ri."mofId" = rp."behavioralFeature"
;

create or replace view dba_foreign_wrappers_internal as
  select 
    dw."name" as foreign_wrapper_name,
    dw."libraryFile" as library,
    dw."language" as "LANGUAGE",
    cast(dw."creationTimestamp" as timestamp) as creation_timestamp,
    cast(dw."modificationTimestamp" as timestamp) last_modified_timestamp,
    dw."description" as remarks,
    g."Grantee",
    dw."mofId",
    dw."lineageId"
  from
    sys_fem.med."DataWrapper" dw
  inner join
    sys_fem."Security"."Grant" g
  on 
    dw."mofId" = g."Element"
  where
    dw."foreign" = true and g."action" = 'CREATION'
;

create or replace view dba_foreign_servers_internal1 as
  select
    foreign_wrapper_name,
    ds."name" as foreign_server_name,
    cast(ds."creationTimestamp" as timestamp) as creation_timestamp,
    cast(ds."modificationTimestamp" as timestamp) as last_modified_timestamp,
    ds."description" as remarks,
    ds."mofId",
    ds."lineageId"
  from
    dba_foreign_wrappers_internal fwi
  inner join
    sys_fem.med."DataServer" ds
  on
    fwi."mofId" = ds."Wrapper"
;

create or replace view dba_foreign_servers_internal2 as
  select
    foreign_wrapper_name,
    foreign_server_name,
    creation_timestamp,
    last_modified_timestamp,
    remarks,
    g."Grantee",
    fsi."mofId",
    fsi."lineageId"
  from
    dba_foreign_servers_internal1 fsi
  inner join
    sys_fem."Security"."Grant" g
  on
    fsi."mofId" = g."Element"
  where
    g."action" = 'CREATION'
;

create or replace view dba_foreign_tables_internal1 as
  select
    fs.foreign_wrapper_name,
    fs.foreign_server_name,
    ft."name" as foreign_table_name,
    cast(ft."creationTimestamp" as timestamp) as creation_timestamp,
    cast(ft."modificationTimestamp" as timestamp) as last_modified_timestamp,
    ft."rowCount" as last_analyze_row_count,
    cast(ft."analyzeTime" as timestamp) as last_analyze_timestamp,
    ft."description" as remarks,
    ft."mofId",
    ft."lineageId"
  from
    dba_foreign_servers_internal1 fs
  inner join
    sys_fem.med."ForeignTable" ft
  on
    fs."mofId" = ft."Server"
;

create or replace view dba_foreign_tables_internal2 as
  select
    fti.foreign_wrapper_name,
    fti.foreign_server_name,
    fti.foreign_table_name,
    fti.creation_timestamp,
    fti.last_modified_timestamp,
    fti.last_analyze_row_count,
    fti.last_analyze_timestamp,
    fti.remarks,
    g."Grantee",
    fti."mofId",
    fti."lineageId"
  from
    dba_foreign_tables_internal1 fti
  inner join
    sys_fem."Security"."Grant" g
  on
    fti."mofId" = g."Element"
  where
    g."action" = 'CREATION'
;

-- Returns the set of all foreign data wrappers which have been marked
-- as suitable for browse connect (mark is via the presence of the
-- BROWSE_CONNECT_DESCRIPTION option).
create or replace view browse_connect_foreign_wrappers as
  select
    dw."name" as foreign_wrapper_name,
    so."value" as browse_connect_description
  from
    sys_fem.med."DataWrapper" dw
  inner join
    sys_fem.med."StorageOption" so
  on
    dw."mofId" = so."StoredElement"
  where 
    dw."foreign" = true
    and so."name" = 'BROWSE_CONNECT_DESCRIPTION'
;

-- Returns the set of options relevant to a given wrapper.  A partial
-- set of options may be passed in via the proposed_server_options
-- cursor parameter, which must have two columns (OPTION_NAME and
-- OPTION_VALUE, in that order).  This allows for an incremental
-- connection interaction, starting with specifying no options, then
-- some, then more, stopping once user and wrapper are both satisfied.
-- The result set is not fully normalized, because some options
-- support a list of choices (e.g. for a dropdown selection UI
-- widget).  optional_choice_ordinal -1 represents the "current"
-- choice (either proposed by the user or chosen as default by the
-- wrapper); other choice ordinals starting from 0 represent possible
-- choices (if known).
create or replace function browse_connect_foreign_server(
  foreign_wrapper_name varchar(128),
  proposed_server_options cursor)
returns table(
  option_ordinal integer,
  option_name varchar(128), 
  option_description varchar(4096),
  is_option_required boolean,
  option_choice_ordinal int,
  option_choice_value varchar(4096))
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.syslib.FarragoMedUDR.browseConnectServer';

-- A view which can be used as the input cursor for proposed_server_options
-- when no options are set (initial browse).
create or replace view browse_connect_empty_options as
select '' as option_name, '' as option_value
from sys_boot.jdbc_metadata.empty_view;

-- Returns foreign schemas accessible via a given foreign server.
create or replace function browse_foreign_schemas(
  foreign_server_name varchar(128))
returns table(
  schema_name varchar(128),
  description varchar(4096))
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.syslib.FarragoMedUDR.browseForeignSchemas';

--
-- Datetime conversion functions
--

-- converts a string to a date, according to the specified format string
create or replace function char_to_date(format varchar(50), dateString varchar(128))
returns date
language java
specific std_char_to_date
no sql
external name 'class net.sf.farrago.syslib.FarragoConvertDatetimeUDR.char_to_date';

create or replace function char_to_time(format varchar(50), timeString varchar(128))
returns time
language java
specific std_char_to_time
no sql
external name 'class net.sf.farrago.syslib.FarragoConvertDatetimeUDR.char_to_time';

create or replace function char_to_timestamp(
     format varchar(50), timestampString varchar(128))
returns timestamp
language java
specific std_char_to_timestamp
no sql
external name 'class net.sf.farrago.syslib.FarragoConvertDatetimeUDR.char_to_timestamp';

-- formats a string as a date, according to the specified format string
create or replace function date_to_char(format varchar(50), d date)
returns varchar(128)
language java
specific std_date_to_char
no sql
external name 'class net.sf.farrago.syslib.FarragoConvertDatetimeUDR.date_to_char';

create or replace function time_to_char(format varchar(50), t time)
returns varchar(128)
language java
specific std_time_to_char
no sql
external name 'class net.sf.farrago.syslib.FarragoConvertDatetimeUDR.time_to_char';

create or replace function timestamp_to_char(format varchar(50), ts timestamp)
returns varchar(128)
language java
specific std_timestamp_to_char
no sql
external name 'class net.sf.farrago.syslib.FarragoConvertDatetimeUDR.timestamp_to_char';

