-- $Id$
-- Test DDL for user-defined types

create schema udttest;

set schema 'udttest';

set path 'udttest';

-- test something basic
create type rectilinear_coord as (
    x double,
    y double
);

-- test default values
create type rectilinear_coord0 as (
    x double default 0,
    y double default 0
);

-- test nested type
create type circle as (
    center rectilinear_coord,
    radius double
);

-- should fail:  at least one attribute required
create type no_attributes as ();

-- should fail:  unknown attribute type
create type bad_attrtype as (
    x foobar
);

-- should fail:  constraints not allowed on attributes
create type bad_constraint as (
    x int not null
);

-- should fail:  default value type mismatch
create type bad_default as (
    x double default 'zero'
);

-- should fail:  self-referencing nested type
create type linked_list_node as (
    x int,
    next linked_list_node
);

-- should fail:  mutual nesting
create schema cardiopulmonary
create type heart as (
    partner lung
)
create type lung as (
    partner heart
)
;

-- test out-of-order definitions
create schema musculoskeletal
create type muscle as (
    s skeleton
)
create type skeleton as (
    x double
)
;

-- test path resolution and explicit qualification

create type tire as (
    inner_radius double,
    outer_radius double,
    tread double
);

create schema spare_types
create type tire as (
    radius double,
    tread double
)
create type toothbrush as (
    firmness double,
    length double,
    head_angle double,
    electric boolean
)
;

-- test explicit qualification
create type toiletries as (
    tb spare_types.toothbrush,
    c udttest.circle
);

-- should fail:  wrong schema
create type bad_schema as (
    tb udttest.toothbrush
);

-- test implicit path lookup

set path 'spare_types';

create type washroom as (
    tb toothbrush
);

-- should fail:  not on path
create type line_segment as (
    endpoint1 rectilinear_coord,
    endpoint2 rectilinear_coord
);


-- test tables which contain typed columns
set path 'udttest';

-- create a mock table without any storage
create table mock_vertex_list(
    vertex_id int not null primary key,
    coord rectilinear_coord0
)
server sys_mock_data_server
options (executor_impl 'JAVA', row_count '1');

select v.vertex_id, v.coord.x, v.coord.y
from mock_vertex_list v;

-- create a real stored table
create table stored_vertex_list(
    vertex_id int not null primary key,
    coord rectilinear_coord0
);
