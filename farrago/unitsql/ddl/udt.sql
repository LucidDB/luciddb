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


-- test tables which store typed columns
-- (put udt columns first to test offset flattening)
set path 'udttest';

create table stored_coord_list(
    coord1 rectilinear_coord0,
    coord2 rectilinear_coord0,
    pair_id int not null primary key
);

-- test views which access typed columns
create view viewed_coord_list as
select scl.pair_id, scl.coord1, scl.coord2.y
from stored_coord_list scl;

!set outputformat csv

explain plan for
select scl.pair_id, scl.coord1.y, scl.coord2.x
from stored_coord_list scl;

explain plan for
select v.pair_id, v.coord1.x, v.coord1.y, v.y as y2 
from viewed_coord_list v;
