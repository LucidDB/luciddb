-- $Id$
-- Test DDL for user-defined types

create schema udttest;

set schema 'udttest';

set path 'udttest';

-- basic distinct types
create type dollar_currency as double;
create type euro_currency as double;

-- should fail:  can't specify NOT INSTANTIABLE for distinct type
create type simolean_currency as double not instantiable;

-- should fail:  can't specify NOT FINAL for distinct type
create type simolean_currency as double not final;

-- should succeed
create type simolean_currency as double final;

-- basic structured type
create type rectilinear_coord as (
    x double,
    y double
) final;

-- structured type with default values
create type rectilinear_coord0 as (
    x double default 0,
    y double default 0
) final;

-- structured type nesting
create type circle as (
    center rectilinear_coord0,
    radius double default 1
) final;

-- explicit instantiability declaration
create type ellipse as (
    major_axis double,
    minor_axis double
) final instantiable;

-- should fail:  must specify finality
create type square as (
    side_length double
);

-- should fail:  can never specify FINAL+NOT INSTANTIABLE for anything
create type square as (
    side_length double
) final not instantiable;

-- should fail:  can't specify INSTANTIABLE+NOT FINAL for structured type...yet!
create type square as (
    side_length double
) instantiable not final;

-- should fail:  can't specify NOT FINAL for structured type...yet!
create type square as (
    side_length double
) not final;

-- should fail:  can't specify NOT FINAL+NOT INSTANTIABLE for 
-- structured type...yet!
create type square as (
    side_length double
) not final not instantiable;

-- should fail:  at least one attribute required
create type no_attributes as () final;

-- should fail:  unknown attribute type
create type bad_attrtype as (
    x foobar
) final;

-- should fail:  constraints not allowed on attributes
create type bad_constraint as (
    x int not null
) final;

-- should fail:  default value type mismatch
create type bad_default as (
    x double default 'zero'
) final;

-- should fail:  self-referencing nested type
create type linked_list_node as (
    x int,
    next linked_list_node
) final;

-- should fail:  mutual nesting
create schema cardiopulmonary
create type heart as (
    partner lung
) final
create type lung as (
    partner heart
) final
;

-- should fail:  can't specify UDT for distinct type
create type simolean_currency as ellipse final;

-- test out-of-order definitions
create schema musculoskeletal
create type muscle as (
    s skeleton
) final
create type skeleton as (
    x double
) final
;

-- test path resolution and explicit qualification

create type tire as (
    inner_radius double,
    outer_radius double,
    tread double
) final;

create schema spare_types
create type tire as (
    radius double,
    tread double
) final
create type toothbrush as (
    firmness double,
    length double,
    head_angle double,
    electric boolean
) final
;

-- test explicit qualification
create type toiletries as (
    tb spare_types.toothbrush,
    c udttest.circle
) final;

-- should fail:  wrong schema
create type bad_schema as (
    tb udttest.toothbrush
) final;

-- test implicit path lookup

set path 'spare_types';

create type washroom as (
    tb toothbrush
) final;

-- should fail:  not on path
create type line_segment as (
    endpoint1 rectilinear_coord,
    endpoint2 rectilinear_coord
) final;


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

-- test default constructor (null default value)
select t.p.x
from (select new rectilinear_coord() as p from (values(0))) as t;

-- test default constructor (non-null default value)
select t.p.x
from (select new rectilinear_coord0() as p from (values(0))) as t;

-- FIXME:  test nested constructors
-- select t.c.radius, t.c.center.y
-- from (select new circle() as c from (values(0))) as t;

-- test storage

insert into stored_coord_list 
values(new rectilinear_coord0(), new rectilinear_coord0(), 1);

insert into stored_coord_list 
values(new rectilinear_coord0(), new rectilinear_coord0(), 2);

select t.pair_id, t.coord1.x, t.coord2.y 
from stored_coord_list t
order by pair_id;

update stored_coord_list set pair_id=-pair_id;

select t.pair_id, t.coord1.x, t.coord2.y 
from stored_coord_list t
order by pair_id;

delete from stored_coord_list t where t.coord1.x=0 and t.pair_id=-1;

select t.pair_id, t.coord1.x, t.coord2.y 
from stored_coord_list t
order by pair_id;

!set outputformat csv

explain plan for
select scl.pair_id, scl.coord1.y, scl.coord2.x
from stored_coord_list scl;

explain plan for
select v.pair_id, v.coord1.x, v.coord1.y, v.y as y2 
from viewed_coord_list v;
