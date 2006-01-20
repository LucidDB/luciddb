-- $Id$
-- Test DDL for user-defined types

create schema udttest;

set schema 'udttest';

set path 'udttest';

-- basic structured type
create type rectilinear_coord as (
    x double,
    y double
) final;

-- decimal type
create type rectilinear_coord_dec as (
    x decimal(10,4),
    y decimal(9,3)
) final;

-- structured type with default values
create type rectilinear_coord0 as (
    x double default 0,
    y double default 0
) final;

-- structured type with a single user-defined constructor
create type rectilinear_coord_non0 as (
    x double default 100.0,
    y double default 100.0
) final
constructor method rectilinear_coord_non0(x_init double,y_init double) 
returns rectilinear_coord_non0 
self as result
contains sql
;

-- should fail:  constructor declared but not yet defined
values rectilinear_coord_non0(8,8);

-- should fail:  method name mismatch
create specific method rectilinear_coord_nonzero
for rectilinear_coord_non0
begin
    set self.x = z_init; set self.y = y_init; return self
; end;

-- should fail:  unknown param ref
create specific method rectilinear_coord_non0
for rectilinear_coord_non0
begin
    set self.x = z_init; set self.y = y_init; return self
; end;

-- should fail:  unknown target ref
create specific method rectilinear_coord_non0
for rectilinear_coord_non0
begin
    set self.z = x_init; set self.y = y_init; return self
; end;

-- should fail:  bad type assignment
create specific method rectilinear_coord_non0
for rectilinear_coord_non0
begin
    set self.x = cast(x_init as varchar(20)); set self.y = y_init; return self
; end;

-- should succeed
create specific method rectilinear_coord_non0
for rectilinear_coord_non0
begin
    set self.x = x_init; set self.y = y_init; return self
; end;


-- decimal type with constructor
create type rectilinear_coord_non0_dec as (
    x decimal(10,4) default 100.12345,
    y decimal(9, 3) default 100.1234
) final
constructor method rectilinear_coord_non0_dec
    (x_init decimal(10,4),y_init double) 
returns rectilinear_coord_non0_dec 
self as result
contains sql
;

create specific method rectilinear_coord_non0_dec
for rectilinear_coord_non0_dec
begin
    set self.x = x_init; set self.y = cast(y_init as decimal(9, 3))
; return self; end;

-- structured type with overloaded constructors
create type rectilinear_coord_overloaded as (
    x double default 50.0,
    y double default 50.0
) final
constructor method rectilinear_coord_overloaded()
returns rectilinear_coord_overloaded
self as result
contains sql
specific rectilinear_coord_overload0,
constructor method rectilinear_coord_overloaded(x_init double) 
returns rectilinear_coord_overloaded
self as result
contains sql
specific rectilinear_coord_overload1,
constructor method rectilinear_coord_overloaded(x_init double,y_init double) 
returns rectilinear_coord_overloaded
self as result
contains sql
specific rectilinear_coord_overload2
;

create specific method rectilinear_coord_overload0
for rectilinear_coord_overloaded
begin
    set self.x = 100.0; set self.y = 100.0; return self; end;

create specific method rectilinear_coord_overload1
for rectilinear_coord_overloaded
begin
    set self.x = x_init; return self; end;

create specific method rectilinear_coord_overload2
for rectilinear_coord_overloaded
begin
    set self.x = x_init; set self.y = y_init; return self; end;

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

-- should fail:  constructor name must match type name
create type bad_constructor_name as (
    x double,
    y double
) final
constructor method bad_constructor_nombre(x_init double,y_init double) 
returns bad_constructor_name
self as result
contains sql
;

-- should fail:  constructor language must be SQL for now
create type bad_constructor_language as (
    x double,
    y double
) final
constructor method bad_constructor_language(x_init double,y_init double) 
returns bad_constructor_language
self as result
language java
contains sql
;

-- should fail:  constructor must return self type 
create type bad_constructor_type as (
    x double,
    y double
) final
constructor method bad_constructor_type(x_init double,y_init double) 
returns int
self as result
contains sql
;

-- should fail:  conflicting constructor methods
create type duplicate_constructor as (
    x double,
    y double
) final
constructor method duplicate_constructor(x_init double,y_init double) 
returns duplicate_constructor
self as result
contains sql
specific duplicate_constructor1,
constructor method duplicate_constructor(x_init double,y_init double) 
returns duplicate_constructor
self as result
contains sql
specific duplicate_constructor2
;

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
create type oval as ellipse final;

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


-- test UDF's which operate on UDT's

create function slope(c rectilinear_coord_non0)
returns double
contains sql
deterministic
return c.y/c.x;

create function slope_dec(c rectilinear_coord_non0_dec)
returns decimal(10, 4)
contains sql
deterministic
return c.y/c.x;

create function make_coord(x double,y double)
returns rectilinear_coord_non0
contains sql
deterministic
return new rectilinear_coord_non0(x,y);

create function transpose(c rectilinear_coord_non0)
returns rectilinear_coord_non0
contains sql
deterministic
return new rectilinear_coord_non0(c.y,c.x);

values slope(new rectilinear_coord_non0(5,20));

values slope_dec(new rectilinear_coord_non0_dec(5.0,20.0));

select t.c.x, t.c.y from (select make_coord(7,9) as c from (values(0))) t;

select t.c.x, t.c.y from 
(select transpose(new rectilinear_coord_non0(7,9)) as c from (values(0))) t;

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
    coord1 rectilinear_coord0 not null,
    coord2 rectilinear_coord0,
    pair_id int not null primary key
);

-- verify that table may have same name as type
create table rectilinear_coord0(i int not null primary key);

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

-- test default constructor for type that also has user-defined constructor
select t.p.x
from (select new rectilinear_coord_non0() as p from (values(0))) as t;

-- test user-defined constructor
select t.p.x, t.p.y
from (select new rectilinear_coord_non0(20,30) as p from (values(0))) as t;

-- test overloaded default constructor
select t.p.x, t.p.y
from (select new rectilinear_coord_overloaded() as p from (values(0))) as t;

-- test overloaded 1-arg constructor
select t.p.x, t.p.y
from (select new rectilinear_coord_overloaded(10) as p from (values(0))) as t;

-- test overloaded 2-arg constructor
select t.p.x, t.p.y
from (select new rectilinear_coord_overloaded(10,10) as p from (values(0))) t;

-- NOTE:  center is null because circle doesn't define its default value
select t.c.radius, t.c.center.y
from (select new circle() as c from (values(0))) as t;

-- test storage

insert into stored_coord_list 
values(new rectilinear_coord0(), new rectilinear_coord0(), 1);

insert into stored_coord_list 
values(new rectilinear_coord0(), new rectilinear_coord0(), 2);

insert into stored_coord_list 
values(new rectilinear_coord0(), null, 3);

-- FIXME: query which returns structured type doesn't always work
-- select * from stored_coord_list order by pair_id;

-- should fail due to NOT NULL constraint
insert into stored_coord_list 
values(null, null, 4);

select t.pair_id, t.coord1.x, t.coord2.y 
from stored_coord_list t
order by pair_id;

select t.pair_id
from stored_coord_list t
where t.coord2 is null
order by pair_id;

select t.pair_id
from stored_coord_list t
where t.coord2 is not null
order by pair_id;

update stored_coord_list set pair_id=-pair_id;

select t.pair_id, t.coord1.x, t.coord2.y 
from stored_coord_list t
order by pair_id;

delete from stored_coord_list t where t.coord1.x=0 and t.pair_id=-1;

select t.pair_id, t.coord1.x, t.coord2.y 
from stored_coord_list t
order by pair_id;


-- test user-defined orderings

create type playing_card as (
    card_suit char(1),
    card_rank integer
) final
constructor method playing_card(
    card_suit_init char(1),card_rank_init integer)
returns playing_card
self as result
contains sql
specific playing_card
;

create specific method playing_card
for playing_card
begin
    set self.card_suit = card_suit_init; set 
    self.card_rank = card_rank_init; return self
; end;

-- TODO jvs 22-Mar-2005:  once validator stops spewing for this
-- should fail: no user-defined ordering available
-- values playing_card('H',11) = playing_card('H',11);

create ordering for playing_card
equals only by state;

values new playing_card('H',11) = new playing_card('H',11);

values new playing_card('H',11) <> new playing_card('H',11);

values new playing_card('H',11) = new playing_card('D',11);

values new playing_card('H',11) <> new playing_card('D',11);

-- TODO jvs 22-Mar-2005:  once validator stops spewing for this
-- should fail: equals only
-- values new playing_card('H',11) > new playing_card('D',11);

-- should fail:  already defined
create ordering for playing_card
equals only by state;

drop ordering for playing_card;

-- should fail:  already gone
drop ordering for playing_card;

-- define a suit-independent ordering (like in war)
create function map_card_to_card_rank(c playing_card)
returns integer
specific map_card_to_card_rank
contains sql
deterministic
return c.card_rank;

create ordering for playing_card
order full by map
with specific function map_card_to_card_rank;

values new playing_card('H',11) = new playing_card('D',11);

values new playing_card('H',11) <> new playing_card('D',11);

values new playing_card('H',11) > new playing_card('D',11);

values new playing_card('H',11) < new playing_card('D',11);

values new playing_card('H',11) >= new playing_card('D',11);

values new playing_card('H',11) <= new playing_card('D',11);

values new playing_card('H',11) = new playing_card('H',10);

values new playing_card('H',11) <> new playing_card('H',10);

values new playing_card('H',11) > new playing_card('H',10);

values new playing_card('H',11) < new playing_card('H',10);

values new playing_card('H',11) >= new playing_card('H',10);

values new playing_card('H',11) <= new playing_card('H',10);

drop ordering for playing_card;

-- define an ordering with spades as trump suit
create function compare_cards_spades_trump(c1 playing_card,c2 playing_card)
returns integer
specific compare_cards_spades_trump
contains sql
deterministic
return case 
when c1.card_suit='S' and c2.card_suit<>'S' then 1
when c2.card_suit='S' and c1.card_suit<>'S' then -1
when c1.card_rank > c2.card_rank then 1
when c1.card_rank < c2.card_rank then -1
else 0
end;

create ordering for playing_card
order full by relative
with specific function compare_cards_spades_trump;

values new playing_card('H',11) = new playing_card('D',11);

values new playing_card('H',11) <> new playing_card('D',11);

values new playing_card('H',11) > new playing_card('D',11);

values new playing_card('H',11) < new playing_card('D',11);

values new playing_card('H',11) >= new playing_card('D',11);

values new playing_card('H',11) <= new playing_card('D',11);

values new playing_card('H',11) = new playing_card('S',10);

values new playing_card('H',11) <> new playing_card('S',10);

values new playing_card('H',11) > new playing_card('S',10);

values new playing_card('H',11) < new playing_card('S',10);

values new playing_card('H',11) >= new playing_card('S',10);

values new playing_card('H',11) <= new playing_card('S',10);

drop ordering for playing_card;

-- test some negative cases for ordering function mismatch

create function compare_cards_bad_arity()
returns integer
specific compare_cards_bad_arity
contains sql
deterministic
return 0;

create function compare_cards_bad_map_nondeterministic(c playing_card)
returns integer
specific compare_cards_bad_map_nondeterministic
contains sql
not deterministic
return 0;

create function compare_cards_bad_relative_nondeterministic(
    c1 playing_card, c2 playing_card)
returns integer
specific compare_cards_bad_relative_nondeterministic
contains sql
not deterministic
return 0;

create function compare_cards_bad_map_read_write(c playing_card)
returns integer
specific compare_cards_bad_map_read_write
modifies sql data
deterministic
return 0;

create function compare_cards_bad_relative_read_write(
    c1 playing_card, c2 playing_card)
returns integer
specific compare_cards_bad_relative_read_write
modifies sql data
deterministic
return 0;

create function compare_cards_bad_map_return(c playing_card)
returns playing_card
specific compare_cards_bad_map_return
contains sql
deterministic
return new playing_card('C',2);

create function compare_cards_bad_relative_return(
    c1 playing_card, c2 playing_card)
returns double
specific compare_cards_bad_relative_return
contains sql
deterministic
return 0;

-- should fail
create ordering for playing_card
order full by map
with specific function compare_cards_bad_arity;

-- should fail
create ordering for playing_card
order full by relative
with specific function compare_cards_bad_arity;

-- should fail
create ordering for playing_card
order full by map
with specific function compare_cards_bad_map_nondeterministic;

-- should fail
create ordering for playing_card
order full by relative
with specific function compare_cards_bad_relative_nondeterministic;

-- should fail
create ordering for playing_card
order full by map
with specific function compare_cards_bad_map_read_write;

-- should fail
create ordering for playing_card
order full by relative
with specific function compare_cards_bad_relative_read_write;

-- should fail
create ordering for playing_card
order full by map
with specific function compare_cards_bad_map_return;

-- should fail
create ordering for playing_card
order full by relative
with specific function compare_cards_bad_relative_return;


-- verify dependencies

create table card_deck(
    card_position int not null primary key,
    card playing_card not null);

create view card_view
as select card from card_deck;

create ordering for playing_card
order full by relative
with specific function compare_cards_spades_trump;

create view high_card_view
as select card from card_deck where card > new playing_card('S',10);

-- should fail
drop function compare_cards_spades_trump;

-- should fail
drop type playing_card;

-- should fail
drop ordering for playing_card;

drop view high_card_view;

-- now should succeed
drop ordering for playing_card;

-- explicitly drop objects that reference playing_card
drop table card_deck cascade;
drop function map_card_to_card_rank;
drop function compare_cards_spades_trump;
drop function compare_cards_bad_map_nondeterministic;
drop function compare_cards_bad_relative_nondeterministic;
drop function compare_cards_bad_map_read_write;
drop function compare_cards_bad_relative_read_write;
drop function compare_cards_bad_map_return;
drop function compare_cards_bad_relative_return;

-- now should succeed
drop type playing_card;


-- basic distinct types
create type dollar_currency as double;
create type euro_currency as double;

-- should fail:  can't specify NOT INSTANTIABLE for distinct type
create type simolean_currency as double not instantiable;

-- should fail:  can't specify NOT FINAL for distinct type
create type simolean_currency as double not final;

-- should succeed
create type simolean_currency as double final;

-- table with distinct types
create table sears_catalog(
    item_name varchar(128) not null primary key,
    dollar_price dollar_currency not null,
    euro_price euro_currency not null);

-- TODO:  enable the rest of this once cast to UDT is working
-- insert into sears_catalog values
-- ('Jeans',cast(29.99 as dollar_currency),cast(15.99 as euro_currency)),
-- ('Thimble',cast(3.0 as dollar_currency),cast(2.0 as euro_currency));

-- select item_name,cast(euro_price as double) as item_price
-- from sears_catalog
-- order by item_price;

-- create function convert_dollar_to_euro(d dollar_currency)
-- returns euro_currency
-- contains sql
-- deterministic
-- return cast(cast(d as double)/2 as euro_currency);

-- select item_name,
-- cast(euro_price as double) 
--   - cast(convert_dollar_to_euro(dollar_price) as double)
--   as euro_diff
-- from sears_catalog
-- order by item_name;

-- create ordering for dollar_currency
-- equals only by state;

-- select item_name from sears_catalog 
-- where dollar_price > cast(10 as dollar_currency);

-- In Europe, today is Opposite Day.
-- create function negate_euro(e euro_currency)
-- returns double
-- specific negate_euro
-- contains sql
-- deterministic
-- return -cast(e as double);

-- create ordering for euro_currency
-- order full by map
-- with specific function negate_euro;

-- select item_name from sears_catalog
-- where euro_price > cast(10 as euro_currency);


-- test internals of flattening

!set outputformat csv

explain plan for
select scl.pair_id, scl.coord1.y, scl.coord2.x
from stored_coord_list scl;

explain plan for
select v.pair_id, v.coord1.x, v.coord1.y, v.y as y2 
from viewed_coord_list v;
