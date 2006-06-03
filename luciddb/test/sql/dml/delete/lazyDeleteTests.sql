set schema 'DT2';

create table fish (
  id int not null, 
  name varchar(20), 
  is_ocean boolean,
  location char(10)
);

-- delete on empty table
delete from fish;

insert into fish values(0, 'Salmon', true, 'NA');
insert into fish values(1, 'Mackerel', true, 'fridge');
insert into fish values(2, 'catfish', false, 'stomach');
insert into fish values(3, 'goldfish', false, 'trash');

select * from fish order by 1,2,3,4;

-- check that deletion index exists
select * from sys_boot.jdbc_metadata.index_info_view 
where table_name = 'FISH'
order by 1,2,3,6;

-- try to drop the deletion index (should fail)
drop index SYS$DELETION_INDEX$DT2$FISH;

-- delete and recreate entries
delete from fish where is_ocean=true;
select * from fish;

create index idx_fish_is_ocean on fish(is_ocean);
insert into fish values(0, 'Salmon', true, 'NA');
insert into fish values(1, 'Mackerel', true, 'fridge');
delete from fish where is_ocean=true;
select * from fish where is_ocean=true;

delete from fish where is_ocean=false;
select count(*) from fish;

insert into fish values(0, 'Salmon', true, 'NA');
insert into fish values(1, 'Mackerel', true, 'fridge');
drop index idx_fish_is_ocean;
insert into fish values(2, 'catfish', false, 'stomach');
insert into fish values(3, 'goldfish', false, 'trash');

select count(*) from fish;

insert into fish values(0, 'Salmon', true, 'NA');
insert into fish values(1, 'Mackerel', true, 'fridge');
insert into fish values(2, 'catfish', false, 'stomach');
insert into fish values(3, 'goldfish', false, 'trash');

delete from fish;
select * from fish where is_ocean=true;

-- delete with join
create table places(location char(10), description varchar(100));
insert into places values 
('NA', 'North America'), 
('fridge', 'green'),
('trash', 'stainless steel');

insert into fish values(0, 'Salmon', true, 'NA');
insert into fish values(1, 'Mackerel', true, 'fridge');
insert into fish values(2, 'catfish', false, 'stomach');
insert into fish values(3, 'goldfish', false, 'trash');

delete from fish where fish.location in
(select distinct fish.location 
 from fish inner join places 
 on fish.location = places.location
 where description <> 'North America');

select * from fish
order by 1,2,3,4;