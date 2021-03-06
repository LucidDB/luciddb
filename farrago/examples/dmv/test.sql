-- $Id$
-- This script exercises package org.eigenbase.dmv
-- (but it doesn't actually verify any output yet)

create schema accounts;

create table accounts.customers(customer_id int not null primary key);

create table accounts.addresses(address_id int not null primary key);

create view accounts.customer_addresses 
as select * from accounts.customers, accounts.addresses;

create schema car_rentals;

create table car_rentals.cars(car_id int not null primary key);

create table car_rentals.contracts(contract_id int not null primary key);

create view car_rentals.customer_rentals as
select * from accounts.customers, car_rentals.contracts, car_rentals.cars;

create schema lodging;

create table lodging.hotels(hotel_id int not null primary key);

create table lodging.cabins(cabin_id int not null primary key);

create view lodging.locations as
select * from lodging.hotels
union all
select * from lodging.cabins;

create view lodging.registrations as
select * from accounts.customers, lodging.locations;

create schema billing;

create view billing.events as
select * from car_rentals.customer_rentals, lodging.registrations;

create view billing.all_addresses as
select * from accounts.addresses, lodging.locations;

create schema dmv_test;

create procedure dmv_test.dmv_render_graphviz(
    foreign_server_name varchar(128),
    lurql_filename varchar(1024),
    transformation_filename varchar(1024),
    dot_filename varchar(1024))
language java
no sql
external name 'class net.sf.farrago.test.DmvTestUdr.renderGraphviz';

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/schemaDependencies.lurql',
    '${FARRAGO_HOME}/examples/dmv/schemaDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/schemaDependencies.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/schemaDependencies.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/objectDependenciesGrouped.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/objectDependencies.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/schemaDependencies.lurql',
    '${FARRAGO_HOME}/examples/dmv/viewsFloating.xml',
    '${FARRAGO_HOME}/examples/dmv/results/viewsFloating.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/customersDownstream.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/customersDownstream.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/eventsUpstream.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/eventsUpstream.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/registrationsUpAndDownStream.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/registrationsUpAndDownStream.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/carRentalsOnly.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/carRentalsOnly.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/carRentalsPlusPeriphery.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependencies.xml',
    '${FARRAGO_HOME}/examples/dmv/results/carRentalsPlusPeriphery.dot');

call dmv_test.dmv_render_graphviz(
    null, 
    '${FARRAGO_HOME}/examples/dmv/carRentalsOnly.lurql',
    '${FARRAGO_HOME}/examples/dmv/objectDependenciesReversed.xml',
    '${FARRAGO_HOME}/examples/dmv/results/carRentalsReversed.dot');
