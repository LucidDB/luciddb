create schema warehouse;
set schema 'warehouse';

create table employee_dimension(
    emp_key int generated always as identity not null primary key,
    empno int not null,
    ename varchar(128) not null,
    dname varchar(128) not null,
    job varchar(128) not null,
    unique(empno)
);

create table calendar_dimension(
    calendar_key int generated always as identity not null primary key,
    calendar_date date not null,
    is_weekend boolean not null,
    unique(calendar_date)
);
create index calendar_weekend_idx on calendar_dimension(is_weekend);

create table timesheet_fact(
    timesheet_key int generated always as identity not null primary key,
    emp_key int not null,
    workday_key int not null,
    hours_worked smallint not null,
    unique(emp_key, workday_key)
);
create index timesheet_workday_idx on timesheet_fact(workday_key);
create index timesheet_hours_worked_idx on timesheet_fact(hours_worked);

