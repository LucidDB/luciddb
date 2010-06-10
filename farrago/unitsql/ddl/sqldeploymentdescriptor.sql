create schema "SS";
set schema 'SS';

-- normal case
create jar test_jar 
library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' 
options(1);
values SS.test_routine(255);
select "name","url","deploymentState" from sys_fem.sql2003."Jar" 
where "name"='TEST_JAR' order by "name";
drop jar test_jar options(1) CASCADE;

-- both jar and routine should be gone
select "name","url","deploymentState" from sys_fem.sql2003."Jar" 
where "name"='TEST_JAR' order by "name";
values SS.test_routine(255);

-- repeat using equivalent sqlj routines
call sqlj.install_jar(
'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar',
'test_jar', 1);
values SS.test_routine(255);
select "name","url","deploymentState" from sys_fem.sql2003."Jar" 
where "name"='TEST_JAR' order by "name";
-- this should fail due to implicit RESTRICT
call sqlj.remove_jar('test_jar', 0);
-- but this should work
call sqlj.remove_jar('test_jar', 1);
select "name","url","deploymentState" from sys_fem.sql2003."Jar" 
where "name"='TEST_JAR' order by "name";
values SS.test_routine(255);


-- this time, do not run deployment action, so routine should not exist
create jar test_jar 
library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' 
options(0);
values SS.test_routine(255);
select "name","url","deploymentState" from sys_fem.sql2003."Jar" 
where "name"='TEST_JAR' order by "name";
drop jar test_jar options(0) CASCADE;

-- this time, run deployment action, but do not run undeployment action;
-- in the middle, replace test_routine with a new one so that we can make
-- sure the CASCADE doesn't drop it either
create jar test_jar 
library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' 
options(1);
values SS.test_routine(255);
drop function test_routine;
create function test_routine(i int)
returns varchar(128)
contains sql
return cast(i*2 as varchar(128));
drop jar test_jar options(0) CASCADE;
values SS.test_routine(255);
drop function test_routine;

-- the deployment descriptor statements need to be executed 
-- with the default schema set to the schema containing the jar 
create schema "TT";
create jar "TT".test_jar 
library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' 
options(1);
values "TT".test_routine(255);
drop jar "TT".test_jar options(1) CASCADE;

-- negative case:  bad sql deployment descriptor,
-- so jar should not be created
!set shownestederrs true

create jar bad_jar 
library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/bad-sddtest.jar' 
options(1);

select "name","url","deploymentState" from sys_fem.sql2003."Jar" 
where "name"='BAD_JAR' order by "name";

-- both should fail since first function creation should have been 
-- rolled back too
values ss.bad_routine(255);
values ss.good_routine(255);
