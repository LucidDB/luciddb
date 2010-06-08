create or replace schema "SS";
set schema 'SS';

--normal case
create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(1);
values SS.test_routine(255);
select "name","url","deploymentState" from sys_fem.sql2003."Jar" where "name"='TEST_JAR' order by "name";
drop jar test_jar options(1) CASCADE;
select "name","url","deploymentState" from sys_fem.sql2003."Jar" where "name"='TEST_JAR' order by "name";
values SS.test_routine(255);

--the deployment descriptor statements need to be executed with the default schema set to the schema containing the jar 
create or replace schema "TT";
create jar "TT".test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(1);
values "TT".test_routine(255);
drop jar "TT".test_jar options(1) CASCADE;
drop schema "TT" cascade;

--failure cases
create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(1);
create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(1);

drop jar test_jar1 options(1) CASCADE;
drop jar test_jar options(1) CASCADE;

--bad sql deployment descriptor.


drop schema "SS" cascade;

