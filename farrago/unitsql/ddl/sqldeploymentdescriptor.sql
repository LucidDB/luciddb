create or replace schema "SS";
set schema 'SS';

create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(0);
values SS.test_routine(255);
select "name","url","deploymentState" from sys_fem.sql2003."Jar" where "name"='TEST_JAR' order by "name";
drop jar test_jar options(0) CASCADE;
select "name","url","deploymentState" from sys_fem.sql2003."Jar" where "name"='TEST_JAR' order by "name";
values SS.test_routine(255);

--failure cases
create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(0);
create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar' options(0);

create jar test_jar library 'file:${FARRAGO_HOME}/unitsql/ddl/sqldeploymentdescriptor/sddtest.jar1' options(0);

drop jar test_jar1 options(0) CASCADE;
drop jar test_jar options(0) CASCADE;

drop schema "SS" cascade;
