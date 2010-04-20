-- Test SQLJ Using a Deployment Descriptor.
create or replace schema "S";
set schema 'S';

CALL sqlj.install_jar('file:${FARRAGO_HOME}/unitsql/ddl/ddlsqljtest.jar', 'ddlsqlj_jar', 1);
select "name","url" from sys_fem.sql2003."Jar";
select * from S.T;
select * from S.T2;

CALL sqlj.remove_jar('DDLSQLJ_JAR', 1);
select "name","url" from sys_fem.sql2003."Jar";
select * from S.T;

drop schema "S" cascade;
