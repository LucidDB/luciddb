create foreign data wrapper test_jdbc
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java;

create server orcl_server
foreign data wrapper test_jdbc
options(
    url 'jdbc:oracle:thin:@akela.lucidera.com:1521:XE',
    user_name 'schoi',
    password 'schoi',
    driver_class 'oracle.jdbc.driver.OracleDriver'
);

create schema orcl_schema;
create schema s;