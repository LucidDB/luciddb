-- $Id$
-- Test queries on Oracle's scott/tiger schema via the MED JDBC adapter.
--
-- To make it work you will need Oracle installed and running, and the standard
-- scott/tiger schema installed. You also need to add your driver jar, say
-- /usr/lib/oracle/xe/app/oracle/product/10.2.0/server/jdbc/lib/ojdbc14.jar
-- in classpath.gen.
--
-- Naturally not every machine running the regression test suite will
-- have Oracle installed, so this test is disabled in build.xml by default.

CREATE SERVER my_oracle_server
  FOREIGN DATA WRAPPER SYS_JDBC
  OPTIONS ( 
    driver_class 'oracle.jdbc.driver.OracleDriver', 
    url 'jdbc:oracle:thin:@localhost:1521:xe', 
    user_name 'scott', 
    password 'tiger');
CREATE SCHEMA orcl;
IMPORT FOREIGN SCHEMA scott
  FROM SERVER my_oracle_server
  INTO orcl;
SELECT * FROM orcl.emp;
SET SCHEMA 'orcl';
SELECT count(*) FROM emp;
SELECT count(*) FROM emp WHERE deptno > 30;
SELECT sum(empno), deptno, count(*) FROM emp GROUP BY deptno;
SELECT 1 FROM emp HAVING max(empno) > 4;
SELECT min(empno) FROM emp GROUP BY deptno HAVING max(empno) > 4;
SELECT * FROM emp LEFT JOIN dept ON emp.deptno = dept.deptno;
SELECT * FROM emp RIGHT JOIN dept ON emp.deptno = dept.deptno;
SELECT * FROM emp FULL JOIN dept ON emp.deptno = dept.deptno;
-- Gives error. Disabled pending bug FRG-324.
--SELECT * FROM dept WHERE EXISTS (
--  SELECT 1 FROM emp WHERE emp.deptno = dept.deptno AND sal >= 800);
-- Gives error. Disabled pending bug FRG-324.
--SELECT * FROM emp WHERE deptno IN (
--  SELECT deptno FROM dept WHERE sal >= 800);
SELECT empno FROM emp
UNION ALL
SELECT deptno FROM dept;

-- End scott.sql

