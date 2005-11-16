CREATE foreign table csv_schema.SHORTREG_DEPT_SRC (
DEPTNO INTEGER,
DNAME VARCHAR(20),
LOCID VARCHAR(2)
)
server csv_server
options (table_name 'dept');

INSERT INTO s.DEPT
SELECT DEPTNO,DNAME,LOCID
FROM csv_schema.SHORTREG_DEPT_SRC;