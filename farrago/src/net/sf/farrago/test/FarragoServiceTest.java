/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.test;

import java.io.PrintWriter;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jmi.reflect.*;
import javax.sql.DataSource;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.test.*;

import net.sf.farrago.namespace.mock.MedMockForeignDataWrapper;
import net.sf.farrago.service.*;

/**
 * Unit test for services.
 *
 * <p>Services ought to work via any driver.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoServiceTest extends FarragoTestCase
{
    private DataSource dataSource;
    private final AtomicInteger openCount = new AtomicInteger();
    private final String libraryFileName;

    /**
     * Creates a FarragoServiceTest.
     *
     * @param testName Test name
     *
     * @throws Exception on error
     */
    public FarragoServiceTest(String testName) throws Exception
    {
        super(testName);
        libraryFileName =
            System.getProperty("net.sf.farrago.home")
            + "/plugin/FarragoMedJdbc.jar";
    }

    @Override
    protected void tearDown() throws Exception
    {
        assertEquals(0, openCount.get());
        super.tearDown();
    }

    /**
     * Returns a data source.
     *
     * @return data source
     */
    protected DataSource getDataSource()
    {
        if (dataSource == null) {
            dataSource = new MyDataSource(connection, openCount);
        }
        return dataSource;
    }

    private String toString(List<DriverPropertyInfo> infoList)
    {
        if (infoList.isEmpty()) {
            return "[]";
        }
        StringBuilder buf = new StringBuilder();
        String sep = "[";
        for (DriverPropertyInfo driverPropertyInfo : infoList) {
            buf.append(sep);
            sep = ", ";
            buf.append(FarragoMedService.toString(driverPropertyInfo));
        }
        buf.append("]");
        return buf.toString();
    }

    // generic version of above
    private <T> String listToString(List<T> list)
    {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder buf = new StringBuilder();
        String sep = "[";
        for (T element : list) {
            buf.append(sep);
            sep = ", ";
            buf.append(element.toString());
        }
        buf.append("]");
        return buf.toString();
    }

    private FarragoMedService getMedService()
    {
        return new FarragoMedService(
            getDataSource(), Locale.getDefault(), tracer);
    }

    /**
     * Data source that always returns the same connection.
     */
    private static class MyDataSource implements DataSource
    {
        private final Connection connection;
        private final AtomicInteger count;

        /**
         * Creates a MyDataSource.
         * @param connection Connection
         * @param count Pointer to the number of times the connection has been
         *    opened without being closed
         */
        MyDataSource(Connection connection, AtomicInteger count)
        {
            this.connection = connection;
            this.count = count;
        }

        public Connection getConnection() throws SQLException
        {
            count.getAndIncrement();
            return (Connection) Proxy.newProxyInstance(
                null,
                new Class<?>[] {Connection.class},
                new InvocationHandler()
                {
                    boolean closed = false;

                    public Object invoke(
                        Object proxy, Method method, Object[] args)
                        throws Throwable
                    {
                        if (method.getName().equals("close")) {
                            if (!closed) {
                                closed = true;
                                count.getAndDecrement();
                            }
                            return null;
                        } else {
                            return method.invoke(connection, args);
                        }
                    }
                }
            );
        }

        public Connection getConnection(
            String username, String password) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public boolean isWrapperFor(Class<?> iface)
        {
            return false;
        }

        public <T> T unwrap(Class<T> iface)
        {
            throw new UnsupportedOperationException();
        }

        public PrintWriter getLogWriter() throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setLogWriter(PrintWriter out) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public void setLoginTimeout(int seconds) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public int getLoginTimeout() throws SQLException
        {
            throw new UnsupportedOperationException();
        }
    }

    // Unit tests follow...

    /**
     * Unit test for {@link FarragoMedService#getServerProperties(String, String, java.util.Map, java.util.Map)}.
     */
    public void testMedServerProperties() throws SQLException
    {
        List<DriverPropertyInfo> infoList =
            getMedService()
                .getServerProperties(
                    UUID.randomUUID().toString(),
                    libraryFileName,
                    Collections.<String, String>emptyMap(),
                    Collections.<String, String>emptyMap());
        assertEquals(
            "["
            + "DriverProperty("
            + "name: DRIVER_CLASS, "
            + "value: null, "
            + "description: Fully-qualified class name of JDBC driver to load, "
            + "choices: null), "
            + "DriverProperty("
            + "name: URL, "
            + "value: null, "
            + "description: JDBC URL for data source, "
            + "choices: null), "
            + "DriverProperty("
            + "name: USER_NAME, "
            + "value: null, "
            + "description: User name for authentication in source DBMS, "
            + "choices: null), "
            + "DriverProperty("
            + "name: PASSWORD, "
            + "value: null, "
            + "description: Password for authentication in source DBMS, "
            + "choices: null), "
            + "DriverProperty("
            + "name: EXTENDED_OPTIONS, "
            + "value: FALSE, "
            + "description: Whether driver-specific options should be accepted, "
            + "choices: [FALSE, TRUE])"
            + "]",
            toString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#getServerProperties(String, String, java.util.Map, java.util.Map)}
     * with non-existent wrapper.
     */
    public void testMedServerPropertiesUnknownWrapper() throws SQLException
    {
        try {
            List<DriverPropertyInfo> infoList =
                getMedService()
                    .getServerProperties(
                        UUID.randomUUID().toString(),
                        libraryFileName + ".bad.jar",
                        Collections.<String, String>emptyMap(),
                        Collections.<String, String>emptyMap());
            fail("expected error, got " + infoList);
        } catch (SQLException e) {
            assertEquals(
                "java.util.zip.ZipException: error in opening zip file",
                e.getMessage());
        }
    }

    /**
     * Unit test for {@link FarragoMedService#getPluginProperties(String, String, java.util.Map)}.
     */
    public void testMedPluginProperties() throws SQLException
    {
        List<DriverPropertyInfo> infoList =
            getMedService()
                .getPluginProperties(
                    UUID.randomUUID().toString(),
                    libraryFileName,
                    Collections.<String, String>emptyMap());
        assertEquals(
            "[]",
            toString(infoList));

        infoList =
            getMedService()
                .getPluginProperties(
                    UUID.randomUUID().toString(),
                    "class " + MedMockForeignDataWrapper.class.getName(),
                    Collections.<String, String>emptyMap());
        assertEquals(
            "["
            + "DriverProperty("
            + "name: EXECUTOR_IMPL, "
            + "value: JAVA, "
            + "description: UNLOCALIZED_EXECUTOR_IMPL_DESCRIPTION, "
            + "choices: [JAVA, FENNEL]), "
            + "DriverProperty("
            + "name: FOO, "
            + "value: null, "
            + "description: UNLOCALIZED_FOO_DESCRIPTION, "
            + "choices: null)"
            + "]",
            toString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#browseTable(String, java.util.Map)}.
     */
    public void testMedBrowseTable() throws SQLException
    {
        List<DriverPropertyInfo> infoList =
            getMedService()
                .browseTable(
                    "SYS_MOCK_FOREIGN_DATA_SERVER",
                    Collections.<String, String>emptyMap());
        assertEquals(
            "["
            + "DriverProperty("
            + "name: Prop1, "
            + "value: null, "
            + "description: UNLOCALIZED_Prop1_DESCRIPTION, "
            + "choices: null), "
            + "DriverProperty("
            + "name: Prop2, "
            + "value: null, "
            + "description: UNLOCALIZED_Prop2_DESCRIPTION, "
            + "choices: null), "
            + "DriverProperty("
            + "name: Prop3, "
            + "value: x, "
            + "description: UNLOCALIZED_Prop3_DESCRIPTION, "
            + "choices: [x, y])"
            + "]",
            toString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#browseColumn(String, java.util.Map, java.util.Map)}.
     */
    public void testMedBrowseColumn() throws SQLException
    {
        List<DriverPropertyInfo> infoList =
            getMedService()
                .browseColumn(
                    "SYS_MOCK_FOREIGN_DATA_SERVER",
                    Collections.<String, String>emptyMap(),
                    Collections.<String, String>emptyMap());
        assertEquals(
            "["
            + "DriverProperty("
            + "name: ColProp1, "
            + "value: null, "
            + "description: UNLOCALIZED_ColProp1_DESCRIPTION, "
            + "choices: null), "
            + "DriverProperty(name: ColProp2, "
            + "value: null, "
            + "description: UNLOCALIZED_ColProp2_DESCRIPTION, "
            + "choices: null), "
            + "DriverProperty(name: ColProp3, "
            + "value: x, "
            + "description: UNLOCALIZED_ColProp3_DESCRIPTION, "
            + "choices: [x, null, y])"
            + "]",
            toString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#browseForeignSchemas(String)}.
     */
    public void testMedBrowseForeignSchemas() throws SQLException
    {
        List<FarragoMedService.ForeignSchemaInfo> infoList =
            getMedService().browseForeignSchemas("HSQLDB_DEMO");
        assertEquals(
            "["
            + "ForeignSchemaInfo(schemaName: INFORMATION_SCHEMA, "
            + "description: null), "
            + "ForeignSchemaInfo(schemaName: PUBLIC, description: null), "
            + "ForeignSchemaInfo(schemaName: SALES, description: null)"
            + "]",
            listToString(infoList));

        infoList =
            getMedService()
            .browseForeignSchemas("SYS_MOCK_FOREIGN_DATA_SERVER");
        assertEquals("[]", listToString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#browseForeignSchemaTables(
     * String, String)}.
     */
    public void testMedBrowseForeignSchemaTables() throws SQLException
    {
        List<FarragoMedService.ForeignSchemaTableAndColumnInfo> infoList =
            getMedService().browseForeignSchemaTables("HSQLDB_DEMO", "SALES");
        assertEquals(
            "["
            + "ForeignSchemaTableAndColumnInfo(tableName: BITFLIP, "
            + "description: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: DEPT, "
            + "description: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "description: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: SALGRADE, "
            + "description: null)"
            + "]",
            listToString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#browseForeignSchemaColumns(
     * String, String)}.
     */
    public void testMedBrowseForeignSchemaColumns() throws SQLException
    {
        List<FarragoMedService.ForeignSchemaTableAndColumnInfo> infoList =
            getMedService().browseForeignSchemaColumns("HSQLDB_DEMO", "SALES");
        assertEquals(
            "["
            + "ForeignSchemaTableAndColumnInfo(tableName: BITFLIP, "
            + "columnName: B1, ordinal: 0, dataType: BOOLEAN, precision: 1, "
            + "decDigits: 0, isNullable: true, formattedDataType: BOOLEAN, "
            + "description: null, defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: BITFLIP, "
            + "columnName: B2, ordinal: 1, dataType: BOOLEAN, precision: 1, "
            + "decDigits: 0, isNullable: false, formattedDataType: BOOLEAN, "
            + "description: null, defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: DEPT, "
            + "columnName: DEPTNO, ordinal: 0, dataType: INTEGER, "
            + "precision: 10, decDigits: 0, isNullable: true, "
            + "formattedDataType: INTEGER, description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: DEPT, "
            + "columnName: DNAME, ordinal: 1, dataType: VARCHAR, "
            + "precision: 1024, decDigits: 0, isNullable: true, "
            + "formattedDataType: VARCHAR(1024), description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: DEPT, "
            + "columnName: LOC, ordinal: 2, dataType: VARCHAR, "
            + "precision: 1024, decDigits: 0, isNullable: true, "
            + "formattedDataType: VARCHAR(1024), description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: EMPNO, ordinal: 0, dataType: INTEGER, "
            + "precision: 10, decDigits: 0, isNullable: true, "
            + "formattedDataType: INTEGER, description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: ENAME, ordinal: 1, dataType: VARCHAR, "
            + "precision: 1024, decDigits: 0, isNullable: true, "
            + "formattedDataType: VARCHAR(1024), description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: JOB, ordinal: 2, dataType: VARCHAR, "
            + "precision: 1024, decDigits: 0, isNullable: true, "
            + "formattedDataType: VARCHAR(1024), description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: MGR, ordinal: 3, dataType: INTEGER, precision: 10, "
            + "decDigits: 0, isNullable: true, formattedDataType: INTEGER, "
            + "description: null, defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: HIREDATE, ordinal: 4, dataType: DATE, precision: 0, "
            + "decDigits: 0, isNullable: true, formattedDataType: DATE, "
            + "description: null, defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: SAL, ordinal: 5, dataType: DECIMAL, precision: 19, "
            + "decDigits: 0, isNullable: true, "
            + "formattedDataType: DECIMAL(19, 0), description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: COMM, ordinal: 6, dataType: DECIMAL, precision: 19, "
            + "decDigits: 0, isNullable: true, "
            + "formattedDataType: DECIMAL(19, 0), description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: EMP, "
            + "columnName: DEPTNO, ordinal: 7, dataType: INTEGER, "
            + "precision: 10, decDigits: 0, isNullable: true, "
            + "formattedDataType: INTEGER, description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: SALGRADE, "
            + "columnName: GRADE, ordinal: 0, dataType: INTEGER, "
            + "precision: 10, decDigits: 0, isNullable: true, "
            + "formattedDataType: INTEGER, description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: SALGRADE, "
            + "columnName: LOSAL, ordinal: 1, dataType: INTEGER, "
            + "precision: 10, decDigits: 0, isNullable: true, "
            + "formattedDataType: INTEGER, description: null, "
            + "defaultValue: null), "
            + "ForeignSchemaTableAndColumnInfo(tableName: SALGRADE, "
            + "columnName: HISAL, ordinal: 2, dataType: INTEGER, "
            + "precision: 10, decDigits: 0, isNullable: true, "
            + "formattedDataType: INTEGER, description: null, "
            + "defaultValue: null)"
            + "]",
            listToString(infoList));
    }

    /**
     * Unit test for {@link FarragoMedService#checkLibraryValid(String, String)}.
     */
    public void testMedIsLibraryValid() throws Exception
    {
        // test succeeds if checkLibraryValid does not throw
        getMedService()
            .checkLibraryValid(
                UUID.randomUUID().toString(),
                libraryFileName);
    }

    /**
     * Unit test for {@link FarragoMedService#checkLibraryValid(String, String)},
     * expecting the answer 'no'.
     */
    public void testMedIsLibraryValidNegative() throws SQLException
    {
        // test succeeds if checkLibraryValid throws
        try {
            getMedService()
                .checkLibraryValid(
                    UUID.randomUUID().toString(),
                    libraryFileName + ".bad.jar");
            fail("expected exception");
        } catch (Exception e) {
            assertEquals(
                "java.util.zip.ZipException: error in opening zip file",
                e.getMessage());
        }
    }

    private FarragoSqlAdvisorService getSqlAdvisorService()
    {
        return new FarragoSqlAdvisorService(getDataSource(), tracer);
    }

    /**
     * Unit test for {@link FarragoSqlAdvisorService#getReservedAndKeyWords()},
     * which should return all SQL92 reserved words, plus any key words defined
     * by the parser, and nothing else.
     * @throws SQLException if database error occurs
     */
    public void testKeywordService() throws SQLException
    {
        List<String> keys = getSqlAdvisorService().getReservedAndKeyWords();
        // did we get all the reserved words?
        final Collection<String> reservedWords =
            SqlAbstractParserImpl.getSql92ReservedWords();
        assertTrue(
            keys.containsAll(reservedWords));
        // OK, so take those out
        keys.removeAll(reservedWords);
        // Check for key words
        Collection<String> keywords =
            Arrays.asList(
                getSession().getDatabaseMetaData().getSQLKeywords().split(","));
        assertTrue(keys.containsAll(keywords));
        // Now take those out, and we should be empty
        keys.removeAll(keywords);
        assertTrue(keys.isEmpty());
    }

    /**
     * Tests the SQL validation in the SQL advisor service. Note that this only
     * tries to make sure the service itself works correctly. Comprehensive SQL
     * validation testing is in {@link SqlValidatorTest}.
     */
    public void testValidationService()
    {
        // Start with one that should validate
        List<FarragoSqlAdvisorService.ValidateErrorInfo> result =
            getSqlAdvisorService().validate("SELECT * FROM SALES.EMPS", null);
        assertNull(result);
        // Use the default schema; should still validate
        result = getSqlAdvisorService().validate("SELECT * FROM EMPS", "SALES");
        assertNull(result);
        // Now one that fails
        result = getSqlAdvisorService().validate("THIS IS NOT SQL", null);
        assertNotNull(result);  // we got something
        assertEquals(1, result.size()); // one error record
        assertEquals(
            "Non-query expression encountered in illegal context",
            result.get(0).getMessage());
    }

    /**
     * Tests the SQL completion hints through the SQL advisor service. Note this
     * does not need to test completion itself comprehensively; that happens in
     * {@link SqlAdvisorTest}. We just need to make sure the service works.
     */
    public void testCompletionService()
    {
        String sql = "select * from sales.^";
        SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
        final String[] replaced = { null };
        List<String> expected = Arrays.asList(
            "Schema(SQLJ)",
            "Schema(INFORMATION_SCHEMA)",
            "Schema(SALES)",
            "Table(DEPTS)",
            "Table(EMPS)",
            "Table(TEMPS)");
        List<FarragoSqlAdvisorService.SqlItem> results = getSqlAdvisorService()
            .getCompletionHints(sap.sql, sap.cursor, replaced, "SALES");
        assertEquals(expected.size(), results.size());
        List<String> strVals = new ArrayList<String>();
        for (FarragoSqlAdvisorService.SqlItem si : results) {
            strVals.add(si.toString());
        }
        assertTrue(expected.containsAll(strVals));
        tracer.warning(replaced[0]);
    }

    /**
     * Tests the SQL formatting through the SQL advisor service. Note this
     * does not need to test the formatter comprehensively; that happens in
     * {@link SqlPrettyWriterTest}. We just need to make sure the service works.
     */
    public void testFormattingService()
    {
        final String select = "select * from sales.emps";
        // Just do something easy, with default format
        String result =
            getSqlAdvisorService().format(select, null);
        assertEquals("SELECT *\nFROM \"SALES\".\"EMPS\"", result);
        // Now test with specified format
        SqlFormatOptions options = new SqlFormatOptions();
        options.setKeywordsLowercase(true);
        options.setQuoteAllIdentifiers(false);
        result = getSqlAdvisorService().format(select, options);
        assertEquals("select *\nfrom SALES.EMPS", result);
    }

    /**
     * Tests the SQL advisor's &quot;isValid&quot; service, which tests for
     * syntactical correctness only. Note this does not need to test the
     * validity checker comprehensively; that happens in {@link SqlAdvisorTest}.
     * We just need to make sure the service works.
     */
    public void testIsValidService()
    {
        // just looking for syntactical correctness
        String select = "select foo from bar";
        boolean result = getSqlAdvisorService().isValid(select);
        assertTrue(result);
        // and check something we know isn't valid
        select = "This is not SQL.";
        result = getSqlAdvisorService().isValid(select);
        assertFalse(result);
    }

    private FarragoLurqlService getLurqlService()
    {
        return new FarragoLurqlService(
            getDataSource(),
            tracer);
    }

    private String formatRefObj(RefBaseObject obj)
    {
        return JmiObjUtil.getMetaObjectName(obj)
            + ":"
            + JmiObjUtil.getAttributeValues((RefObject) obj).get("name")
            + "\n";
    }

    /**
     * Tests LURQL query service. Note this does not need to test LURQL itself
     * comprehensively; that happens in {@link LurqlQueryTest}. We just need to
     * make sure the service works.
     */
    public void testLurqlService()
    {
        Collection<RefBaseObject> objects =
            getLurqlService().executeLurql(
                "select *\n"
                + "  from class Table where name='DEPTS' then (\n"
                + "  follow association ClassifierFeature\n"
                + ");",
                repos.getTransientFarragoPackage());
        assertEquals(1, objects.size());
        String stringVal = "";
        for (RefBaseObject obj : objects) {
            stringVal += formatRefObj(obj);
        }
        assertEquals("LocalTable:DEPTS\n", stringVal);
    }
}

// End FarragoServiceTest.java
