/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2003 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.test.*;
import org.eigenbase.util14.*;


/**
 * FarragoQueryTest tests miscellaneous aspects of Farrago query processing
 * which are impossible to test via SQL scripts.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoQueryTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoQueryTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoQueryTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoQueryTest.class);
    }

    /**
     * Tests a query which involves operation on columns.
     */
    public void testPrimitiveColumnOperation()
        throws Exception
    {
        String sql =
            "select deptno*1, deptno/1, deptno+0, deptno-0,"
            + " deptno*deptno, deptno/deptno,deptno"
            + " from sales.emps order by deptno";
        preparedStmt = connection.prepareStatement(sql);
        resultSet = preparedStmt.executeQuery();
        List<String> refList = new ArrayList<String>();
        refList.add("10");
        refList.add("20");
        refList.add("20");
        refList.add("40");
        compareResultList(refList);
    }

    /**
     * Tests a query which involves comparison with VARBINARY values.
     */
    public void testVarbinaryComparison()
        throws Exception
    {
        String sql = "select name from sales.emps where public_key=?";
        preparedStmt = connection.prepareStatement(sql);
        final byte [] bytes = { 0x41, 0x62, 0x63 };
        preparedStmt.setBytes(1, bytes);
        resultSet = preparedStmt.executeQuery();
        Set<String> refSet = new HashSet<String>();
        refSet.add("Eric");
        compareResultSet(refSet);
    }

    /**
     * Tests a query which involves a dynamic parameter in the FROM clause.
     */
    public void testDynamicParamInUdx()
        throws Exception
    {
        String sql = "create schema piu";
        stmt.execute(sql);
        sql = "create function piu.ramp(n int) returns table(i int) "
            + "language java parameter style system defined java no sql "
            + "external name 'class net.sf.farrago.test.FarragoTestUDR.ramp'";
        stmt.execute(sql);

        sql = "select * from table(piu.ramp(cast(? as integer)))";
        preparedStmt = connection.prepareStatement(sql);
        preparedStmt.setInt(1, 3);
        resultSet = preparedStmt.executeQuery();
        List<String> refList = new ArrayList<String>();
        refList.add("0");
        refList.add("1");
        refList.add("2");
        compareResultList(refList);
        preparedStmt.close();
        preparedStmt = null;

        sql = "drop schema piu cascade";
        stmt.execute(sql);
    }

    /**
     * Tests a query which involves sorting VARBINARY values.
     */
    public void testOrderByVarbinary()
        throws Exception
    {
        String sql =
            "select name,public_key from sales.emps" + " order by public_key";
        resultSet = stmt.executeQuery(sql);
        List<String> refList = new ArrayList<String>();
        refList.add("Wilma");
        refList.add("Eric");
        refList.add("Fred");
        refList.add("John");
        compareResultList(refList);
    }

    /**
     * Tests a query using a different catalog.
     */
    public void testSetCatalog()
        throws Exception
    {
        String sql = "set catalog 'sys_cwm'";
        stmt.execute(sql);
        sql = "select \"name\" from \"Relational\".\"Schema\"";
        resultSet = stmt.executeQuery(sql);
        Set<String> refSet = new HashSet<String>();
        refSet.add("INFORMATION_SCHEMA");
        refSet.add("JDBC_METADATA");
        refSet.add("MGMT");
        refSet.add("SALES");
        refSet.add("SQLJ");
        refSet.add("SYS_BOOT");
        compareResultSet(refSet);

        // restore default catalog
        sql = "set catalog 'localdb'";
        stmt.execute(sql);
    }

    /**
     * Tests execution of an internal LURQL query defined in a resource file.
     */
    public void testInternalLurqlQuery()
        throws Exception
    {
        String lurql = FarragoInternalQuery.instance().TestQuery.str();

        checkLurqlTableSchema(lurql, "DEPTS", "SALES");
        checkLurqlTableSchema(lurql, "CATALOGS_VIEW", "JDBC_METADATA");
    }

    /**
     * Verifies non-standard behavior preventing more than one statement active
     * at a time in autocommit mode.
     */
    public void testAutocommitCursorLimit()
        throws Exception
    {
        // TODO jvs 20-Mar-2006:  move this test to FarragoJdbcTest
        // after that gets refactored.

        String sql = "select name from sales.depts";
        Statement stmt2 = null;
        connection.setAutoCommit(true);
        try {
            // First, open a cursor.
            resultSet = stmt.executeQuery(sql);

            // Now, try to open another one on the same connection while the
            // first one is still open: should fail.
            stmt2 = connection.createStatement();
            resultSet = stmt2.executeQuery(sql);
        } catch (SQLException ex) {
            // verify expected error message
            Assert.assertTrue(
                "Expected message about cursor still open but got '"
                + ex.getMessage() + "'",
                ex.getMessage().indexOf("cursor is still open") > -1);
        } finally {
            if (stmt2 != null) {
                stmt2.close();
            }
            connection.setAutoCommit(false);
        }
    }

    /**
     * Verifies that multiple statements can be active when not in autocommit
     * mode.
     */
    public void testNoAutocommitCursorLimit()
        throws Exception
    {
        String sql = "select name from sales.depts";
        Statement stmt2 = null;
        try {
            // First, open a cursor.
            resultSet = stmt.executeQuery(sql);

            // Now, try to open another one on the same connection while the
            // first one is still open.
            stmt2 = connection.createStatement();
            resultSet = stmt2.executeQuery(sql);
        } finally {
            if (stmt2 != null) {
                stmt2.close();
            }
        }
    }

    private void checkLurqlTableSchema(
        String lurql,
        String tableName,
        String schemaName)
        throws Exception
    {
        repos.beginReposSession();
        repos.beginReposTxn(false);
        try {
            Map<String, String> argMap = new HashMap<String, String>();
            argMap.put("tableName", tableName);
            FarragoJdbcEngineConnection farragoConnection =
                (FarragoJdbcEngineConnection) connection;
            FarragoSession session = farragoConnection.getSession();
            Collection<RefObject> result =
                session.executeLurqlQuery(
                    lurql,
                    argMap);
            assertEquals(
                1,
                result.size());
            RefObject obj = result.iterator().next();
            assertTrue(obj instanceof CwmSchema);
            assertEquals(
                schemaName,
                ((CwmSchema) obj).getName());
        } finally {
            repos.endReposTxn(false);
            repos.endReposSession();
        }
    }

    public void testAbandonedResultSet()
        throws Exception
    {
        // Start a query that returns N rows where N > 1.  Get the first
        // row and walk away.  This is similar to a query timeout.

        String sql = "select deptno, name from sales.depts";
        preparedStmt = connection.prepareStatement(sql);

        // won't trigger this, but it causes a different type of result set
        preparedStmt.setQueryTimeout(60);

        resultSet = preparedStmt.executeQuery();

        try {
            if (!resultSet.next()) {
                fail("Query has no rows!");
            }
        } finally {
            resultSet.close();
            resultSet = null;
        }
    }

    /**
     * Tests relational expression metadata derivation via
     * FarragoSession.analyzeSql.
     */
    public void testRelMetadata()
        throws Exception
    {
        // This test currently requires Volcano in order to succeed.
        stmt.executeUpdate(
            "alter session implementation add jar"
            + " sys_boot.sys_boot.volcano_plugin");

        String sql =
            "select deptno, max(name) from sales.depts"
            + " where name like '%E%G' group by deptno";
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();
        FarragoSessionAnalyzedSql analyzedSql =
            session.analyzeSql(
                sql,
                new FarragoTypeFactoryImpl(session.getRepos()),
                null,
                true);

        Set<RelColumnOrigin> rcoSet = analyzedSql.columnOrigins.get(0);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "DEPTS",
            "DEPTNO",
            false);

        rcoSet = analyzedSql.columnOrigins.get(1);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "DEPTS",
            "NAME",
            true);

        // By default, tables without stats are assumed to have
        // 100 rows, and default selectivity assumption is 25% for a
        // LIKE predicate. The single GROUP BY column gives a further 10%
        // selectivity. 100 * 25% * 10% = 2.5.
        assertEquals(2.5, analyzedSql.rowCount);

        stmt.executeUpdate(
            "alter session implementation set default");
    }

    public void testUdxRelMetadata()
        throws Exception
    {
        String sql = "create schema udx";
        stmt.executeUpdate(sql);

        sql =
            "create function udx.digest(c cursor) "
            + "returns table(c.*, row_digest int) "
            + "language java "
            + "parameter style system defined java "
            + "no sql "
            + "external name 'class net.sf.farrago.test.FarragoTestUDR.digest'";
        stmt.executeUpdate(sql);

        sql =
            "select * from "
            + "table(udx.digest(cursor(select * from sales.depts)))";

        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();
        FarragoSessionAnalyzedSql analyzedSql =
            session.analyzeSql(
                sql,
                new FarragoTypeFactoryImpl(session.getRepos()),
                null,
                false);

        Set<RelColumnOrigin> rcoSet = analyzedSql.columnOrigins.get(0);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "DEPTS",
            "DEPTNO",
            true);

        rcoSet = analyzedSql.columnOrigins.get(1);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "DEPTS",
            "NAME",
            true);

        rcoSet = analyzedSql.columnOrigins.get(2);
        assertEquals(
            0,
            rcoSet.size());
        sql = "drop schema udx cascade";
        stmt.executeUpdate(sql);
    }

    public void testUdxReturnsColListParamRelMetadata()
        throws Exception
    {
        String sql = "create schema udx";
        stmt.executeUpdate(sql);

        sql =
            "create function udx.returnInput(inputCursor cursor, "
            + "columnSubset select from inputCursor) "
            + "returns table(columnSubset.*) "
            + "language java "
            + "parameter style system defined java "
            + "no sql "
            + "external name "
            + "'class net.sf.farrago.test.FarragoTestUDR.returnInput'";
        stmt.executeUpdate(sql);

        sql =
            "select * from "
            + "table(udx.returnInput(cursor(select * from sales.emps), "
            + "row(name, empno, age)))";

        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();
        FarragoSessionAnalyzedSql analyzedSql =
            session.analyzeSql(
                sql,
                new FarragoTypeFactoryImpl(session.getRepos()),
                null,
                false);

        Set<RelColumnOrigin> rcoSet = analyzedSql.columnOrigins.get(0);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "EMPS",
            "NAME",
            true);

        rcoSet = analyzedSql.columnOrigins.get(1);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "EMPS",
            "EMPNO",
            true);

        rcoSet = analyzedSql.columnOrigins.get(2);
        assertEquals(
            1,
            rcoSet.size());
        RelMetadataTest.checkColumnOrigin(
            rcoSet.iterator().next(),
            "EMPS",
            "AGE",
            true);

        sql = "drop schema udx cascade";
        stmt.executeUpdate(sql);
    }

    /**
     * Tests that the transaction manager correctly notifies listeners of table
     * accesses.
     */
    public void testTxnMgrListener()
        throws Exception
    {
        // make sure we're starting a new txn
        connection.commit();

        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();
        FarragoSessionTxnMgr txnMgr = session.getTxnMgr();
        TxnListener listener = new TxnListener();
        txnMgr.addListener(listener);
        try {
            String sql = "select * from sales.depts";
            resultSet = stmt.executeQuery(sql);
            resultSet.close();
            connection.commit();
        } finally {
            txnMgr.removeListener(listener);
        }

        List<String> expectedName =
            Arrays.asList(
                "LOCALDB",
                "SALES",
                "DEPTS");

        assertEquals(
            "begin",
            listener.events.get(0));
        assertEquals(
            expectedName,
            listener.events.get(1));
        assertEquals(
            TableAccessMap.Mode.READ_ACCESS,
            listener.events.get(2));
        assertEquals(
            FarragoSessionTxnEnd.COMMIT,
            listener.events.get(3));
    }

    /**
     * Tests that ResultSet.getObject(int) implementation in farrago returns a
     * distinct object for a ZonelessDateTime type column in each new row.
     *
     * @throws Exception
     */
    public void testDateTimeOverLocalJdbc()
        throws Exception
    {
        String createStmt =
            "create table sales.datetable"
            + "(keycol int not null primary key, datecol date)";

        String dropStmt = " drop table sales.datetable";

        String insertStmt1 =
            "insert into sales.datetable values(0, DATE '2007-07-07')";

        String insertStmt2 =
            "insert into sales.datetable values(1, DATE '2007-07-08')";

        String selectStmt1 =
            "select * from (values cast('2007-06-22' as date), cast('2007-06-23' as date))";

        String selectStmt2 = "select t.datecol from sales.datetable as t";

        String selectStmt3 =
            "select t.datecol from sales.datetable as t where 1 = 1";

        stmt.executeUpdate(createStmt);
        stmt.executeUpdate(insertStmt1);
        stmt.executeUpdate(insertStmt2);

        Object obj1, obj2;

        try {
            // FarragoTupleIterResultSet with a CompoundTupleIter fetching
            // from input iterators returning onw row each.
            resultSet = stmt.executeQuery(selectStmt1);
            resultSet.next();
            obj1 = resultSet.getObject(1);
            resultSet.next();
            obj2 = resultSet.getObject(1);
            assertFalse(obj1 == obj2);
            resultSet.close();

            // FennelOnlyResultSet
            resultSet = stmt.executeQuery(selectStmt2);
            resultSet.next();
            obj1 = resultSet.getObject(1);
            resultSet.next();
            obj2 = resultSet.getObject(1);
            assertFalse(obj1 == obj2);
            resultSet.close();

            // FarragoTupleIterResultSet
            resultSet = stmt.executeQuery(selectStmt3);
            resultSet.next();
            obj1 = resultSet.getObject(1);
            resultSet.next();
            obj2 = resultSet.getObject(1);
            assertFalse(obj1 == obj2);
            resultSet.close();
        } finally {
            resultSet.close();
            stmt.executeUpdate(dropStmt);
        }
    }

    public void testLobTextUdxNull()
        throws Exception
    {
        String schemaName = "SALES";

        String refMofId = getSchemaMofId(schemaName, null);

        String actualDescription = fetchLobText(refMofId, "description");
        assertNull(actualDescription);
    }

    public void testLobTextUdxEmptyString()
        throws Exception
    {
        // Create a schema with an empty string as its description.
        stmt.execute(
            "CREATE SCHEMA EMPTY_DESC DESCRIPTION ''");

        StringBuilder descriptionOut = new StringBuilder();
        String refMofId = getSchemaMofId("EMPTY_DESC", descriptionOut);

        assertEquals("", descriptionOut.toString());

        String actualDescription = fetchLobText(refMofId, "description");
        assertEquals("", actualDescription);

        stmt.execute("DROP SCHEMA EMPTY_DESC");
    }

    public void testLobTextUdxOneChunk()
        throws Exception
    {
        String schemaName = "SALES";

        String refMofId = getSchemaMofId(schemaName, null);

        String actualSchemaName = fetchLobText(refMofId, "name");
        assertEquals("SALES", actualSchemaName);
    }

    public void testLobTextUdxMultiChunk()
        throws Exception
    {
        // Create a very long string.
        char [] x = new char[10000];
        Arrays.fill(x, 'X');
        String description = new String(x);

        // Create a schema with this string as its description.
        stmt.execute(
            "CREATE SCHEMA LONG_DESC DESCRIPTION '" + description + "'");

        StringBuilder descriptionBuf = new StringBuilder();
        String refMofId = getSchemaMofId("LONG_DESC", descriptionBuf);
        Assert.assertEquals(description, descriptionBuf.toString());

        String actualDescription = fetchLobText(refMofId, "description");
        assertEquals(description, actualDescription);

        stmt.execute("DROP SCHEMA LONG_DESC");
    }

    private String getSchemaMofId(
        String schemaName,
        StringBuilder descriptionOut)
    {
        String refMofId;
        repos.beginReposSession();
        repos.beginReposTxn(false);
        try {
            CwmCatalog catalog = repos.getSelfAsCatalog();
            FemLocalSchema schema =
                (FemLocalSchema) FarragoCatalogUtil.getModelElementByName(
                    catalog.getOwnedElement(),
                    schemaName);

            if (descriptionOut != null) {
                descriptionOut.setLength(0);
                descriptionOut.append(schema.getDescription());
            }

            refMofId = schema.refMofId();
        } finally {
            repos.endReposTxn(false);
            repos.endReposSession();
        }

        return refMofId;
    }

    private String fetchLobText(String mofId, String attributeName)
        throws Exception
    {
        String sql =
            "select chunk_offset, chunk_text from table("
            + "sys_boot.mgmt.repository_lob_text('" + mofId + "', '"
            + attributeName + "')) order by chunk_offset";
        StringBuilder sb = new StringBuilder();
        try {
            resultSet = stmt.executeQuery(sql);
            assertTrue(resultSet.next());
            do {
                int offset = resultSet.getInt(1);
                String chunk = resultSet.getString(2);
                if (chunk == null) {
                    assertEquals(0, sb.length());
                    assertEquals(-1, offset);
                    return null;
                }
                assertEquals(sb.length(), offset);
                sb.append(chunk);
            } while (resultSet.next());
        } finally {
            resultSet.close();
        }
        return sb.toString();
    }

    public void testNoNativeTraceLeak()
        throws Exception
    {
        // LER-7367 (leaks from loggers for native Segments and ExecStreams);
        // note that if you have xo tracing enabled, this test will fail
        // (see FRG-309)
        resultSet =
            stmt.executeQuery(
                "select * from (values(0)) order by 1");
        resultSet.next();
        resultSet.close();
        Enumeration<String> e = LogManager.getLogManager().getLoggerNames();
        while (e.hasMoreElements()) {
            String s = e.nextElement();
            if (!s.startsWith("net.sf.fennel.xo")) {
                continue;
            }

            // The # character is part of the per-object logger, which
            // is not supposed to exist, so we expect to not see it.
            assertEquals(-1, s.indexOf('#'));
        }
    }

    public void testUnicodeLiteral()
        throws Exception
    {
        // Note that here we are constructing a SQL statement which directly
        // contains Unicode characters (not SQL Unicode escape sequences).  The
        // escaping here is Java-only, so by the time it gets to the SQL
        // parser, the literal already contains Unicode characters.
        String sql = "values U&'"
            + ConversionUtil.TEST_UNICODE_STRING + "'";
        Set<String> refSet = new HashSet<String>();
        refSet.add(ConversionUtil.TEST_UNICODE_STRING);
        resultSet = stmt.executeQuery(sql);
        compareResultSet(refSet);
    }

    public void testUnencodableUnicodeLiteral()
    {
        // Negative test for Unicode characters without a Unicode
        // introducer and no explicit _UTF16 character set
        // (so ISO-8859-1 is used by default, and it can't encode
        // the given characters).
        String sql = "values '" + ConversionUtil.TEST_UNICODE_STRING + "'";
        try {
            stmt.executeQuery(sql);
            Assert.fail("Expected error about encoding.");
        } catch (SQLException ex) {
            // verify expected error message
            Assert.assertTrue(
                "Expected message about encoding but got '"
                + ex.getMessage() + "'",
                ex.getMessage().indexOf("Failed to encode") > -1);
        }
    }

    /**
     * Tests the fix for LDB-240.
     */
    public void testIsNullWithDynamicParam()
        throws Exception
    {
        String sql =
            "select * from sales.depts where deptno is null and ? is null";
        preparedStmt = connection.prepareStatement(sql);
        preparedStmt.setInt(1, 1);
        preparedStmt.executeQuery();
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class TxnListener
        implements FarragoSessionTxnListener
    {
        List<Object> events;

        TxnListener()
        {
            events = new ArrayList<Object>();
        }

        public void transactionBegun(
            FarragoSession session,
            FarragoSessionTxnId txnId)
        {
            events.add("begin");
        }

        public void tableAccessed(
            FarragoSessionTxnId txnId,
            List<String> localTableName,
            TableAccessMap.Mode accessType)
        {
            events.add(localTableName);
            events.add(accessType);
        }

        public void transactionEnded(
            FarragoSessionTxnId txnId,
            FarragoSessionTxnEnd endType)
        {
            events.add(endType);
        }
    }
}

// End FarragoQueryTest.java
