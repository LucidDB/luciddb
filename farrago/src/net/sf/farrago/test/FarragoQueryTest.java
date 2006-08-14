/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import junit.framework.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.test.*;


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
        List refList = new ArrayList();
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
        Set refSet = new HashSet();
        refSet.add("Eric");
        compareResultSet(refSet);
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
        List refList = new ArrayList();
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
        Set refSet = new HashSet();
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
            ex.getMessage();
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
        Map argMap = new HashMap();
        argMap.put("tableName", tableName);
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();
        Collection result = session.executeLurqlQuery(
                lurql,
                argMap);
        assertEquals(
            1,
            result.size());
        Object obj = result.iterator().next();
        assertTrue(obj instanceof CwmSchema);
        assertEquals(
            schemaName,
            ((CwmSchema) obj).getName());
    }

    /**
     * Tests execution of a LURQL query to check role cycle. If role_2 has been
     * granted to role_1, then role_1 can't be granted to role_2. This query
     * expanded all the roles inherited by a specified input role, the test then
     * scans through the inherited roles to ensure that a second specified role
     * (to be granted to the first specified role) does not exist.
     */
    public void testCheckSecurityRoleCyleLurqlQuery()
        throws Exception
    {
        // CREATE ROLE ROLE_1, ROLE_2
        // GRANT ROLE_2 TO ROLE_1
        // Simulate GRANT ROLE ROLE_1 TO ROLE_2. This should fail.

        stmt.execute("CREATE ROLE ROLE_1");
        stmt.execute("CREATE ROLE ROLE_2");
        stmt.execute("GRANT ROLE ROLE_2 TO ROLE_1");

        // NOTE: now we want to simulate GRANT ROLE ROLE_1 TO ROLE_2.
        // So the grantee is ROLE_2, and the granted role is ROLE_1.
        // For the cycle check, we need to look for paths in the
        // opposite direction, so we reverse the two roles in the
        // call below.
        String lurql =
            FarragoInternalQuery.instance().TestSecurityRoleCycleCheck.str();
        assertTrue(checkLurqlSecurityRoleCycle(lurql, "ROLE_1", "ROLE_2"));
    }

    private boolean checkLurqlSecurityRoleCycle(
        String lurql,
        String granteeName,
        String grantedRoleName)
        throws Exception
    {
        Map argMap = new HashMap();
        argMap.put("granteeName", granteeName);
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = farragoConnection.getSession();
        Collection result = session.executeLurqlQuery(
                lurql,
                argMap);
        Iterator iter = result.iterator();
        while (iter.hasNext()) {
            FemRole role = (FemRole) iter.next();
            if (role.getName().equals(grantedRoleName)) {
                return true;
            }
        }
        return false;
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
            "create function udx.digest(c cursor) " +
            "returns table(c.*, row_digest int) " +
            "language java " +
            "parameter style system defined java " +
            "no sql " +
            "external name 'class net.sf.farrago.test.FarragoTestUDR.digest'";
        stmt.executeUpdate(sql);
            
        sql =
            "select * from " +
            "table(udx.digest(cursor(select * from sales.depts)))";
        
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
    }

    /**
     * Tests that the transaction manager correctly notifies listeners of table
     * accesses.
     */
    public void testTxnMgrListener()
        throws Exception
    {
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
            Arrays.asList(new String[] {
                    "LOCALDB",
                "SALES",
                "DEPTS"
                });

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
