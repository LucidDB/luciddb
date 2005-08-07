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

import net.sf.farrago.session.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.security.*;

import java.sql.*;
import java.util.*;

import junit.framework.*;

import org.eigenbase.jmi.*;

/**
 * FarragoQueryTest tests miscellaneous aspects of Farrago query
 * processing which are impossible to test via SQL scripts.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoQueryTest extends FarragoTestCase
{
    //~ Constructors ----------------------------------------------------------

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

    //~ Methods ---------------------------------------------------------------

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
            "select deptno*1, deptno/1, deptno+0,deptno-0,deptno*deptno,deptno/deptno,deptno"
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
        String lurql = FarragoInternalQuery.instance().getTestQuery();

        checkLurqlTableSchema(lurql, "DEPTS", "SALES");
        checkLurqlTableSchema(lurql, "CATALOGS_VIEW", "JDBC_METADATA");
    }

    private void checkLurqlTableSchema(
        String lurql, String tableName, String schemaName)
        throws Exception
    {
        Map argMap = new HashMap();
        argMap.put("tableName", tableName);
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoSession session = (FarragoSession)
            farragoConnection.getSession();
        Collection result = session.executeLurqlQuery(
            lurql, argMap);
        assertEquals(1, result.size());
        Object obj = result.iterator().next();
        assertTrue(obj instanceof CwmSchema);
        assertEquals(schemaName, ((CwmSchema) obj).getName());
    }

    // REVIEW jvs 6-Aug-2005:  Tai, please fix and uncomment these
    // or remove them.

    /**
     * Tests execution of a LURQL query to check role cycle. If role_2 has been
     * granted to role_1,  then role_1 can't be granted to role_2.
     * This query expanded all the roles inherited by a specified input role,
     * the test then scans through the inherit roles to ensure that a second
     * specified role (to be granted to the first specified role) does not
     * exist.
     */
    /*
     public void testCheckSecurityRoleCyleLurqlQuery()
         throws Exception
     {
         // Create Role_1,  Role_2
         // Grant Role_2 to Role_1
         // Grant Role_1 to Role_2. This should fail

         // TODO: remove this temporary setting of the session current user
         // once we have a proper login i.e. login user exists in the database
         FarragoJdbcEngineConnection farragoConnection =
             (FarragoJdbcEngineConnection) connection;
         FarragoSession session = (FarragoSession)
             farragoConnection.getSession();
         session.getSessionVariables().currentUserName = "_SYSTEM";
        
         stmt.execute("create Role ROLE_1");
         stmt.execute("create Role ROLE_2");
         stmt.execute("grant role ROLE_2 to ROLE_1");
        
         String lurql =
             FarragoInternalQuery.instance().getTestSecurityRoleCycleCheck();
         assertTrue(checkLurqlSecurityRoleCycle(lurql,  "ROLE_1",  "ROLE_2"));
     }
    
     private boolean checkLurqlSecurityRoleCycle(
         String lurql, String granteeName, String grantedRoleName)
         throws Exception
     {
         Map argMap = new HashMap();
         argMap.put("granteeName", granteeName);
         FarragoJdbcEngineConnection farragoConnection =
             (FarragoJdbcEngineConnection) connection;
         FarragoSession session = (FarragoSession)
             farragoConnection.getSession();
         Collection result = session.executeLurqlQuery(
             lurql, argMap);
         Iterator iter = result.iterator();
         while(iter.hasNext())
         {
             FemRole role = (FemRole) iter.next();
             if (role.getName().equals(grantedRoleName)) {
                 return true;
             }
         }
         return false;
     }
    */
}

// End FarragoQueryTest.java
