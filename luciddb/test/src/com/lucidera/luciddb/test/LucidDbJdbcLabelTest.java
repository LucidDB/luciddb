/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.test;

import java.sql.*;
import java.util.*;

import org.luciddb.test.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.test.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.property.*;


/**
 * LucidDB JDBC test for testing label settings
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LucidDbJdbcLabelTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    public LucidDbJdbcLabelTest(String testname)
        throws Exception
    {
        super(testname);
    }

    //~ Methods ----------------------------------------------------------------
    
    public void setUp()
        throws Exception
    {
        // Set the properties so the LucidDB session factory and LucidDB data
        // files are used.  The LucidDB data files need to be used; otherwise,
        // we won't use versioned data segment pages.
        //
        // REVIEW zfong 7/11/08 - Is there a better way of doing this?
        FarragoProperties farragoPropInstance = FarragoProperties.instance();
        StringProperty sessionFactory =
            farragoPropInstance.defaultSessionFactoryLibraryName;
        System.setProperty(
            sessionFactory.getPath(),
            "class:org.luciddb.session.LucidDbSessionFactory");
        String homeDirString = farragoPropInstance.homeDir.get(true);
        String catalogDir = homeDirString + "/../luciddb/catalog";
        farragoPropInstance.catalogDir.set(catalogDir);
        
        // Create a new connection so we're sure we have the right session
        // factory and db
        if (connection != null) {
            connection.close();
        }
        
        connection = newConnection();
        repos = getSession().getRepos();
        saveParameters();
        
        runCleanup();
        super.setUp();
    }
    
    public static void runCleanup()
        throws Exception
    {
        // Use the special LucidDB cleanup factory to avoid dropping schemas
        // like APPLIB
        FarragoTestCase.CleanupFactory.setFactory(new LucidDbCleanupFactory());
        FarragoTestCase.runCleanup();
    }
    
    public void tearDown()
        throws Exception
    {
        // Close the connection created by this test.
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    public void testInvalidLabelInConnectString()
        throws Exception
    {
        final String driverURI = "jdbc:luciddb:";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();
        Properties props = newProperties();
        props.setProperty("label", "foo");
        try {
            driver.connect(driverURI, props);     
            fail(
                "connection should fail because of invalid label");
        } catch (Exception ex) {
            FarragoJdbcTest.assertExceptionMatches(
                ex,
                ".*Invalid label property.*");
        }
    }
    
    public void testLabelSetting()
        throws Exception
    {
        final String driverURI = "jdbc:luciddb:";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();
        Properties props = newProperties();
        
        // Create a label based on an empty table
        Connection conn = driver.connect(driverURI, props);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("create schema s");
        stmt.executeUpdate("set schema 's'");
        stmt.executeUpdate("create table t(a int)");
        stmt.executeUpdate("create label l");
        stmt.executeUpdate("insert into t values(1)");
        stmt.close();
        conn.close();
            
        // Set up a connection based on that label and select from the
        // table.  It should return no rows.
        props.setProperty("label", "L");
        props.setProperty("schema", "S");
        conn = driver.connect(driverURI, props);
        stmt = conn.createStatement();
        ResultSet rset = stmt.executeQuery("select * from t");
        assertTrue("expected no rows", !rset.next());
        stmt.close();
        conn.close();
        
        // Now create a connection without the label setting and make sure
        // the query now returns 1 row.
        props.remove("label");
        props.setProperty("schema", "S");
        conn = driver.connect(driverURI, props);
        stmt = conn.createStatement();
        rset = stmt.executeQuery("select * from t");
        assertTrue("expected a row", rset.next());
        stmt.close();
        conn.close();
    }
    
    /**
     * Creates test connection properties.
     */
    private static Properties newProperties()
    {
        Properties props = new Properties();
        props.put("user", FarragoCatalogInit.SA_USER_NAME);
        props.put("password", "");
        return props;
    }
    
    /**
     * Cleanup factory that uses LucidDbTestCleanup
     */
    private static class LucidDbCleanupFactory
        extends FarragoTestCase.CleanupFactory
    {
        public Cleanup newCleanup(String name)
            throws Exception
        {
            return new LucidDbTestCleanup(connection);
        }
    }
}

// End LucidDbJdbcLabelTest.java
