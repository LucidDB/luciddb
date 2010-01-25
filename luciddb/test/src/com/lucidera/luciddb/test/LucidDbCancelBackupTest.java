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
import java.util.logging.*;

import org.luciddb.test.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.test.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.property.*;


/**
 * LucidDB JDBC test for testing cancel during system backup.  In order for
 * this test to run successfully, there must already be a schema named
 * RWCONCUR that contains enough data so a system backup of the Fennel data
 * requires more than few seconds.  The data was created by a backup test in
 * test/sql/concurrency/readwrite.  Those tests have also created the directory
 * where the backup in this test will be written.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LucidDbCancelBackupTest
    extends FarragoTestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getTestTracer();

    //~ Instance fields --------------------------------------------------------
    
    private String testDir;

    //~ Constructors -----------------------------------------------------------

    public LucidDbCancelBackupTest(String testname)
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
        FarragoProperties farragoPropInstance = FarragoProperties.instance();
        StringProperty sessionFactory =
            farragoPropInstance.defaultSessionFactoryLibraryName;
        System.setProperty(
            sessionFactory.getPath(),
            "class:org.luciddb.session.LucidDbSessionFactory");
        String homeDirString = farragoPropInstance.homeDir.get(true);
        String catalogDir = homeDirString + "/../luciddb/catalog";
        farragoPropInstance.catalogDir.set(catalogDir);

        testDir = 
            homeDirString + "/../luciddb/test/sql/concurrency/backupRestore";
        
        // Create a new connection with the right session factory and db
        final String driverURI = "jdbc:luciddb:";
        FarragoAbstractJdbcDriver driver =
            FarragoTestCase.newJdbcEngineDriver();
        Properties props = newProperties();
        
        connection = driver.connect(driverURI, props);
        stmt = connection.createStatement();
    }
    
    public static void runCleanup()
        throws Exception
    {
        // Use a special cleanup factory to avoid dropping schemas
        // like APPLIB and the schema used by this test.
        FarragoTestCase.CleanupFactory.setFactory(new LucidDbCleanupFactory());
        FarragoTestCase.runCleanup();
    }
    
    public void tearDown()
        throws Exception
    {
        // Close the stmt and connection created by this test.
        if (stmt != null) {
            stmt.close();
            stmt = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
    
    /**
     * Initiates a backup but cancels it before it completes.
     */
    public void testCancelBackup()
        throws Exception
    {
        resultSet =
            stmt.executeQuery(
                "select count(*) from sys_root.dba_system_backups");
        int beforeCount = 0;
        if (resultSet.next()) {
            beforeCount = resultSet.getInt(1);
        } else {
            fail("count query on backup catalog failed");
        }

        String archiveDir = testDir + "/fullArchive2";
        executeAndCancel(
            "call sys_root.backup_database('" + 
                archiveDir + "', 'FULL', 'UNCOMPRESSED')",
            3000);
        
        // Make sure there are no records marked as PENDING in the backup
        // catalog and the number of records in the backup catalog is the
        // same as before the backup.
        resultSet =
            stmt.executeQuery(
                "select status from sys_root.dba_system_backups");
        int afterCount = 0;
        while (resultSet.next()) {
            afterCount++;
            String status = resultSet.getString(1);
            if (status.equals("PENDING")) {
                fail("PENDING status found in backup catalog");
            }
        }
        if (beforeCount != afterCount) {
            fail("wrong number of records in backup catalog");
        }
    }
    
    private void executeAndCancel(String sql, int waitMillis)
        throws SQLException
    {
        if (waitMillis == 0) {
            // cancel immediately
            stmt.cancel();
        } else {
            // Schedule timer to cancel after waitMillis
            Timer timer = new Timer(true);
            TimerTask task =
                new TimerTask() {
                    public void run()
                    {
                        Thread thread = Thread.currentThread();
                        thread.setName("FarragoJdbcCancelThread");
                        try {
                            tracer.fine(
                                "TimerTask "
                                + toStringThreadInfo(thread)
                                + " will cancel " + stmt);
                            stmt.cancel();
                        } catch (SQLException ex) {
                            fail(
                                "Cancel request failed:  "
                                + ex.getMessage());
                        }
                    }
                };
            tracer.fine("scheduling cancel task with delay=" + waitMillis);
            timer.schedule(task, waitMillis);
        }
        try {
            stmt.executeUpdate(sql);
        } catch (SQLException ex) {
            // expected
            assertTrue(
                "Expected statement canceled message but got '"
                + ex.getMessage() + "'",
                checkCancelException(ex));
            return;
        }
        fail("Expected failure due to cancel request");
    }
    
    /**
     * Returns string representation of thread info.
     */
    private String toStringThreadInfo(Thread thread)
    {
        if (thread == null) {
            thread = Thread.currentThread();
        }
        StringBuffer buf = new StringBuffer();
        buf.append("thread[");
        buf.append(thread.isInterrupted() ? "INT" : "!int");
        buf.append(",").append(thread.getId());
        buf.append(",").append(thread.getName());
        buf.append("]");
        return buf.toString();
    }

    private boolean checkCancelException(SQLException ex)
    {
        return (ex.getMessage().indexOf("abort") > -1);
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
     * Cleanup factory that uses LucidDbCancelTestCleanup.
     */
    private static class LucidDbCleanupFactory
        extends FarragoTestCase.CleanupFactory
    {
        public Cleanup newCleanup(String name)
            throws Exception
        {
            return new LucidDbCancelTestCleanup(connection);
        }
    }

    /**
     * Overrides LucidDbTestCleanup by avoiding the drop of the RWCONCUR
     * schema, which contains the volume of data needed for the backup
     * initiated by this test to run for a non-trivial amount of time.
     */
    private static class LucidDbCancelTestCleanup
        extends LucidDbTestCleanup
    {
        public LucidDbCancelTestCleanup(Connection ldbConn)
            throws Exception
        {
            super(ldbConn);
        }
        
        public boolean isBlessedSchema(CwmSchema schema)
        {
            String name = schema.getName();
            if (name.equals("RWCONCUR")) {
                return true;
            }
            return super.isBlessedSchema(schema);
        }
    }
}

// End LucidDbCancelBackupTest.java
