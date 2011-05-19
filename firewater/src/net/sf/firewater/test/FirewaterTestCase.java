/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater.test;

import org.eigenbase.test.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.test.*;
import net.sf.farrago.fwm.distributed.*;

import junit.extensions.*;
import junit.framework.*;

import net.sf.firewater.*;

import com.lucidera.luciddb.test.*;

import java.util.*;

import org.eigenbase.sql.*;

/**
 * FirewaterTestCase is an abstract base for Firewater unit tests.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class FirewaterTestCase extends LucidDbSqlTest
{
    public FirewaterTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    protected void runTest()
        throws Throwable
    {
        if (isSqlTest()) {
            super.runTest();
        } else {
            runTestSuper();
        }
    }

    protected boolean isSqlTest()
    {
        return false;
    }

    // override LucidDbSqlTest
    public static Test wrappedSuite(TestSuite suite)
    {
        TestSetup wrapper =
            new TestSetup(suite) {
                protected void setUp()
                    throws Exception
                {
                    staticSetUp();
                }

                protected void tearDown()
                    throws Exception
                {
                    staticTearDown();
                }
            };
        return wrapper;
    }

    // override LucidDbSqlTest
    public static void staticSetUp()
        throws Exception
    {
        CleanupFactory.setFactory(
            new FirewaterCleanupFactory());
        FarragoTestCase.staticSetUp();
    }
    
    // override LucidDbSqlTest
    public static void staticTearDown()
        throws Exception
    {
        FarragoTestCase.staticTearDown();
        CleanupFactory.resetFactory();
    }

    /**
     * Custom implementation of CleanupFactory.
     */
    private static class FirewaterCleanupFactory extends CleanupFactory
    {
        // override CleanupFactory
        public Cleanup newCleanup(String name) throws Exception
        {
            return new FirewaterCleanup(name);
        }
    }

    /**
     * Custom implementation of Cleanup.
     */
    public static class FirewaterCleanup extends LucidDbCleanup
    {
        public FirewaterCleanup(String name)
            throws Exception
        {
            super(name);
        }

        // override FarragoTestCase
        protected void dropSchemas()
            throws Exception
        {
            super.dropSchemas();

            // now that tables are gone, drop partitions
            // (before super tries to drop data servers which they
            // might be referencing)
            List<String> list = new ArrayList<String>();
            stmt.execute(
                "select \"name\" from sys_cwm.\"Core\".\"ModelElement\" "
                + "where \"mofClassName\" = 'Partition'");
            resultSet = stmt.getResultSet();
            while (resultSet.next()) {
                String name = resultSet.getString(1);
                list.add(name);
            }
            resultSet.close();
            
            Iterator<String> iter = list.iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                String sql =
                    "drop partition " 
                    + SqlDialect.EIGENBASE.quoteIdentifier(name);
                getStmt().execute(sql);
            }
        }
    }
}

// End FirewaterTestCase.java
