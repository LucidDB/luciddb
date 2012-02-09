/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
