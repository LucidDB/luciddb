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
package com.lucidera.luciddb.test;

import java.io.*;
import junit.extensions.*;
import junit.framework.*;
import net.sf.farrago.util.*;
import net.sf.farrago.test.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;

/**
 * LucidDbSqlTest refines {@link net.sf.farrago.test.FarragoSqlTest} 
 * with LucidDB-specific test infrastructure.
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class LucidDbSqlTest extends FarragoTestCase
{

    public LucidDbSqlTest(String testName)
        throws Exception
    {
        super(testName);
    }
    

    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetUnitsql.get(true),
            new LucidDbSqlTestFactory() {
                public FarragoTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new LucidDbSqlTest(testName);
                }
            });
    }

    protected static Test gatherSuite(
        String fileSet,
        LucidDbSqlTestFactory fac)
        throws Exception
    {
        StringReader stringReader = new StringReader(fileSet);
        LineNumberReader lineReader = new LineNumberReader(stringReader);
        TestSuite suite = new TestSuite();
        for (;;) {
            String file = lineReader.readLine();
            if (file == null) {
                break;
            }
            suite.addTest(fac.createSqlTest(file));
        }
        return wrappedSuite(suite);
    }
    
    // override FarragoTestCase
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
    
    // override FarragoTestCase
    public static void staticSetUp()
        throws Exception
    {
        CleanupFactory.setFactory(
            new LucidDbCleanupFactory());
        FarragoTestCase.staticSetUp();
    }
    
    // override FarragoTestCase
    public static void staticTearDown()
        throws Exception
    {
        FarragoTestCase.staticTearDown();
        CleanupFactory.resetFactory();
    }

    protected void runTest()
        throws Throwable
    {
        // mask out source control Id, etc
        setRefFileDiffMasks();
        runSqlLineTest(getName());
    }

    protected void runTestSuper()
        throws Throwable
    {
        super.runTest();
    }

    public interface LucidDbSqlTestFactory
    {
        public FarragoTestCase createSqlTest(String testName)
            throws Exception;
    }
    
    /**
     * Custom implementation of CleanupFactory.
     */
    private static class LucidDbCleanupFactory extends CleanupFactory
    {
        // override CleanupFactory
        public Cleanup newCleanup(String name) throws Exception
        {
            return new LucidDbCleanup(name);
        }
    }
    
    /**
     * Custom implementation of Cleanup.
     */
    public static class LucidDbCleanup extends Cleanup
    {
        public LucidDbCleanup(String name)
            throws Exception
        {
            super(name);
        }

        // override Cleanup
        protected boolean isBlessedSchema(CwmSchema schema)
        {
            String name = schema.getName();
            return name.equals("APPLIB")
                || name.equals("FOODMART")
                || super.isBlessedSchema(schema);
        }

        // override Cleanup
        protected boolean isBlessedWrapper(FemDataWrapper wrapper)
        {
            String name = wrapper.getName();
            return name.equals("ORACLE")
                || name.equals("SQL SERVER")
                || name.equals("FLAT FILE")
                || name.equals("LUCIDDB LOCAL")
                || name.equals("LUCIDDB REMOTE")
                || name.equals("SALESFORCE")
                || name.contains("NETSUITE")
                || super.isBlessedWrapper(wrapper);
        }

        // override Cleanup
        protected boolean isBlessedAuthId(FemAuthId authId)
        {
            String name = authId.getName();
            return super.isBlessedAuthId(authId);
        }
    }

}

// End LucidDbSqlTest.java
