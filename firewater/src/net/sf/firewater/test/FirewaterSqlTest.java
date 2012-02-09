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

import junit.extensions.*;
import junit.framework.*;

import java.io.*;

import net.sf.farrago.test.*;
import net.sf.farrago.util.*;

import com.lucidera.luciddb.test.*;

/**
 * FirewaterSqlTest runs sqlline-based unit tests for Firewater.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterSqlTest extends FirewaterTestCase
{
    public FirewaterSqlTest(String testName)
        throws Exception
    {
        super(testName);
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        runCleanup();
    }
    
    protected boolean isSqlTest()
    {
        return true;
    }

    // override LucidDbSqlTest
    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetUnitsql.get(true),
            new LucidDbSqlTestFactory() {
                public FarragoTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new FirewaterSqlTest(testName);
                }
            });
    }

    // override LucidDbSqlTest
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
}

// End FirewaterSqlTest.java
