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
package net.sf.farrago.test.regression;

import junit.framework.*;

import net.sf.farrago.test.*;
import net.sf.farrago.util.*;


/**
 * FarragoSqlRegressionTest is a JUnit harness for executing tests which are
 * implemented by running an SQL script and diffing the output against a
 * reference file containing the expected results.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class FarragoSqlRegressionTest
    extends FarragoSqlTest
{
    //~ Constructors -----------------------------------------------------------

    public FarragoSqlRegressionTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetRegression.get(true),
            new FarragoSqlTestFactory() {
                public FarragoTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new FarragoSqlRegressionTest(testName);
                }
            });
    }

    protected void runTest()
        throws Exception
    {
        // mask out source control Id and other data sections that are not
        // pertinent to the test being performed.
        setRefFileDiffMasks();

        // Need to have a specific pair comparison.
        // only both matches then it passes.
        addDiffMask("Error: .*\\(state=,code=0\\)"); // java error msg
        addDiffMask(
            "Error: could not calculate results for the following row:");

        /*
        addDiffMask("2891E"); addDiffMask("2889E");
         addDiffMask("199999999996E"); addDiffMask("200000000003E");
         addDiffMask("4000E"); addDiffMask("4003E");
         addIgnorePattern("\\[.*\\]"); // fennel data row
         addIgnorePattern("Messages:"); // fennel message
         addIgnorePattern("\\[0\\].*\\(state=,code=0\\)");  // fennel error code
         */
        setGC(100);
        stmt.execute(
            FarragoCalcSystemTest.VirtualMachine.Fennel
            .getAlterSystemCommand());
        runSqlLineTest(getName());
    }
}

// End FarragoSqlRegressionTest.java
