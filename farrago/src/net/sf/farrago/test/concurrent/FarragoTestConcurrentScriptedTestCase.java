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
package net.sf.farrago.test.concurrent;

import java.io.*;
import java.util.*;
import org.eigenbase.test.concurrent.*;

/**
 * FarragoTestConcurrentScriptedTestCase is a base class for multi-threaded,
 * scripted tests. Subclasses must implement the suite() method in order to get
 * a database connection correctly initialized.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class FarragoTestConcurrentScriptedTestCase
    extends FarragoTestConcurrentTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoTestConcurrentScriptedTestCase object.
     *
     * @param testName .
     *
     * @throws Exception .
     */
    protected FarragoTestConcurrentScriptedTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Executes the given multi-threaded test script.
     */
    protected void runScript(String mtsqlFile, String jdbcURL) throws Exception
    {
        ConcurrentTestCommandScript cmdGen =
            newScriptedCommandGenerator(mtsqlFile);
        if (cmdGen.isDisabled()) {
            return;
        }
        setDataSource(cmdGen, jdbcURL);
        innerExecuteTest(cmdGen, cmdGen.useLockstep());

        File mtsqlFileBase =
            new File(mtsqlFile.substring(0, mtsqlFile.length() - 6));
        OutputStream outStream = openTestLogOutputStream(mtsqlFileBase);
        BufferedWriter out =
            new BufferedWriter(new OutputStreamWriter(outStream));
        cmdGen.printResults(out);
        diffTestLog();
    }
}

// End FarragoTestConcurrentScriptedTestCase.java
