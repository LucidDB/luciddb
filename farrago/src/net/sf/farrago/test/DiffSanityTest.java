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
package net.sf.farrago.test;

import java.io.*;

import junit.framework.*;

import org.eigenbase.test.*;


/**
 * DiffSanityTest verifies that the facilities in DiffTestCase are actually
 * working and not just happily rubber-stamping away.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DiffSanityTest
    extends DiffTestCase
{
    //~ Constructors -----------------------------------------------------------

    public DiffSanityTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // override DiffTestCase
    protected File getTestlogRoot()
        throws Exception
    {
        return FarragoTestCase.getTestlogRootStatic();
    }

    /**
     * Negative test. I checked in a mismatching .ref file to make sure the diff
     * gets detected.
     */
    public void testDiff()
        throws Exception
    {
        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        pw.println("a");
        pw.println("b");
        pw.println("c");
        pw.flush();
        try {
            diffTestLog();
        } catch (AssertionFailedError ex) {
            // expected
            return;
        }
        Assert.fail("Expected failure due to diff");
    }

    /**
     * Negative test. Purposefully do NOT create a .ref file, and make sure its
     * absence gets detected.
     */
    public void testMissingRefFile()
        throws Exception
    {
        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        pw.println("a");
        pw.flush();
        try {
            diffTestLog();
        } catch (AssertionFailedError ex) {
            // expected
            return;
        }
        Assert.fail("Expected failure due to missing ref file");
    }

    /**
     * Positive test. The .ref file is good.
     */
    public void testNoDiff()
        throws Exception
    {
        Writer writer = openTestLog();
        PrintWriter pw = new PrintWriter(writer);
        pw.println("a");
        pw.println("b");
        pw.println("c");
        pw.flush();
        diffTestLog();
    }

    /**
     * This is here to verify that Java assert is correctly enabled during
     * testing.
     */
    public void testJavaAssert()
    {
        boolean asserted = true;
        try {
            assert (false);
            asserted = false;
        } catch (AssertionError ex) {
            return;
        }
        fail("Java assert failure undetected");
    }
}

// End DiffSanityTest.java
