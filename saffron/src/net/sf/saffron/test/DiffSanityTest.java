/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.test;

import junit.framework.*;

import java.io.*;

/**
 * DiffSanityTest verifies that the facilities in DiffTestCase are actually
 * working and not just happily rubber-stamping away.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DiffSanityTest extends DiffTestCase
{
    public DiffSanityTest(String testName) throws Exception
    {
        super(testName);
    }

    /**
     * Negative test.  I checked in a mismatching .ref file to make sure
     * the diff gets detected.
     */
    public void testDiff() throws Exception
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
     * Negative test.  Purposefully do NOT create a .ref file, and make
     * sure its absence gets detected.
     */
    public void testMissingRefFile() throws Exception
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
     * Positive test.  The .ref file is good.
     */
    public void testNoDiff() throws Exception
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
