/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
public class DiffSanityTest extends DiffTestCase
{
    //~ Constructors ----------------------------------------------------------

    public DiffSanityTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // override DiffTestCase
    protected File getTestlogRoot()
        throws Exception
    {
        return FarragoTestCase.getTestlogRootStatic();
    }
    
    /**
     * Negative test.  I checked in a mismatching .ref file to make sure
     * the diff gets detected.
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
     * Negative test.  Purposefully do NOT create a .ref file, and make
     * sure its absence gets detected.
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
     * Positive test.  The .ref file is good.
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
