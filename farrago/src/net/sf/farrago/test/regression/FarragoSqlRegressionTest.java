/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
