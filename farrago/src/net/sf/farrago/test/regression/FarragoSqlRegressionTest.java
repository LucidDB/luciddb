/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import junit.framework.Test;

import net.sf.farrago.test.*;
import net.sf.farrago.util.*;


/**
 * FarragoSqlRegressionTest is a JUnit harness for executing tests which are implemented
 * by running an SQL script and diffing the output against a reference
 * file containing the expected results.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class FarragoSqlRegressionTest extends FarragoSqlTest
{
    //~ Constructors ----------------------------------------------------------

    public FarragoSqlRegressionTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

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
        addDiffMask("\\$Id.*\\$");
        /*
        addDiffMask("Error: .*\\(state=,code=0\\)"); // java error msg
        addDiffMask("Error: could not calculate results for the following row:");
        addDiffMask("3\\.1415927886962891E");
        addDiffMask("3\\.1415927886962889E");
        addIgnorePattern("\\[.*\\]"); // fennel data row
        addIgnorePattern("Messages:"); // fennel message
        addIgnorePattern("\\[0\\].*\\(state=,code=0\\)");  // fennel error code
        */
        stmt.execute(FarragoCalcSystemTest.VirtualMachine.Fennel
              .getAlterSystemCommand());
        runSqlLineTest(getName());

        // stmt.execute(FarragoCalcSystemTest.VirtualMachine.Java
        //      .getAlterSystemCommand());
        //  runSqlLineTest(getName());
    }
}
