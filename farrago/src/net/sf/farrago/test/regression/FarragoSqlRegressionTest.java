/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.test.regression;

import net.sf.farrago.util.*;
import net.sf.farrago.test.*;

import junit.framework.Test;

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
    public FarragoSqlRegressionTest(String testName) throws Exception
    {
        super(testName);
    }

    public static Test suite() throws Exception
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

    protected void runTest() throws Exception {
        addDiffMask("\\$Id.*\\$");
        stmt.execute(FarragoCalcSystemTest.vmFennel);
        runSqlLineTest(getName());
//        stmt.execute(FarragoCalcSystemTest.vmJava);
//        runSqlLineTest(getName());

        //reset to use java
        stmt.execute(FarragoCalcSystemTest.vmJava);
    }
}
