/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.test.concurrent;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestSuite;

import net.sf.farrago.test.FarragoSqlTest;
import net.sf.farrago.test.FarragoTestCase;
import net.sf.farrago.util.FarragoProperties;

/**
 * FarragoTestConcurrentTest executes a variety of SQL DML and DDL
 * commands via a multi-threaded test harness in an effort to detect
 * errors in concurrent execution.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoTestConcurrentTest
    extends FarragoTestConcurrentScriptedTestCase
{
    //~ Constructors ----------------------------------------------------------

    public FarragoTestConcurrentTest(String name)
        throws Exception
    {
        super(name);
    }
    

    //~ Methods ---------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetConcurrent.get(true),
            new FarragoConcurrentSqlTestFactory() {
                public FarragoTestConcurrentTestCase createSqlTest(
                    String testName) throws Exception
                {
                    return new FarragoTestConcurrentTest(testName);
                }
            });
    }

    // REVIEW: SZ: 10/21/2004: Copied this from FarragoSqlTest.  If
    // that method moved into FarragoTestCase, we could reuse it.
    protected static Test gatherSuite(
        String fileSet,
        FarragoConcurrentSqlTestFactory fac)
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

    protected void runTest()
        throws Exception
    {
        // mask out source control Id
        addDiffMask("\\$Id.*\\$");

        runScript(getName());
    }

    //~ Inner Interfaces ------------------------------------------------------

    public interface FarragoConcurrentSqlTestFactory
    {
        public FarragoTestConcurrentTestCase createSqlTest(String testName)
            throws Exception;
    }
}
