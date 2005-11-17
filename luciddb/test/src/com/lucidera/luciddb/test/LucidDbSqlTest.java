/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.luciddb.test;

import java.io.*;
import junit.framework.*;
import net.sf.farrago.util.*;


/**
 * LucidDbSqlTest refines {@link net.sf.farrago.test.FarragoSqlTest} 
 * with LucidDB-specific test infrastructure.
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class LucidDbSqlTest extends LucidDbTestCase
{

    public LucidDbSqlTest(String testName)
        throws Exception
    {
        super(testName);
    }
    

    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetUnitsql.get(true),
            new LucidDbSqlTestFactory() {
                public LucidDbTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new LucidDbSqlTest(testName);
                }
            });
    }

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
    
    protected void runTest()
        throws Exception
    {
        runSqlLineTest(getName());
    }

    public interface LucidDbSqlTestFactory
    {
        public LucidDbTestCase createSqlTest(String testName)
            throws Exception;
    }
}
