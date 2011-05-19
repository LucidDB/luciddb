/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater.test;

import junit.extensions.*;
import junit.framework.*;

import java.io.*;

import net.sf.farrago.test.*;
import net.sf.farrago.util.*;

import com.lucidera.luciddb.test.*;

/**
 * FirewaterSqlTest runs sqlline-based unit tests for Firewater.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterSqlTest extends FirewaterTestCase
{
    public FirewaterSqlTest(String testName)
        throws Exception
    {
        super(testName);
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        runCleanup();
    }
    
    protected boolean isSqlTest()
    {
        return true;
    }

    // override LucidDbSqlTest
    public static Test suite()
        throws Exception
    {
        return gatherSuite(
            FarragoProperties.instance().testFilesetUnitsql.get(true),
            new LucidDbSqlTestFactory() {
                public FarragoTestCase createSqlTest(String testName)
                    throws Exception
                {
                    return new FirewaterSqlTest(testName);
                }
            });
    }

    // override LucidDbSqlTest
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
}

// End FirewaterSqlTest.java
