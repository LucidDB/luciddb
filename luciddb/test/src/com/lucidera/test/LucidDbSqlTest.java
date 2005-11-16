package com.lucidera.test;

import java.io.*;
import junit.framework.*;
import net.sf.farrago.util.*;


/**
 *
 * See: //open/lu/dev/farrago/src/net/sf/farrago/test/FarragoSqlTest.java 
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
