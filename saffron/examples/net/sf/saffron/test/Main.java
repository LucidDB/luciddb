/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Technologies, Inc.
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

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import net.sf.saffron.oj.xlat.SqlToOpenjavaConverter;
import net.sf.saffron.sql2rel.SqlToRelConverter;
import net.sf.saffron.util.Graph;
import net.sf.saffron.util.OptionsListTest;
import net.sf.saffron.util.SaffronProperties;
import sales.InMemorySalesTestCase;
import sales.JdbcSalesTestCase;
import sales.SalesInMemorySchemaTestCase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Vector;
import java.util.regex.Pattern;


/**
 * Main test suite for saffron.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 18 April, 2002
 */
public class Main extends TestCase
{
    //~ Constructors ----------------------------------------------------------

    public Main(String s)
    {
        super(s);
    }

    Main()
    {
        super("saffron");
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @see #getErrorMessage(Throwable,boolean)
     */
    public static String getErrorMessage(Throwable err)
    {
        boolean prependClassName =
            !(err instanceof java.sql.SQLException
                || (err.getClass() == java.lang.Exception.class));
        return getErrorMessage(err,prependClassName);
    }

    /**
     * Constructs the message associated with an arbitrary Java error, making
     * up one based on the stack trace if there is none.
     *
     * @param err the error
     * @param prependClassName should the error be preceded by the class name
     *        of the Java exception?  defaults to false, unless the error is
     *        derived from {@link java.sql.SQLException} or is exactly a
     *        {@link java.lang.Exception}
     */
    public static String getErrorMessage(
        Throwable err,
        boolean prependClassName)
    {
        String errMsg = err.getMessage();
        if ((errMsg == null) || (err instanceof RuntimeException)) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            err.printStackTrace(pw);
            return sw.toString();
        } else {
            if (prependClassName) {
                return err.getClass().getName() + ": " + errMsg;
            } else {
                return errMsg;
            }
        }
    }

    /**
     * Converts an error into an array of strings, the most recent error
     * first.
     *
     * @param e the error; may be null. Errors may be chained.
     */
    public static String [] convertStackToString(Throwable e)
    {
        Vector v = new Vector();
        while (e != null) {
            String sMsg = getErrorMessage(e);
            v.addElement(sMsg);

            //			if (e instanceof ChainableThrowable) {
            //				e = ((ChainableThrowable) e).getNextThrowable();
            //			} else {
            e = null;

            //			}
        }
        String [] msgs = new String[v.size()];
        v.copyInto(msgs);
        return msgs;
    }

    static public void main(String [] args) throws Exception
    {
        boolean success = new Main().run(args);

        if (!success) {
            // signal failure to the outside world (specifically to
            // the continuous integration builds)
            System.exit(-1);
        }
    }

    /**
     * Creates the main saffron test suite. This method has a special meaning
     * to JUnit; see {@link TestCase}. It uses the following properties:
     * 
     * <ul>
     * <li>
     * {@link SaffronProperties#testName} is a comma-separated list of tests
     * (method names) to run within {@link SaffronProperties#testClass}.
     * If not specified or empty, run all tests.
     * </li>
     * <li>
     * {@link SaffronProperties#testClass} is the name of a
     * test class. It must implement {@link junit.framework.Test}.
     * </li>
     * <li>
     * {@link SaffronProperties#testSuite} is the name of a
     * class which has a method <code>public static {@link Test}
     * suite()</code>. The harness executes that method, and runs the
     * resulting suite.
     * </li>
     * <li>
     * If {@link SaffronProperties#testEverything} is true,
     * all of the previous parameters are ignored, and the harness runs the
     * whole suite.
     * </li>
     * </ul>
     */
    public static Test suite() throws Exception
    {
        SaffronProperties properties = SaffronProperties.instance();
        String testName = properties.testName.get();
        String testClass = properties.testClass.get();
        String testSuite = properties.testSuite.get();
        boolean testEverything = properties.testEverything.get();
        TestSuite suite = new TestSuite();
        if (testEverything) {
            addAllTests(suite);
            return suite;
        }
        if (testClass != null) {
            Class clazz = Class.forName(testClass);
            suite.addTestSuite(clazz);
        } else if (testSuite != null) {
            // e.g. testSuite = "saffron.ext.ObjectSchema". Class does not
            // necessarily implement Test. We call its 'public static Test
            // suite()' method.
            Class clazz = Class.forName(testSuite);
            Method method = clazz.getMethod("suite",new Class[0]);
            Object o = method.invoke(null,new Object[0]);
            suite.addTest((Test) o);
        } else {
            addAllTests(suite);
        }
        if (testName != null) {
            // Filter the suite, so that only tests whose names match
            // "testName" (in its entirety) will be run.
            Pattern testPattern = Pattern.compile(testName);
            suite = SaffronTestCase.copySuite(suite,testPattern);
        }
        return suite;
    }

    private static void addAllTests(TestSuite suite) throws Exception {
        suite.addTest(net.sf.saffron.util.Util.suite());
        if (false) {
            suite.addTestSuite(JdbcSalesTestCase.class);
            // TODO: enable
            suite.addTestSuite(InMemorySalesTestCase.class);
            // TODO: enable
            suite.addTestSuite(SalesInMemorySchemaTestCase.class);
        }
        suite.addTestSuite(
            net.sf.saffron.runtime.BufferedIterator.Test.class);
        suite.addTestSuite(
            net.sf.saffron.runtime.ThreadIterator.Test.class);
        if (false) {
            // TODO: enable
            suite.addTest(net.sf.saffron.ext.ObjectSchema.suite());
        }
        suite.addTestSuite(net.sf.saffron.sql.parser.SqlParserTest.class);
        suite.addTest(SqlToOpenjavaConverter.suite());
        suite.addTestSuite(JdbcTest.class);
        suite.addTestSuite(Graph.GraphTest.class);
        suite.addTest(SqlToRelConverter.suite());
        suite.addTestSuite(OptionsListTest.class);
        suite.addTestSuite(SaffronSqlValidationTest.class);
        suite.addTestSuite(RexTransformerTest.class);
    }

    /**
     * Creates and runs the root test suite.
     */
    boolean run(String [] args) throws Exception
    {
        try {
            TestResult result = TestRunner.run(suite());
            if (result.errorCount() > 0 || result.failureCount() > 0) {
                return false;
            }

            return true;
        } catch (Throwable e) {
            System.out.print("Received exception: ");
            String [] msgs = convertStackToString(e);
            for (int i = 0; i < msgs.length; i++) {
                System.out.println(msgs[i]);
            }

            return false;
        }
    }

}


// End Main.java
