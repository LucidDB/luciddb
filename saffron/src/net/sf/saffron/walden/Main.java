/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.walden;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import net.sf.saffron.oj.stmt.OJStatement;
import net.sf.saffron.runtime.VarDecl;
import net.sf.saffron.util.SaffronProperties;
import net.sf.saffron.util.Util;

import openjava.mop.Environment;
import openjava.mop.OJClass;

import openjava.ptree.ClassDeclaration;
import openjava.ptree.Expression;
import openjava.ptree.ParseTree;

import openjava.tools.parser.ParseException;
import openjava.tools.parser.Parser;

import java.io.*;

import java.util.ArrayList;
import java.util.Properties;


/**
 * <code>Walden</code> is a command-line interpreter for Saffron, and dynamic
 * Java statements in general; <code>Main</code> is its main entry point.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 10, 2002
 */
public class Main
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a set of queries. Used by <code>walden/index.jsp</code>.
     */
    public static String [] getQueries()
    {
        final String nl = Util.lineSeparator;
        return new String [] {
            "1",
            
            "\"foo\" + \"bar\"",
            
            "sales.SalesInMemory sales = new sales.SalesInMemory();",
            
            "select from sales.emps as emp" + nl + "join sales.depts as dept"
            + nl + "on emp.deptno == dept.deptno",
            
            "select emp.empno from sales.emps as emp" + nl + "union" + nl
            + "select dept.deptno from sales.depts as dept",
        };
    }

    public static void main(String [] args)
    {
        // Set properties specified using '-Dproperty=value' on the command
        // line. This allows us to invoke walden from ant without forking.
        Properties properties = SaffronProperties.instance();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-D")) { // e.g. "-Dfoo.bar=value"
                arg = arg.substring(2); // e.g. "foo.bar=value"
                int equals = arg.indexOf('=');
                String property;
                String value = "";
                if (equals < 0) {
                    property = arg;
                } else {
                    property = arg.substring(0,equals); // e.g. "foo.bar"
                    value = arg.substring(equals + 1); // e.g. "value"
                }
                properties.setProperty(property,value);
            }
        }
        Interpreter interpreter = new Interpreter();
        InputStreamReader reader = new InputStreamReader(System.in);
        PrintWriter pw = new PrintWriter(System.out,true);
        Handler handler = new PrintHandler(interpreter,pw,true);
        interpreter.run(reader,handler);
    }

    /**
     * Returns a JUnit test to test the Schedule class. This method is
     * automatically recognized by JUnit test harnesses.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(WaldonTestCase.class);
        return suite;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * JUnit test suite for {@link Main}.
     */
    public static class WaldonTestCase extends TestCase
    {
        public WaldonTestCase(String name)
        {
            super(name);
        }

        public void _testOne()
        {
            assertInterpret(new String [] { "1" },new String [] { "1" });
        }

        public void _testTwo()
        {
            assertInterpret(
                new String [] { "\"x\"","null" },
                new String [] { "x","null" });
        }

        public void testDeclaration()
        {
            assertInterpret(
                new String [] { "int i = 2","i + 3" },
                new String [] { "i: int: 2","int: 5" });
        }

        public void testSelect()
        {
            assertInterpret(
                new String [] { "select from new int[] {1,2,3}" },
                new String [] { "class int[]: {","1,","2,","3}" });
        }

        public void testVoid()
        {
            assertInterpret(
                new String [] { "System.out.println(\"foo\")" },
                new String [] { "void" });
        }

        private void assertInterpret(String input,String output)
        {
            StringWriter sw = new StringWriter();
            Interpreter interpreter = new Interpreter();
            InputStreamReader reader = new InputStreamReader(System.in);
            Handler handler =
                new PrintHandler(interpreter,new PrintWriter(sw),false);
            interpreter.run(reader,handler);
            assertEquals(output,sw.toString());
        }

        private void assertInterpret(
            String [] inputLines,
            String [] outputLines)
        {
            String input = linesToString(inputLines,";" + Util.lineSeparator);
            String output = linesToString(outputLines,Util.lineSeparator);
            assertInterpret(input,output);
        }

        private String linesToString(String [] lines,String sep)
        {
            StringWriter sw = new StringWriter();
            for (int i = 0; i < lines.length; i++) {
                sw.write(lines[i]);
                sw.write(sep);
            }
            return sw.toString();
        }
    }
}


// End Main.java
