/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package net.sf.saffron.util;

import junit.framework.ComparisonFailure;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import openjava.mop.Toolbox;
import openjava.ptree.Expression;
import openjava.ptree.StatementList;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Miscellaneous utility functions.
 */
public class Util extends Toolbox
{
    //~ Static fields/initializers --------------------------------------------

    /** System-dependent newline character. */
    public static String lineSeparator = System.getProperty("line.separator");

    /** System-dependent file separator, for example, "/" or "\." */
    public static String fileSeparator = System.getProperty("file.separator");
    public static PrintWriter debugWriter;
    public static final Object [] emptyObjectArray = new Object[0];
    public static final String [] emptyStringArray = new String[0];
    private static boolean driversLoaded = false;

    //~ Methods ---------------------------------------------------------------

    public static final void discard(Object o)
    {
        if (false) {
            discard(o);
        }
    }

    public static final void discard(int i)
    {
        if (false) {
            discard(i);
        }
    }

    /**
     * Returns whether two strings are equal or are both null.
     */
    public static final boolean equal(String s0,String s1)
    {
        if (s0 == null) {
            return s1 == null;
        } else if (s1 == null) {
            return false;
        } else {
            return s0.equals(s1);
        }
    }

    public static StatementList clone(StatementList e)
    {
        throw new UnsupportedOperationException();
    }

    //      static SortItem clone(SortItem sortItem)
    //      {
    //          try {
    //              return (SortItem) sortItem.clone();
    //          } catch (CloneNotSupportedException e) {
    //              Util.processInternal(e);
    //              return null;
    //          }
    //      }
    //      static SortItem[] clone(SortItem[] a)
    //      {
    //          SortItem[] a2 = new SortItem[a.length];
    //          for (int i = 0; i < a.length; i++)
    //              a2[i] = clone(a[i]);
    //          return a2;
    //      }
    public static String [] clone(String [] a)
    {
        String [] a2 = new String[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = a[i];
        }
        return a2;
    }

    //      static private Class primitive2wrapperClass(Class clazz)
    //      {
    //          return clazz == Boolean.TYPE ? java.lang.Boolean.class
    //              : clazz == Byte.TYPE ? java.lang.Byte.class
    //              : clazz == Character.TYPE ? java.lang.Character.class
    //              : clazz == Short.TYPE ? java.lang.Short.class
    //              : clazz == Integer.TYPE ? java.lang.Integer.class
    //              : clazz == Long.TYPE ? java.lang.Long.class
    //              : clazz == Float.TYPE ? java.lang.Float.class
    //              : clazz == Double.TYPE ? java.lang.Double.class
    //              : clazz == Void.TYPE ? java.lang.Void.class
    //              : null;
    //      }
    //      public static final Boolean getBoolean(boolean b)
    //      {
    //          return b ? Boolean.TRUE : Boolean.FALSE;
    //      }
    public static int [] clone(int [] a)
    {
        int [] b = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            b[i] = a[i];
        }
        return b;
    }

    /**
     * Returns whether two integer arrays are equal. Neither must be null.
     */
    public static boolean equals(int [] a,int [] b)
    {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static int hash(int i,int j)
    {
        return (i << 4) ^ j;
    }

    /**
     * Runs the test suite.
     */
    public static void main(String [] args) throws Exception
    {
        TestRunner.run(suite());
    }

    /**
     * Return a set of the elements which are in <code>set1</code> but not in
     * <code>set2</code>, without modifying either.
     */
    public static Set minus(Set set1,Set set2)
    {
        if (set1.isEmpty()) {
            return set1;
        } else if (set2.isEmpty()) {
            return set1;
        } else {
            Set set = new HashSet(set1);
            set.removeAll(set2);
            return set;
        }
    }

    //      public static synchronized PrintWriter getDebug()
    //      {
    //          if (debugWriter == null) {
    //              boolean autoFlush = true;
    //              debugWriter = new PrintWriter(System.out, autoFlush);
    //          }
    //          return debugWriter;
    //      }

    /**
     * Computes <code>nlogn(n)</code> (or <code>n</code> if <code>n</code> is
     * small, so the result is never negative.
     */
    public static double nLogN(double d)
    {
        return (d < Math.E) ? d : (d * Math.log(d));
    }

    /**
     * Print an object using reflection. We can handle <code>null</code>;
     * arrays of objects and primitive values; for regular objects, we print
     * all public fields.
     */
    public static void print(PrintWriter pw,Object o)
    {
        print(pw,o,0);
    }

    public static void print(PrintWriter pw,Object o,int indent)
    {
        if (o == null) {
            pw.print("null");
            return;
        }
        Class clazz = o.getClass();
        if (o instanceof String) {
            printJavaString(pw,(String) o,true);
        } else if (
            (clazz == Integer.class)
                || (clazz == Boolean.class)
                || (clazz == Character.class)
                || (clazz == Byte.class)
                || (clazz == Short.class)
                || (clazz == Long.class)
                || (clazz == Float.class)
                || (clazz == Double.class)
                || (clazz == Void.class)) {
            pw.print(o.toString());
        } else if (clazz.isArray()) {
            // o is an array, but we can't cast to Object[] because it may be
            // an array of primitives.
            Object [] a; // for debug
            if (o instanceof Object []) {
                a = (Object []) o;
                discard(a);
            }
            int n = Array.getLength(o);
            pw.print("{");
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    pw.println(",");
                } else {
                    pw.println();
                }
                for (int j = 0; j < indent; j++) {
                    pw.print("\t");
                }
                print(pw,Array.get(o,i),indent + 1);
            }
            pw.print("}");
        } else if (o instanceof Iterator) {
            pw.print(clazz.getName());
            Iterator iter = (Iterator) o;
            pw.print(" {");
            int i = 0;
            while (iter.hasNext()) {
                if (i++ > 0) {
                    pw.println(",");
                }
                print(pw,iter.next(),indent + 1);
            }
            pw.print("}");
        } else if (o instanceof Enumeration) {
            pw.print(clazz.getName());
            Enumeration enum = (Enumeration) o;
            pw.print(" {");
            int i = 0;
            while (enum.hasMoreElements()) {
                if (i++ > 0) {
                    pw.println(",");
                }
                print(pw,enum.nextElement(),indent + 1);
            }
            pw.print("}");
        } else {
            pw.print(clazz.getName());
            pw.print(" {");
            Field [] fields = clazz.getFields();
            int printed = 0;
            for (int i = 0; i < fields.length; i++) {
                if (isStatic(fields[i])) {
                    continue;
                }
                if (printed++ > 0) {
                    pw.println(",");
                } else {
                    pw.println();
                }
                for (int j = 0; j < indent; j++) {
                    pw.print("\t");
                }
                pw.print(fields[i].getName());
                pw.print("=");
                Object val = null;
                try {
                    val = fields[i].get(o);
                } catch (IllegalAccessException e) {
                    throw newInternal(e);
                }
                print(pw,val,indent + 1);
            }
            pw.print("}");
        }
    }

    /**
     * Prints a string, enclosing in double quotes (") and escaping if
     * necessary. For examples,
     * <code>printDoubleQuoted(w,"x\"y",false)</code> prints
     * <code>"x\"y"</code>.
     */
    public static final void printJavaString(
        PrintWriter pw,
        String s,
        boolean nullMeansNull)
    {
        if (s == null) {
            if (nullMeansNull) {
                pw.print("null");
            } else {
                //pw.print("");
            }
        } else {
            String s1 = replace(s,"\\","\\\\");
            String s2 = replace(s1,"\"","\\\"");
            String s3 = replace(s2,"\n\r","\\n");
            String s4 = replace(s3,"\n","\\n");
            String s5 = replace(s4,"\r","\\r");
            pw.print("\"");
            pw.print(s5);
            pw.print("\"");
        }
    }

    public static void println(PrintWriter pw,Object o)
    {
        print(pw,o,0);
        pw.println();
    }

    /**
     * Converts a byte array into a bit string or a hex string.
     *
     * <p>For example,
     * <code>toStringFromByteArray(new byte[] {0xAB, 0xCD}, 16)</code> returns
     * <code>ABCD</code>.
     */
    public static String toStringFromByteArray(byte[] value, int radix) {
        assert 2 == radix || 16 == radix :
                "Make sure that the algorithm below works for your radix";
        if (0 == value.length){
            return "";
        }

        int trick = radix*radix;
        StringBuffer ret = new StringBuffer();
        for (int i = 0; i < value.length; i++) {
            ret.append(Integer.toString(trick|(0x0ff & value[i]),radix).substring(1));
        }

        return ret.toString().toUpperCase();
    }

    /**
     * Replaces every occurrence of <code>find</code> in <code>s</code> with
     * <code>replace</code>.
     */
    public static final String replace(String s,String find,String replace)
    {
        // let's be optimistic
        int found = s.indexOf(find);
        if (found == -1) {
            return s;
        }
        StringBuffer sb = new StringBuffer(s.length());
        int start = 0;
        for (;;) {
            for (; start < found; start++) {
                sb.append(s.charAt(start));
            }
            if (found == s.length()) {
                break;
            }
            sb.append(replace);
            start += find.length();
            found = s.indexOf(find,start);
            if (found == -1) {
                found = s.length();
            }
        }
        return sb.toString();
    }

    //      static final boolean isBlank(String s)
    //      {
    //          return s == null || s.equals("");
    //      }

    /**
     * Creates a file-protocol URL for the given file.
     **/
    public static URL toURL(File file) throws MalformedURLException {
        String path = file.getAbsolutePath();
        // This is a bunch of weird code that is required to
        // make a valid URL on the Windows platform, due
        // to inconsistencies in what getAbsolutePath returns.
        String fs = System.getProperty("file.separator");
        if (fs.length() == 1) {
            char sep = fs.charAt(0);
            if (sep != '/') {
                path = path.replace(sep, '/');
            }
            if (path.charAt(0) != '/') {
                path = '/' + path;
            }
        }
        path = "file://" + path;
        return new URL(path);
    }

    public static Expression clone(Expression exp)
    {
        return (Expression) exp.makeRecursiveCopy();
    }

    public static Expression [] clone(Expression [] a)
    {
        Expression [] a2 = new Expression[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = clone(a[i]);
        }
        return a2;
    }

    /**
     * Converts double-quoted Java strings to their contents. For example,
     * <code>"foo\"bar"</code> becomes <code>foo"bar</code>.
     */
    public static String stripDoubleQuotes(String value)
    {
        assert(value.charAt(0) == '"');
        assert(value.charAt(value.length() - 1) == '"');
        String s5 = value.substring(1,value.length() - 1);
        String s4 = Util.replace(s5,"\\r","\r");
        String s3 = Util.replace(s4,"\\n","\n");
        String s2 = Util.replace(s3,"\\\"","\"");
        String s1 = Util.replace(s2,"\\\\","\\");
        return s1;
    }

    public static Test suite() throws Exception
    {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(UtilTestCase.class);
        suite.addTestSuite(BinaryHeap.BinaryHeapTestCase.class);
        return suite;
    }

    /**
     * @deprecated use {@link Vector#toArray} on Java2
     */
    public static Object [] toArray(Vector v)
    {
        Object [] objects = new Object[v.size()];
        v.copyInto(objects);
        return objects;
    }

    /**
     * Equivalent to {@link Vector#toArray(Object[])}.
     */
    public static Object [] toArray(Vector v,Object [] a)
    {
        int elementCount = v.size();
        if (a.length < elementCount) {
            a = (Object []) Array.newInstance(
                    a.getClass().getComponentType(),
                    elementCount);
        }
        v.copyInto(a);
        if (a.length > elementCount) {
            a[elementCount] = null;
        }
        return a;
    }

    static boolean isStatic(java.lang.reflect.Member member)
    {
        int modifiers = member.getModifiers();
        return java.lang.reflect.Modifier.isStatic(modifiers);
    }

    /**
     * Returns the connect string with which to connect to the 'Sales'
     * test database. In the process, it loads the necessary drivers.
     */
    public static String getSalesConnectString()
    {
        loadDrivers();
        return SaffronProperties.instance().testJdbcUrl.get();
    }

    private static synchronized void loadDrivers()
    {
        if (driversLoaded) {
            return;
        }
        String jdbcDrivers =
                SaffronProperties.instance().testJdbcDrivers.get();
        StringTokenizer tok = new StringTokenizer(jdbcDrivers,",");
        while (tok.hasMoreTokens()) {
            String jdbcDriver = tok.nextToken();
            try {
                Class.forName(jdbcDriver);
            } catch (ClassNotFoundException e) {
                System.out.println(
                    "Warning: could not find driver " + jdbcDriver);
            }
        }
        driversLoaded = true;
    }

    public static void assertEqualsVerbose(String expected, String actual) {
        if (expected == null && actual == null)
            return;
        if (expected != null && expected.equals(actual))
            return;
        String s = actual;
        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split" + NL +
        // "across lines
        //
        //
        s = replace(s, "\"", "\\\"");
        final String lineBreak = "\" + NL + " + lineSeparator + "\"";
        s = Pattern.compile("\r\n|\r|\n").matcher(s).replaceAll(lineBreak);
        s = "\"" + s + "\"";
        String message = "Expected:" + lineSeparator +
                expected + lineSeparator +
                "Actual: " + lineSeparator +
                actual + lineSeparator +
                "Actual java: " + lineSeparator +
                s + lineSeparator;
        throw new ComparisonFailure(message, expected, actual);
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class UtilTestCase extends TestCase
    {
        public UtilTestCase(String name)
        {
            super(name);
        }

        public void testPrintEquals()
        {
            assertPrintEquals("\"x\"","x",true);
        }

        public void testPrintEquals2()
        {
            assertPrintEquals("\"x\"","x",false);
        }

        public void testPrintEquals3()
        {
            assertPrintEquals("null",null,true);
        }

        public void testPrintEquals4()
        {
            assertPrintEquals("",null,false);
        }

        public void testPrintEquals5()
        {
            assertPrintEquals("\"\\\\\\\"\\r\\n\"","\\\"\r\n",true);
        }

        private void assertPrintEquals(
            String expect,
            String in,
            boolean nullMeansNull)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            printJavaString(pw,in,nullMeansNull);
            pw.flush();
            String out = sw.toString();
            assertEquals(expect,out);
        }
    }
}


// End Util.java
