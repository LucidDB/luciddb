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
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import net.sf.saffron.sql.parser.ParserPosition;
import net.sf.saffron.runtime.ThreadIterator;
import net.sf.saffron.runtime.TimeoutQueueIterator;
import net.sf.saffron.runtime.TimeoutIteratorTest;
import openjava.mop.Toolbox;
import openjava.ptree.Expression;
import openjava.ptree.StatementList;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.math.BigDecimal;

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
    /**
     * Regular expression for a valid java identifier which contains no
     * underscores and can therefore be returned intact by {@link #toJavaId}.
      */
    private static final Pattern javaIdPattern =
            Pattern.compile("[a-zA-Z_$][a-zA-Z0-9$]*");

    //~ Methods ---------------------------------------------------------------

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn
     * that you are not using it.
     */
    public static final void discard(Object o)
    {
        if (false) {
            discard(o);
        }
    }

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn
     * that you are not using it.
     */
    public static final void discard(int i)
    {
        if (false) {
            discard(i);
        }
    }

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn
     * that you are not using it.
     */
    public static final void discard(boolean b)
    {
        if (false) {
            discard(b);
        }
    }

    /**
     * Records that an exception has been caught but will not be re-thrown.
     * If the tracer is not null, logs the exception to the tracer.
     *
     * @param e Exception
     * @param logger If not null, logs exception to this logger
     */
    public static final void swallow(Throwable e, Logger logger) {
        if (logger != null) {
            logger.log(Level.FINER, "Discarding exception", e);
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
        return (StatementList) e.makeCopy();
    }

    public static String [] clone(String [] a)
    {
        String [] a2 = new String[a.length];
        for (int i = 0; i < a.length; i++) {
            a2[i] = a[i];
        }
        return a2;
    }

    public static int [] clone(int [] a)
    {
        int [] b = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            b[i] = a[i];
        }
        return b;
    }

    /**
     * Combines two integers into a hash code.
     */
    public static int hash(int i,int j)
    {
        return (i << 4) ^ j;
    }

    /**
     * Computes a hash code from an existing hash code and an object (which
     * may be null).
     */
    public static int hash(int h, Object o) {
        int k = o == null ? 0 :
                o.hashCode();
        return ((h << 4) | h) ^ k;
    }

    /**
     * Computes a hash code from an existing hash code and an array of objects
     * (which may be null).
     */
    public static int hashArray(int h, Object[] a) {
        // The hashcode for a null array and an empty array should be different
        // than h, so use magic numbers.
        if (a == null) {
            return hash(h, 19690429);
        }
        if (a.length == 0) {
            return hash(h, 19690721);
        }
        for (int i = 0; i < a.length; i++) {
            h = hash(h, a[i]);
        }
        return h;
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

    /**
     * Computes <code>nlogn(n)</code> using the natural logarithm
     * (or <code>n</code> if <code>n<{@link Math#E}</code>,
     * so the result is never negative.
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
     * Formats a {@link BigDecimal} value to a string in scientific notation
     * For example<br>
     * <ul>
     * <li>A value of 0.00001234 would be formated as <code>1.234E-5</code></li>
     * <li>A value of 100000.00 would be formated as <code>1.00E5</code></li>
     * <li>A value of 100 (scale zero) would be formated as <code>1E2</code></li>
     * <br>
     * If {@param bd} has a precision higher than 20, this method will truncate
     * the output string to have a precision of 20
     * (no rounding will be done, just a truncate).
     */
    public static String toScientificNotation(BigDecimal bd) {
        final int truncateAt = 20;
        String unscaled = bd.unscaledValue().toString();
        if (bd.signum() < 0) {
            unscaled = unscaled.substring(1);
        }
        int len = unscaled.length();
        int scale = bd.scale();
        int e = len - scale - 1;

        StringBuffer ret = new StringBuffer();
        if (bd.signum() < 0) {
            ret.append('-');
        }
        //do truncation
        unscaled = unscaled.substring(0, Math.min(truncateAt, len));
        ret.append(unscaled.charAt(0));
        if (unscaled.length()>1) {
            ret.append(".");
            ret.append(unscaled.substring(1));
        }

        ret.append("E");
        ret.append(e);
        return ret.toString();
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

    /**
     * Converts an arbitrary string into a string suitable for use as a Java
     * identifier.
     *
     * <p>The mapping is one-to-one (that is, distinct strings will produce
     * distinct java identifiers). The mapping is also reversible, but the
     * inverse mapping is not implemented.</p>
     *
     * <p>A valid Java identifier must start with a Unicode letter, underscore,
     * or dollar sign ($). The other characters, if any, can be a Unicode
     * letter, underscore, dollar sign, or digit.</p>
     *
     * <p>This method uses an algorithm similar to URL encoding. Valid
     * characters are unchanged; invalid characters are converted to an
     * underscore followed by the hex code of the character; and underscores
     * are doubled.</p>
     *
     * Examples:<ul>
     * <li><code>toJavaId("foo")</code> returns <code>"foo"</code>
     * <li><code>toJavaId("foo bar")</code> returns <code>"foo_20_bar"</code>
     * <li><code>toJavaId("foo_bar")</code> returns <code>"foo__bar"</code>
     * <li><code>toJavaId("0bar")</code> returns <code>"_40_bar"</code>
     *     (digits are illegal as a prefix)
     * <li><code>toJavaId("foo0bar")</code> returns <code>"foo0bar"</code>
     * </ul>
     *
     * @testcase {@link net.sf.saffron.util.UtilTest#testToJavaId}
     */
    public static String toJavaId(String s, int ordinal) {
        // If it's already a valid Java id (and doesn't contain any
        // underscores), return it unchanged.
        if (javaIdPattern.matcher(s).matches()) {
            // prepend "ID$" to string so it doesn't clash with java keywords
            return "ID$"+ ordinal +"$" + s;
        }
        // Escape underscores and other undesirables.
        StringBuffer buf = new StringBuffer(s.length() + 10);
        buf.append("ID$");
        buf.append(ordinal);
        buf.append("$");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '_') {
                buf.append("__");
            } else if (c < 0x7F /* Normal ascii character */
                && !Character.isISOControl(c)
                && (i == 0 ?
                    Character.isJavaIdentifierStart(c) :
                    Character.isJavaIdentifierPart(c))) {
                buf.append(c);
            } else {
                buf.append("_");
                buf.append(Integer.toString(c, 16));
                buf.append("_");
            }
        }
        return buf.toString();
    }

    public static Test suite() throws Exception
    {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(UtilTest.class);
        suite.addTestSuite(BinaryHeap.BinaryHeapTestCase.class);
        suite.addTestSuite(ThreadIterator.Test.class);
        suite.addTestSuite(TimeoutIteratorTest.class);
        return suite;
    }


    /**
     * Converts the elements of an array into a {@link java.util.List}
     */
    public static List toList(final Object[] array) {
        List ret = new ArrayList(array.length);
        for (int i = 0; i < array.length; i++) {
            ret.add(array[i]);
        }
        return ret;
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

    /**
     * Returns the {@link java.nio.charset.Charset} object representing
     * the value of {@link SaffronProperties#defaultCharset}
     * @throws java.nio.charset.IllegalCharsetNameException - If the given charset name is illegal
     * @throws java.nio.charset.UnsupportedCharsetException - If no support for the named charset is available in this instance of the Java virtual machine
     */
    public static Charset getDefaultCharset() {
        return Charset.forName(SaffronProperties.instance().defaultCharset.get());
    }

    /**
     * Simply returns a string saying "encounted at line ?, column ?" using
     * {@link ParserPosition#getBeginLine} and {@link ParserPosition#getBeginColumn}
     * respectively
     */
    public static String encountedAt(ParserPosition pos) {
        StringBuffer ret = new StringBuffer();
        ret.append("encountered at line ");
        ret.append(pos.getBeginLine());
        ret.append(", column");
        ret.append(pos.getBeginColumn());
        return ret.toString();
    }

    /**
     * Returns whether two objects are equal. Either may be null.
     */
    public static boolean equals(Object o0, Object o1) {
        if (o0 == o1) {
            return true;
        }
        if (o0 == null || o1 == null) {
            return false;
        }
        return o0.equals(o1);
    }
}


// End Util.java
