/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

package org.eigenbase.util;

import java.io.*;
import java.sql.*;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import openjava.mop.Toolbox;
import openjava.ptree.Expression;
import openjava.ptree.StatementList;

import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlMoniker;
import org.eigenbase.sql.util.SqlBasicVisitor;

/**
 * Miscellaneous utility functions.
 */
public class Util extends Toolbox
{
    //~ Static fields/initializers --------------------------------------------

    /** System-dependent newline character. */
    public static final String lineSeparator =
        System.getProperty("line.separator");

    /** System-dependent file separator, for example, "/" or "\." */
    public static final String fileSeparator =
        System.getProperty("file.separator");
    public static final Object [] emptyObjectArray = new Object[0];
    public static final String [] emptyStringArray = new String[0];
    public static final SqlMoniker [] emptySqlMonikerArray = new SqlMoniker[0];

    private static boolean driversLoaded = false;

    /**
     * Regular expression for a valid java identifier which contains no
     * underscores and can therefore be returned intact by {@link #toJavaId}.
      */
    private static final Pattern javaIdPattern =
        Pattern.compile("[a-zA-Z_$][a-zA-Z0-9$]*");

    //~ Inner Classes ---------------------------------------------------------
    /**
     * Exception used to interrupt a tree walk of any kind.
     */
    public static class FoundOne extends RuntimeException {
        private final Object node;


        public FoundOne(Object node) {
            this.node = node;
        }

        public Object getNode() {
            return node;
        }
    }

    /**
     * Describes a node, its parent and if and where in the parent a node lives.
     * If parent is null, the offset value is not valid.
     */
    public static class SqlNodeDescriptor {
        private final SqlNode node;
        private final SqlNode parent;
        private final Integer parentOffset;

        public SqlNodeDescriptor(SqlNode node, SqlNode parent, Integer parentOffset)
        {
            assert(null == parent ||
                parent instanceof SqlCall ||
                parent instanceof SqlNodeList);
            this.node = node;
            this.parent = parent;
            this.parentOffset = parentOffset;
        }

        public SqlNode getNode()
        {
            return node;
        }

        public SqlNode getParent()
        {
            return parent;
        }

        public Integer getParentOffset()
        {
            return parentOffset;
        }
    }

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
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn
     * that you are not using it.
     */
    public static final void discard(double d)
    {
        if (false) {
            discard(d);
        }
    }

    /**
     * Records that an exception has been caught but will not be re-thrown.
     * If the tracer is not null, logs the exception to the tracer.
     *
     * @param e Exception
     * @param logger If not null, logs exception to this logger
     */
    public static final void swallow(
        Throwable e,
        Logger logger)
    {
        if (logger != null) {
            logger.log(Level.FINER, "Discarding exception", e);
        }
    }

    /**
     * Returns whether two objects are equal or are both null.
     */
    public static final boolean equal(
        Object s0,
        Object s1)
    {
        if (s0 == null) {
            return s1 == null;
        } else if (s1 == null) {
            return false;
        } else {
            return s0.equals(s1);
        }
    }

    /**
     * Returns whether two arrays are equal or are both null.
     */
    public static final boolean equal(
        Object[] s0,
        Object[] s1)
    {
        if (s0 == null) {
            return s1 == null;
        } else if (s1 == null) {
            return false;
        } else if (s0.length != s1.length) {
            return false;
        } else {
            for (int i = 0; i < s0.length; i++) {
                Object o0 = s0[i];
                Object o1 = s1[i];
                if (!equal(o0, o1)) {
                    return false;
                }
            }
            return true;
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
    public static int hash(
        int i,
        int j)
    {
        return (i << 4) ^ j;
    }

    /**
     * Computes a hash code from an existing hash code and an object (which
     * may be null).
     */
    public static int hash(
        int h,
        Object o)
    {
        int k = (o == null) ? 0 : o.hashCode();
        return ((h << 4) | h) ^ k;
    }

    /**
     * Computes a hash code from an existing hash code and an array of objects
     * (which may be null).
     */
    public static int hashArray(
        int h,
        Object [] a)
    {
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
     * Returns a set of the elements which are in <code>set1</code> but not in
     * <code>set2</code>, without modifying either.
     */
    public static Set minus(
        Set set1,
        Set set2)
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
     * Prints an object using reflection. We can handle <code>null</code>;
     * arrays of objects and primitive values; for regular objects, we print
     * all public fields.
     */
    public static void print(
        PrintWriter pw,
        Object o)
    {
        print(pw, o, 0);
    }

    public static void print(
        PrintWriter pw,
        Object o,
        int indent)
    {
        if (o == null) {
            pw.print("null");
            return;
        }
        Class clazz = o.getClass();
        if (o instanceof String) {
            printJavaString(pw, (String) o, true);
        } else if ((clazz == Integer.class) || (clazz == Boolean.class)
                || (clazz == Character.class) || (clazz == Byte.class)
                || (clazz == Short.class) || (clazz == Long.class)
                || (clazz == Float.class) || (clazz == Double.class)
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
                print(
                    pw,
                    Array.get(o, i),
                    indent + 1);
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
                print(
                    pw,
                    iter.next(),
                    indent + 1);
            }
            pw.print("}");
        } else if (o instanceof Enumeration) {
            pw.print(clazz.getName());
            Enumeration e = (Enumeration) o;
            pw.print(" {");
            int i = 0;
            while (e.hasMoreElements()) {
                if (i++ > 0) {
                    pw.println(",");
                }
                print(
                    pw,
                    e.nextElement(),
                    indent + 1);
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
                print(pw, val, indent + 1);
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
            String s1 = replace(s, "\\", "\\\\");
            String s2 = replace(s1, "\"", "\\\"");
            String s3 = replace(s2, "\n\r", "\\n");
            String s4 = replace(s3, "\n", "\\n");
            String s5 = replace(s4, "\r", "\\r");
            pw.print("\"");
            pw.print(s5);
            pw.print("\"");
        }
    }

    public static void println(
        PrintWriter pw,
        Object o)
    {
        print(pw, o, 0);
        pw.println();
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
    public static String toScientificNotation(BigDecimal bd)
    {
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
        unscaled = unscaled.substring(
                0,
                Math.min(truncateAt, len));
        ret.append(unscaled.charAt(0));
        if (scale == 0) {
            // trim trailing zeroes since they aren't significant
            int i = unscaled.length();
            while (i > 1) {
                if (unscaled.charAt(i - 1) != '0') {
                    break;
                }
                --i;
            }
            unscaled = unscaled.substring(0, i);
        }
        if (unscaled.length() > 1) {
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
    public static final String replace(
        String s,
        String find,
        String replace)
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
            found = s.indexOf(find, start);
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
    public static URL toURL(File file)
        throws MalformedURLException
    {
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
        assert (value.charAt(0) == '"');
        assert (value.charAt(value.length() - 1) == '"');
        String s5 = value.substring(1, value.length() - 1);
        String s4 = Util.replace(s5, "\\r", "\r");
        String s3 = Util.replace(s4, "\\n", "\n");
        String s2 = Util.replace(s3, "\\\"", "\"");
        String s1 = Util.replace(s2, "\\\\", "\\");
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
     * @testcase {@link org.eigenbase.util.UtilTest#testToJavaId}
     */
    public static String toJavaId(
        String s,
        int ordinal)
    {
        // If it's already a valid Java id (and doesn't contain any
        // underscores), return it unchanged.
        if (javaIdPattern.matcher(s).matches()) {
            // prepend "ID$" to string so it doesn't clash with java keywords
            return "ID$" + ordinal + "$" + s;
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
            } else if ((c < 0x7F) /* Normal ascii character */
                    && !Character.isISOControl(c)
                    && ((i == 0) ? Character.isJavaIdentifierStart(c)
                    : Character.isJavaIdentifierPart(c))) {
                buf.append(c);
            } else {
                buf.append("_");
                buf.append(Integer.toString(c, 16));
                buf.append("_");
            }
        }
        return buf.toString();
    }

    /**
     * @deprecated Use {@link java.util.Arrays#asList(Object[])} instead
     *
     * Converts the elements of an array into a {@link java.util.List}
     */
    public static List toList(final Object [] array)
    {
        List ret = new ArrayList(array.length);
        for (int i = 0; i < array.length; i++) {
            ret.add(array[i]);
        }
        return ret;
    }

    /**
     * Materializes the results of a {@link java.util.Iterator} as a
     * {@link java.util.List}.
     *
     * @param iter iterator to materialize
     *
     * @return materialized list
     */
    public static List toList(Iterator iter)
    {
        List list = new ArrayList();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        return list;
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
    public static Object [] toArray(
        Vector v,
        Object [] a)
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
     * @return true if s==null or if s.length()==0
     */
    public static boolean isNullOrEmpty(String s) {
        return (null==s) || (s.length()==0);
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
        StringTokenizer tok = new StringTokenizer(jdbcDrivers, ",");
        while (tok.hasMoreTokens()) {
            String jdbcDriver = tok.nextToken();
            try {
                Class.forName(jdbcDriver);
            } catch (ClassNotFoundException e) {
                System.out.println("Warning: could not find driver "
                    + jdbcDriver);
            }
        }
        driversLoaded = true;
    }

    /**
     * Returns the {@link Charset} object representing
     * the value of {@link SaffronProperties#defaultCharset}
     *
     * @throws  java.nio.charset.IllegalCharsetNameException
     *          If the given charset name is illegal
     *
     * @throws  java.nio.charset.UnsupportedCharsetException
     *          If no support for the named charset is available
     *          in this instance of the Java virtual machine
     */
    public static Charset getDefaultCharset()
    {
        return Charset.forName(
            SaffronProperties.instance().defaultCharset.get());
    }

    public static Error newInternal()
    {
        return newInternal("(unknown cause)");
    }

    public static Error newInternal(String s)
    {
        return new AssertionError("Internal error: " + s);
    }

    public static Error newInternal(Throwable e)
    {
        return newInternal(e, "(unknown cause)");
    }

    public static Error newInternal(Throwable e, String s)
    {
        String message = "Internal error: " + s;
        if (false) {
            // TODO re-enable this code when we're no longer throwing spurious
            //   internal errors (which should be parse errors, for example)
            System.err.println(message);
            e.printStackTrace(System.err);
        }
        AssertionError ae = new AssertionError(message);
        ae.initCause(e);
        return ae;
    }

    /**
     * Checks a pre-condition.
     *
     * <p>For example,
     *
     * <pre>
     * /**
     *   * @ pre x != 0
     *   * /
     * void foo(int x) {
     *     Util.pre(x != 0, "x != 0");
     * }</pre>
     *
     * @param b Result of evaluating the pre-condition.
     * @param description Description of the pre-condition.
     */
    public static void pre(boolean b, String description)
    {
        if (!b) {
            throw newInternal("pre-condition failed: " + description);
        }
    }

    /**
     * Checks a post-condition.
     *
     * <p>For example,
     *
     * <pre>
     * /**
     *   * @ post return != 0
     *   * /
     * void foo(int x) {
     *     int res = bar(x);
     *     Util.post(res != 0, "return != 0");
     * }</pre>
     *
     * @param b Result of evaluating the pre-condition.
     * @param description Description of the pre-condition.
     */
    public static void post(boolean b, String description)
    {
        if (!b) {
            throw newInternal("post-condition failed: " + description);
        }
    }

    /**
     * Checks an invariant.
     *
     * <p>This is similar to <code>assert</code> keyword, except that the
     * condition is always evaluated even if asserts are disabled.
     */
    public static void permAssert(boolean b, String description)
    {
        if (!b) {
            throw newInternal("invariant violated: " + description);
        }
    }

    /**
     * Returns a {@link java.lang.RuntimeException} indicating that a
     * particular feature has not been implemented, but should be.
     *
     * <p>If every 'hole' in our functionality uses this method, it will be
     * easier for us to identity the holes. Throwing a
     * {@link java.lang.UnsupportedOperationException} isn't as good, because
     * sometimes we actually want to partially implement an API.
     *
     * <p>Example usage:
     * <blockquote><pre><code>class MyVisitor extends BaseVisitor {
     *     void accept(Foo foo) {
     *         // Exception will identify which subclass forgot to override
     *         // this method
     *         throw Util.needToImplement(this);
     *     }
     * }</pre></blockquote>
     *
     * @param o The object which was the target of the call, or null.
     *   Passing the object gives crucial information if a method needs to be
     *   overridden and a subclass forgot to do so.
     *
     * @return an {@link UnsupportedOperationException}.
     */
    public static RuntimeException needToImplement(Object o)
    {
        String description = null;
        if (o != null) {
            description = o.getClass().toString() + ": " + o.toString();
        }
        throw new UnsupportedOperationException(description);
    }

    /**
     * Flags a piece of code as needing to be cleaned up before you check in.
     *
     * <p>Introduce a call to this method to indicate that a piece of code,
     * or a javadoc comment, needs work before you check in. If you have an IDE
     * which can easily trace references, this is an easy way to maintain a
     * to-do list.
     *
     * <p><strong>Checked-in code must never call this method</strong>: you
     * must remove all calls/references to this method before you check in.
     *
     * <p>The <code>argument</code> has generic type and determines the
     * type of the result. This allows you to use the method inside an
     * expression, for example
     *
     * <blockquote><pre><code>int x = Util.deprecated(0, false);</code></pre></blockquote>
     *
     * but the usual usage is to pass in a descriptive string.
     *
     * <h3>Examples</h3>
     *
     * <h4>Example #1: Using <code>deprecated</code> to fail if a piece of
     * supposedly dead code is reached</h4>
     *
     * <blockquote><pre><code>void foo(int x) {
     *     if (x &lt; 0) {
     *         // If this code is executed, an error will be thrown.
     *         Util.deprecated("no longer need to handle negative numbers", true);
     *         bar(x);
     *     } else {
     *         baz(x);
     *     }
     * }</code></pre></blockquote>
     *
     * <h4>Example #2: Using <code>deprecated</code> to comment out dead
     * code</h4>
     *
     * <blockquote><pre>if (Util.deprecated(false, false)) {
     *     // This code will not be executed, but an error will not be thrown.
     *     baz();
     * }</pre></blockquote>
     *
     * @param argument Arbitrary argument to the method.
     * @param fail Whether to throw an exception if this method is called
     * @return The value of the <code>argument</code>.
     *
     * @deprecated If a piece of code calls this method, it indicates that the
     *    code needs to be cleaned up.
     */
    public static <T> T deprecated(T argument, boolean fail) {
        if (fail) {
            throw new UnsupportedOperationException();
        }
        return argument;
    }

    public static void loadLibrary(String libName)
    {
        if (!System.mapLibraryName(libName).startsWith("lib")) {
            // assume mingw
            System.loadLibrary("cyg" + libName + "-0");
        } else {
            System.loadLibrary(libName);
        }
    }

    public static void restartIterator(Iterator iterator)
    {
        if (iterator instanceof RestartableIterator) {
            ((RestartableIterator) iterator).restart();
        } else {
            throw new UnsupportedOperationException("restart");
        }
    }

    /**
     * Searches recursively for a {@link SqlIdentifier}
     * @param node in which to look in
     * @return null if no Identifier was found.
     */
    public static SqlNodeDescriptor findIdentifier(SqlNode node) {
        SqlBasicVisitor visitor = new SqlBasicVisitor<Void>() {
            public Void visit(SqlIdentifier id)
            {
                throw new FoundOne(id);
            }
        };

        try {
            node.accept(visitor);
        } catch (FoundOne e){
            return new SqlNodeDescriptor((SqlNode) e.getNode(),
                visitor.getCurrentParent(),
                visitor.getCurrentOffset());
        }
        return null;
    }

    /**
     * Generates a unique name
     *
     * @param names  Array of existing names
     * @param length Number of existing names
     * @param s Suggested name
     * @return Name which does not match any of the names in the first
     *   <code>length</code> positions of the <code>names</code> array.
     */
    public static String uniqueFieldName(
        String [] names,
        int length,
        String s)
    {
        if (!contains(names, length, s)) {
            return s;
        }
        int n = length;
        while (true) {
            s = "EXPR_" + n;
            if (!contains(names, length, s)) {
                return s;
            }

            // FIXME jvs 15-Nov-2003:  If we ever get here, it's an infinite
            // loop; should be ++n?
            assert(false);
        }
    }

    public static boolean contains(
        String [] names,
        int length,
        String s)
    {
        for (int i = 0; i < length; i++) {
            if (names[i].equals(s)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Closes an InputStream, ignoring any I/O exception.  This should only
     * be used in finally blocks when it's necessary to avoid throwing
     * an exception which might mask a real exception.
     *
     * @param stream stream to close
     */
    public static void squelchStream(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes an OutputStream, ignoring any I/O exception.  This should only
     * be used in finally blocks when it's necessary to avoid throwing
     * an exception which might mask a real exception.  If you want
     * to make sure that data has been successfully flushed, do NOT use
     * this anywhere else; use stream.close() instead.
     *
     * @param stream stream to close
     */
    public static void squelchStream(OutputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Reader, ignoring any I/O exception.  This should only
     * be used in finally blocks when it's necessary to avoid throwing
     * an exception which might mask a real exception.
     *
     * @param reader reader to close
     */
    public static void squelchReader(Reader reader)
    {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Writer, ignoring any I/O exception.  This should only
     * be used in finally blocks when it's necessary to avoid throwing
     * an exception which might mask a real exception.  If you want
     * to make sure that data has been successfully flushed, do NOT use
     * this anywhere else; use writer.close() instead.
     *
     * @param writer writer to close
     */
    public static void squelchWriter(Writer writer)
    {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Statement, ignoring any SQL exception.  This should only
     * be used in finally blocks when it's necessary to avoid throwing
     * an exception which might mask a real exception.
     *
     * @param stmt stmt to close
     */
    public static void squelchStmt(Statement stmt)
    {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Connection, ignoring any SQL exception.  This should only
     * be used in finally blocks when it's necessary to avoid throwing
     * an exception which might mask a real exception.
     *
     * @param connection connection to close
     */
    public static void squelchConnection(Connection connection)
    {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            // intentionally suppressed
        }
    }
}


// End Util.java
