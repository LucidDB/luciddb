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

package net.sf.saffron.test;

import junit.framework.TestCase;

import net.sf.saffron.oj.stmt.OJStatement;
import net.sf.saffron.runtime.SyntheticObject;
import net.sf.saffron.core.SaffronConnection;

import java.lang.reflect.Field;

import java.util.*;


public abstract class SaffronTestCase extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    static final OJStatement.Argument [] emptyArguments =
        new OJStatement.Argument[0];
    protected static final String nl = System.getProperty("line.separator");
    protected static final String [] emptyStringArray = new String[0];

    //~ Instance fields -------------------------------------------------------

    protected OJStatement.Argument [] arguments;

    //~ Constructors ----------------------------------------------------------

    public SaffronTestCase(String s) throws Exception
    {
        super(s);
        arguments = emptyArguments;
    }

    //~ Methods ---------------------------------------------------------------

    protected static void assertEqualsDeep(Object o,Object o2)
    {
        if (o instanceof Object [] && o2 instanceof Object []) {
            Object [] a = (Object []) o;
            Object [] a2 = (Object []) o2;
            assertEquals(a.length,a2.length);
            for (int i = 0; i < a.length; i++) {
                assertEqualsDeep(a[i],a2[i]);
            }
            return;
        }
        if (
            (o != null)
                && (o2 != null)
                && o.getClass().isArray()
                && (o.getClass() == o2.getClass())) {
            boolean eq;
            if (o instanceof boolean []) {
                eq = Arrays.equals((boolean []) o,(boolean []) o2);
            } else if (o instanceof byte []) {
                eq = Arrays.equals((byte []) o,(byte []) o2);
            } else if (o instanceof char []) {
                eq = Arrays.equals((char []) o,(char []) o2);
            } else if (o instanceof short []) {
                eq = Arrays.equals((short []) o,(short []) o2);
            } else if (o instanceof int []) {
                eq = Arrays.equals((int []) o,(int []) o2);
            } else if (o instanceof long []) {
                eq = Arrays.equals((long []) o,(long []) o2);
            } else if (o instanceof float []) {
                eq = Arrays.equals((float []) o,(float []) o2);
            } else if (o instanceof double []) {
                eq = Arrays.equals((double []) o,(double []) o2);
            } else {
                eq = false;
            }
            if (!eq) {
                fail("arrays not equal");
            }
        } else {
            // will handle the case 'o instanceof int[]' ok, because
            // shallow comparison is ok for ints
            assertEquals(o,o2);
        }
    }

    /**
     * Checks that <code>o</code> is a result set with a given number of rows,
     * a list of columns with given names, and every row is the same type.
     */
    protected void assertSynthetic(Object o,int rows,String [] columnNames)
    {
        assertSynthetic(o,rows,columnNames,null);
    }

    /**
     * Checks that <code>o</code> is a result set with a given number of rows,
     * a list of columns with given names and types, and every row is the
     * same type.
     */
    protected void assertSynthetic(
        Object o,
        int rows,
        String [] columnNames,
        Class [] columnClasses)
    {
        assertTrue(o instanceof SyntheticObject []);
        SyntheticObject [] a = (SyntheticObject []) o;
        assertEquals("row count",rows,a.length);
        Class rowClazz0 = null;
        for (int i = 0; i < a.length; i++) {
            SyntheticObject row = a[i];
            Class rowClazz = row.getClass();
            if (i == 0) {
                rowClazz0 = rowClazz;
                Field [] fields = row.getFields();
                assertEquals(
                    "number of fields",
                    columnNames.length,
                    fields.length);
                for (int j = 0; j < fields.length; j++) {
                    Field field = fields[j];
                    assertEquals(
                        "field name",
                        columnNames[j],
                        fields[j].getName());
                    if (columnClasses != null) {
                        Class valueClazz = field.getType();
                        assertEquals(
                            "column class",
                            columnClasses[j],
                            valueClazz);
                    }
                }
            } else {
                assertEquals("row class same as first",rowClazz0,rowClazz);
            }
        }
    }

    /**
     * Fails if <code>throwable</code> is null, or if its message does not
     * contain the string <code>pattern</code>.
     */
    protected void assertThrowableContains(Throwable throwable,String pattern)
    {
        if (throwable == null) {
            fail(
                "expected exception containing pattern <" + pattern
                + "> but got none");
        }
        String message = throwable.getMessage();
        if ((message == null) || (message.indexOf(pattern) < 0)) {
            fail(
                "expected pattern <" + pattern + "> in exception <"
                + throwable + ">");
        }
    }

    /**
     * Returns an iterator over the elements of an array.
     */
    public static Iterator makeIterator(Object [] a)
    {
        return Arrays.asList(a).iterator();
    }

    public SaffronConnection getConnection() {
        throw new UnsupportedOperationException();
    }

    /**
     * Runs a query and returns the result.
     */
    protected Object runQuery(String query,OJStatement.Argument [] arguments)
    {
        OJStatement statement = new OJStatement(getConnection());
        return statement.execute(query,arguments);
    }

    /**
     * Runs a query, using the default arguments, and returns the result.
     */
    protected Object runQuery(String query)
    {
        OJStatement statement = new OJStatement(getConnection());
        return statement.execute(query,this.arguments);
    }

    /**
     * Runs a query, and returns any exception.
     */
    protected Throwable runQueryCatch(String query)
    {
        try {
            Object o = runQuery(query);
            return null;
        } catch (Throwable throwable) {
            return throwable;
        }
    }

    /**
     * Converts an iterator to a list.
     */
    protected static List toList(Iterator iterator)
    {
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    /**
     * Converts an enumeration to a list.
     */
    protected static List toList(Enumeration enumeration)
    {
        ArrayList list = new ArrayList();
        while (enumeration.hasMoreElements()) {
            list.add(enumeration.nextElement());
        }
        return list;
    }

    protected void setUp() throws Exception
    {
        super.setUp();
    }

    /**
     * Checks that an iterator returns the same objects as the contents of an
     * array.
     */
    protected void assertEquals(Iterator iterator,Object [] a)
    {
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        assertEquals(list,a);
    }

    /**
     * Checks that a list has the same contents as an array.
     */
    protected void assertEquals(List list,Object [] a)
    {
        Object [] b = list.toArray();
        assertEquals(a,b);
    }

    /**
     * Checks that two arrays are equal.
     */
    protected void assertEquals(Object [] expected,Object [] actual)
    {
        assertTrue(expected.length == actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i],actual[i]);
        }
    }

    protected void assertEquals(Object [] expected,Object actual)
    {
        if (actual instanceof Object []) {
            assertEquals(expected,(Object []) actual);
        } else {
            // They're different. Let assertEquals(Object,Object) give the error.
            assertEquals((Object) expected,actual);
        }
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
    }
}


// End SaffronTestCase.java
