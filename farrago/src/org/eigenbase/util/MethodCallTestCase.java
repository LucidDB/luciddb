/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.lang.reflect.*;

import junit.framework.*;


/**
 * A <code>MethodCallTestCase</code> is a {@link TestCase} which invokes a
 * method on an object. You can use this class to expose methods of a
 * non-TestCase class as unit tests; {@link #addTestMethods} does this for all
 * <code>public</code>, non-<code>static</code>, <code>void</code> methods whose
 * names start with "test", and have one .
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 19, 2003
 */
public class MethodCallTestCase
    extends TestCase
{
    //~ Instance fields --------------------------------------------------------

    private final Dispatcher dispatcher;
    private final Method method;
    private final Object o;
    private final Object [] args;

    //~ Constructors -----------------------------------------------------------

    MethodCallTestCase(
        String name,
        Object o,
        Method method,
        Dispatcher dispatcher)
    {
        super(name);
        this.o = o;
        this.args = new Object[] { this };
        this.method = method;
        this.dispatcher = dispatcher;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns whether a method can be called as a test case; it must:
     *
     * <ol>
     * <li>be <code>public</code></li>
     * <li>be non-<code>static</code></li>
     * <li>return <code>void</code></li>
     * <li>begin with <code>test</code></li>
     * <li>have precisely one parameter of type {@link TestCase} (or a class
     * derived from it)</li>
     * </ol>
     */
    public static boolean isSuitable(Method method)
    {
        final int modifiers = method.getModifiers();
        if (!Modifier.isPublic(modifiers)) {
            //return false;
        }
        if (Modifier.isStatic(modifiers)) {
            return false;
        }
        if (method.getReturnType() != Void.TYPE) {
            return false;
        }
        if (!method.getName().startsWith("test")) {
            return false;
        }
        final Class [] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1) {
            return false;
        }
        if (!TestCase.class.isAssignableFrom(parameterTypes[0])) {
            return false;
        }
        return true;
    }

    public static void addTestMethods(
        TestSuite suite,
        Object o,
        Dispatcher dispatcher)
    {
        Class clazz = o.getClass();
        Method [] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            if (isSuitable(method)) {
                suite.addTest(
                    new MethodCallTestCase(
                        method.getName(),
                        o,
                        method,
                        dispatcher));
            }
        }
    }

    protected void runTest()
        throws Throwable
    {
        Util.discard(dispatcher.call(method, o, args));
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * A class implementing <code>Dispatcher</code> calls a method from within
     * its own security context. It exists to allow a {@link MethodCallTestCase}
     * to call non-public methods.
     */
    public interface Dispatcher
    {
        Object call(
            Method method,
            Object o,
            Object [] args)
            throws IllegalAccessException,
                IllegalArgumentException,
                InvocationTargetException;
    }
}

// End MethodCallTestCase.java
