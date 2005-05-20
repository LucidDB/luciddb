/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;


/**
 * A class derived from <code>BarfingInvocationHandler</code> handles a method
 * call by looking for a method in itself with identical parameters. If no
 * such method is found, it throws {@link UnsupportedOperationException}.
 *
 * <p>
 * It is useful when you are prototyping code. You can rapidly create a
 * prototype class which implements the important methods in an interface,
 * then implement other methods as they are called.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @see DelegatingInvocationHandler
 * @since Dec 23, 2002
 */
public class BarfingInvocationHandler implements InvocationHandler
{
    //~ Constructors ----------------------------------------------------------

    protected BarfingInvocationHandler()
    {
    }

    //~ Methods ---------------------------------------------------------------

    public Object invoke(
        Object proxy,
        Method method,
        Object [] args)
        throws Throwable
    {
        Class clazz = getClass();
        Method matchingMethod = null;
        try {
            matchingMethod =
                clazz.getMethod(
                    method.getName(),
                    method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            throw noMethod(method);
        } catch (SecurityException e) {
            throw noMethod(method);
        }
        if (matchingMethod.getReturnType() != method.getReturnType()) {
            throw noMethod(method);
        }

        // Invoke the method in the derived class.
        try {
            return matchingMethod.invoke(this, args);
        } catch (UndeclaredThrowableException e) {
            throw e.getCause();
        }
    }

    /**
     * Called when this class (or its derived class) does not have the
     * required method from the interface.
     */
    protected UnsupportedOperationException noMethod(Method method)
    {
        StringBuffer buf = new StringBuffer();
        final Class [] parameterTypes = method.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(parameterTypes[i].getName());
        }
        String signature =
            method.getReturnType().getName() + " "
            + method.getDeclaringClass().getName() + "." + method.getName()
            + "(" + buf.toString() + ")";
        return new UnsupportedOperationException(signature);
    }
}


// End BarfingInvocationHandler.java
