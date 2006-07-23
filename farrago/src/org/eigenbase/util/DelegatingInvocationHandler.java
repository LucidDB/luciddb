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

import java.lang.reflect.*;


/**
 * A class derived from <code>DelegatingInvocationHandler</code> handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it forwards the call to a fallback object, which
 * must implement all of the interfaces which this proxy implements.
 *
 * <p>It is useful in creating a wrapper class around an interface which may
 * change over time.</p>
 *
 * <p>Example:
 *
 * <blockquote>
 * <pre>import java.sql.Connection;
 * Connection connection = ...;
 * Connection tracingConnection = (Connection) Proxy.newProxyInstance(
 *     null,
 *     new Class[] {Connection.class},
 *     new DelegatingInvocationHandler() {
 *         protected Object getTarget() {
 *             return connection;
 *         }
 *         Statement createStatement() {
 *             System.out.println("statement created");
 *             return connection.createStatement();
 *         }
 *     });</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since 21 March, 2002
 */
public abstract class DelegatingInvocationHandler
    implements InvocationHandler
{

    //~ Methods ----------------------------------------------------------------

    public Object invoke(
        Object proxy,
        Method method,
        Object [] args)
        throws Throwable
    {
        Class clazz = getClass();
        Method matchingMethod;
        try {
            matchingMethod =
                clazz.getMethod(
                    method.getName(),
                    method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            matchingMethod = null;
        } catch (SecurityException e) {
            matchingMethod = null;
        }
        try {
            if (matchingMethod != null) {
                // Invoke the method in the derived class.
                return matchingMethod.invoke(this, args);
            } else {
                // Invoke the method on the proxy.
                return method.invoke(
                        getTarget(),
                        args);
            }
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    /**
     * Returns the object to forward method calls to, should the derived class
     * not implement the method. Generally, this object will be a member of the
     * derived class, supplied as a parameter to its constructor.
     */
    protected abstract Object getTarget();
}

// End DelegatingInvocationHandler.java
