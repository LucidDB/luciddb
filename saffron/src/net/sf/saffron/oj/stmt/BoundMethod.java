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

package net.sf.saffron.oj.stmt;

import java.lang.reflect.*;

/**
 * BoundMethod is a "thunk":  a method which has already been bound to a
 * particular object on which it should be invoked, together with the arguments
 * which should be passed on invocation.
 */
class BoundMethod
{
    Method method;
    Object o;
    String [] parameterNames;
    Object [] args;

    BoundMethod(Object o,Method method,String [] parameterNames)
    {
        this.o = o;
        this.method = method;
        this.parameterNames = parameterNames;
    }

    Object call()
        throws IllegalAccessException,InvocationTargetException
    {
        return method.invoke(o,args);
    }
}

// End BoundMethod.java
