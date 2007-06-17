/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
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
package org.eigenbase.oj.stmt;

import java.lang.reflect.*;


/**
 * BoundMethod is a "thunk": a method which has already been bound to a
 * particular object on which it should be invoked, together with the arguments
 * which should be passed on invocation.
 */
public class BoundMethod
{
    //~ Instance fields --------------------------------------------------------

    Method method;
    Object o;
    String [] parameterNames;
    Object [] args;

    //~ Constructors -----------------------------------------------------------

    BoundMethod(
        Object o,
        Method method,
        String [] parameterNames)
    {
        this.o = o;
        this.method = method;
        this.parameterNames = parameterNames;
    }

    //~ Methods ----------------------------------------------------------------

    public Object call()
        throws IllegalAccessException, InvocationTargetException
    {
        return method.invoke(o, args);
    }
}

// End BoundMethod.java
