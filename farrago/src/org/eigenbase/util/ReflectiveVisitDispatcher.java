/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 Disruptive Tech
// Copyright (C) 2008-2008 LucidEra, Inc.
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

import java.util.*;


/**
 * Interface for looking up methods relating to reflective visitation. One
 * possible implementation would cache the results.
 *
 * <p>Type parameter 'R' is the base class of visitoR class; type parameter 'E'
 * is the base class of visiteE class.
 *
 * <p>TODO: obsolete {@link ReflectUtil#lookupVisitMethod}, and use caching in
 * implementing that method.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public interface ReflectiveVisitDispatcher<R extends ReflectiveVisitor, E>
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up a visit method taking additional parameters beyond the
     * overloaded visitee type.
     *
     * @param visitorClass class of object whose visit method is to be invoked
     * @param visiteeClass class of object to be passed as a parameter to the
     * visit method
     * @param visitMethodName name of visit method
     * @param additionalParameterTypes list of additional parameter types
     *
     * @return method found, or null if none found
     */
    Method lookupVisitMethod(
        Class<? extends R> visitorClass,
        Class<? extends E> visiteeClass,
        String visitMethodName,
        List<Class> additionalParameterTypes);

    /**
     * Looks up a visit method.
     *
     * @param visitorClass class of object whose visit method is to be invoked
     * @param visiteeClass class of object to be passed as a parameter to the
     * visit method
     * @param visitMethodName name of visit method
     *
     * @return method found, or null if none found
     */
    Method lookupVisitMethod(
        Class<? extends R> visitorClass,
        Class<? extends E> visiteeClass,
        String visitMethodName);

    /**
     * Implements the {@link Glossary#VisitorPattern} via reflection. The basic
     * technique is taken from <a
     * href="http://www.javaworld.com/javaworld/javatips/jw-javatip98.html">a
     * Javaworld article</a>. For an example of how to use it, see {@link
     * ReflectVisitorTest}. Visit method lookup follows the same rules as if
     * compile-time resolution for VisitorClass.visit(VisiteeClass) were
     * performed. An ambiguous match due to multiple interface inheritance
     * results in an IllegalArgumentException. A non-match is indicated by
     * returning false.
     *
     * @param visitor object whose visit method is to be invoked
     * @param visitee object to be passed as a parameter to the visit method
     * @param visitMethodName name of visit method, e.g. "visit"
     *
     * @return true if a matching visit method was found and invoked
     */
    boolean invokeVisitor(
        R visitor,
        E visitee,
        String visitMethodName);
}

// End ReflectiveVisitDispatcher.java
