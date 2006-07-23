/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.jmi;

import javax.jmi.model.*;
import javax.jmi.reflect.*;


/**
 * JmiClassVertex represents a class in a JMI model.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiClassVertex
{

    //~ Instance fields --------------------------------------------------------

    private final MofClass mofClass;
    Class<? extends RefObject> javaInterface;
    RefClass refClass;

    //~ Constructors -----------------------------------------------------------

    JmiClassVertex(MofClass mofClass)
    {
        this.mofClass = mofClass;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the MofClass represented by this vertex
     */
    public MofClass getMofClass()
    {
        return mofClass;
    }

    /**
     * @return the RefClass represented by this vertex
     */
    public RefClass getRefClass()
    {
        return refClass;
    }

    /**
     * @return the Java interface represented by this vertex
     */
    public Class<? extends RefObject> getJavaInterface()
    {
        return javaInterface;
    }

    // implement Object
    public String toString()
    {
        return mofClass.getName();
    }

    // implement Object
    public int hashCode()
    {
        return mofClass.hashCode();
    }

    // implement Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof JmiClassVertex)) {
            return false;
        }
        return mofClass.equals(((JmiClassVertex) obj).mofClass);
    }
}

// End JmiClassVertex.java
