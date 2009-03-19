/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import org.jgrapht.graph.*;


/**
 * JmiInheritanceEdge represents an inheritance relationship in a JMI model. The
 * source vertex is the superclass and the target vertex is the subclass.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiInheritanceEdge
    extends DefaultEdge
{
    //~ Instance fields --------------------------------------------------------

    private final JmiClassVertex superClass;
    private final JmiClassVertex subClass;

    //~ Constructors -----------------------------------------------------------

    JmiInheritanceEdge(
        JmiClassVertex superClass,
        JmiClassVertex subClass)
    {
        this.superClass = superClass;
        this.subClass = subClass;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the vertex representing the superclass
     */
    public JmiClassVertex getSuperClass()
    {
        return superClass;
    }

    /**
     * @return the vertex representing the subclass
     */
    public JmiClassVertex getSubClass()
    {
        return subClass;
    }

    // implement Object
    public String toString()
    {
        return getSuperClass().toString() + "_generalizes_" + getSubClass();
    }
}

// End JmiInheritanceEdge.java
