/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.fennel.tuple;

import java.io.*;

import java.util.*;


/**
 * FennelTupleDescriptor provides the metadata describing a tuple. This is used
 * in conjunction with FennelTupleAccessor objects to marshall and unmarshall
 * data into FennelTupleData objects from external formats. This class is JDK
 * 1.4 compatible.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public class FennelTupleDescriptor
    implements Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -7075506007273800588L;

    //~ Instance fields --------------------------------------------------------

    /**
     * a collection of the FennelTupleAttributeDescriptor objects we're keeping.
     */
    private final List attrs = new ArrayList();

    //~ Constructors -----------------------------------------------------------

    /**
     * default constructor
     */
    public FennelTupleDescriptor()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the number of attributes we are holding.
     */
    public int getAttrCount()
    {
        return attrs.size();
    }

    /**
     * Gets an FennelTupleAttributeDescriptor given an ordinal index.
     */
    public FennelTupleAttributeDescriptor getAttr(int i)
    {
        return (FennelTupleAttributeDescriptor) attrs.get(i);
    }

    /**
     * Adds a new FennelTupleAttributeDescriptor.
     *
     * @return the index where it was added
     */
    public int add(FennelTupleAttributeDescriptor newDesc)
    {
        int ndx = attrs.size();
        attrs.add(newDesc);
        return ndx;
    }

    /**
     * Indicates if any descriptors we're keeping might contain nulls.
     */
    public boolean containsNullable()
    {
        int i;
        for (i = 0; i < attrs.size(); ++i) {
            if (getAttr(i).isNullable) {
                return true;
            }
        }
        return false;
    }
}

// End FennelTupleDescriptor.java
