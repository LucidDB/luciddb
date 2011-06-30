/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
package org.eigenbase.reltype;

import java.io.*;


/**
 * Default implementation of {@link RelDataTypeField}.
 *
 * @author jhyde
 * @version $Id$
 */
public class RelDataTypeFieldImpl
    implements RelDataTypeField,
        Serializable
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;
    private final String name;
    private final int index;

    //~ Constructors -----------------------------------------------------------

    /**
     * @pre name != null
     * @pre type != null
     */
    public RelDataTypeFieldImpl(
        String name,
        int index,
        RelDataType type)
    {
        assert (name != null);
        assert (type != null);
        this.name = name;
        this.index = index;
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int hashCode()
    {
        return index
            ^ name.hashCode()
            ^ type.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RelDataTypeFieldImpl)) {
            return false;
        }
        RelDataTypeFieldImpl that = (RelDataTypeFieldImpl) obj;
        return this.index == that.index
            && this.name.equals(that.name)
            && this.type.equals(that.type);
    }

    // implement RelDataTypeField
    public String getName()
    {
        return name;
    }

    // implement RelDataTypeField
    public int getIndex()
    {
        return index;
    }

    // implement RelDataTypeField
    public RelDataType getType()
    {
        return type;
    }

    // implement Map.Entry
    public final String getKey()
    {
        return getName();
    }

    // implement Map.Entry
    public final RelDataType getValue()
    {
        return getType();
    }

    // implement Map.Entry
    public RelDataType setValue(RelDataType value)
    {
        throw new UnsupportedOperationException();
    }

    // for debugging
    public String toString()
    {
        return "#" + index + ": " + name + " " + type;
    }
}

// End RelDataTypeFieldImpl.java
