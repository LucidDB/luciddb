/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

    // for debugging
    public String toString()
    {
        return "#" + index + ": " + name + " " + type;
    }
}

// End RelDataTypeFieldImpl.java
