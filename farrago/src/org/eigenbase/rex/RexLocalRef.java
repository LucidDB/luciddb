/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;

/**
 * Local variable.
 *
 * @author jhyde
 * @since Oct 25, 2005
 * @version $Id$
 */
public class RexLocalRef extends RexSlot
{
    // array of common names, to reduce memory allocations
    private static final String[] names = makeArray(32, "$t");

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a local variable.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     *
     * @pre type != null
     * @pre index >= 0
     */
    public RexLocalRef(
        int index,
        RelDataType type)
    {
        super(createName(index), index, type);
    }

    //~ Methods ---------------------------------------------------------------
    
    public Object clone()
    {
        return new RexLocalRef(index, type);
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitLocalRef(this);
    }

    public RexNode accept(RexShuttle shuttle)
    {
        return shuttle.visitLocalRef(this);
    }

    private static String createName(int index)
    {
        return index < names.length ?
            names[index] :
            "$t" + index;
    }
}

// End RexLocalRef.java
