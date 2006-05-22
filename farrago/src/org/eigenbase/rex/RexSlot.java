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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Util;


/**
 * Abstract base class for {@link RexInputRef} and {@link RexLocalRef}.
 *
 * @author jhyde
 * @since Oct 25, 2005
 * @version $Id$
 */
public abstract class RexSlot extends RexVariable
{
    //~ Instance fields -------------------------------------------------------

    protected final int index;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a slot.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     *
     * @pre type != null
     * @pre index >= 0
     */
    protected RexSlot(
        String name,
        int index,
        RelDataType type)
    {
        super(name, type);
        Util.pre(type != null, "type != null");
        Util.pre(index >= 0, "index >= 0");
        this.index = index;
    }

    //~ Methods ---------------------------------------------------------------

    public int getIndex()
    {
        return index;
    }

    protected static String[] makeArray(int length, String prefix)
    {
        final String[] a = new String[length];
        for (int i = 0; i < a.length; i++) {
            a[i] = prefix + i;
        }
        return a;
    }
}

// End RexSlot.java
