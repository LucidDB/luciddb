/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

package org.eigenbase.util.mapping;

/**
 * An immutable pair of integers.
 *
 * @see Mapping#iterator() 
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 24, 2006
 */
public class IntPair
{
    public IntPair(int source, int target)
    {
        this.source = source;
        this.target = target;
    }

    public final int source;
    public final int target;

    public String toString()
    {
        return source + "-" + target;
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof IntPair) {
            IntPair that = (IntPair) obj;
            return this.source == that.source &&
                this.target == that.target;
        }
        return false;
    }

    public int hashCode()
    {
        return this.source ^ (this.target << 4);
    }
}

// End IntPair.java
