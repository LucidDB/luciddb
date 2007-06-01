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
package org.eigenbase.rel;

/**
 * Enumeration of join types.
 *
 * @author jhyde
 * @version $Id$
 */
public enum JoinRelType
{
    INNER, LEFT, RIGHT, FULL;

    /**
     * Returns whether a join of this type may generate NULL values on the
     * right-hand side.
     */
    public boolean generatesNullsOnRight()
    {
        return (this == LEFT) || (this == FULL);
    }

    /**
     * Returns whether a join of this type may generate NULL values on the
     * left-hand side.
     */
    public boolean generatesNullsOnLeft()
    {
        return (this == RIGHT) || (this == FULL);
    }
}

// End JoinRelType.java
