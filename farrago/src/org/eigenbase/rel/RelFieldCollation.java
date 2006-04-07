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

package org.eigenbase.rel;


/**
 * RelFieldCollation defines the ordering for one field of a RelNode whose
 * output is to be sorted.
 *
 *<p>
 *
 * TODO:  collation sequence (including ASC/DESC)
 */
public class RelFieldCollation
{
    //~ Static fields/initializers --------------------------------------------

    public static final RelFieldCollation [] emptyCollationArray =
        new RelFieldCollation[0];

    //~ Instance fields -------------------------------------------------------

    /**
     * 0-based index of field being sorted.
     */
    private final int fieldIndex;

    /**
     * Direction of sorting.
     */
    private final Direction direction;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an ascending field collation.
     */
    public RelFieldCollation(int fieldIndex)
    {
        this(fieldIndex, Direction.Ascending);
    }

    /**
     * Creates a field collation.
     */
    public RelFieldCollation(int fieldIndex, Direction direction)
    {
        this.fieldIndex = fieldIndex;
        this.direction = direction;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelFieldCollation)) {
            return false;
        }
        RelFieldCollation other = (RelFieldCollation) obj;
        return fieldIndex == other.fieldIndex &&
            direction == other.direction;
    }

    // implement Object
    public int hashCode()
    {
        return this.direction.ordinal() << 4 | this.fieldIndex;
    }

    public int getFieldIndex()
    {
        return fieldIndex;
    }

    public RelFieldCollation.Direction getDirection()
    {
        return direction;
    }

    public String toString()
    {
        return fieldIndex + " " + direction;
    }

    /**
     * Direction that a field is ordered in.
     */
    public static enum Direction
    {
        /**
         * Ascending direction: A value is always followed by a greater
         * or equal value.
         */
        Ascending,

        /**
         * Strictly ascending direction: A value is always followed by a
         * greater value.
         */
        StrictlyAscending,

        /**
         * Descending direction: A value is always followed by a lesser
         * or equal value.
         */
        Descending,

        /**
         * Strictly descending direction: A value is always followed by a
         * lesser value.
         */
        StrictlyDescending,

        /**
         * Clustered direction: Values occur in no particular order, and
         * the same value may occur in contiguous groups, but never occurs
         * after that. This sort order tends to occur when values are
         * ordered according to a hash-key.
         */
        Clustered,
    }
}


// End RelFieldCollation.java
