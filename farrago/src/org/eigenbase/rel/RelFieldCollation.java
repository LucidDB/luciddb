/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
    public final int iField;

    //~ Constructors ----------------------------------------------------------

    public RelFieldCollation(int iField)
    {
        this.iField = iField;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelFieldCollation)) {
            return false;
        }
        RelFieldCollation other = (RelFieldCollation) obj;
        return iField == other.iField;
    }

    // implement Object
    public int hashCode()
    {
        return iField;
    }

    // implement Object
    public String toString()
    {
        return "RelFieldCollation:" + iField;
    }
}


// End RelFieldCollation.java
