/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

/**
 * RelDataTypePrecedenceList defines a type precedence list
 * for a particular type.
 *
 * @sql.99 Part 2 Section 9.5
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface RelDataTypePrecedenceList
{
    /**
     * Determines whether a type appears in this precedence list.
     *
     * @param type type to check
     *
     * @return true iff this list contains type
     */
    public boolean containsType(RelDataType type);

    /**
     * Compares the precedence of two types.
     *
     * @param type1 first type to compare
     *
     * @param type2 second type to compare
     *
     * @return positive if type1 has higher precedence; negative if type2 has
     * higher precedence; 0 if types have equal precedence
     *
     * @pre containsType(type1) && containsType(type2)
     */
    public int compareTypePrecedence(RelDataType type1, RelDataType type2);
}

// End RelDataTypePrecedenceList.java
