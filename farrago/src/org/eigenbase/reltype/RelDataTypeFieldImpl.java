/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
 * Default implementation of {@link RelDataTypeField}.
 *
 * @author jhyde
 * @version $Id$
 */
public class RelDataTypeFieldImpl implements RelDataTypeField
{
    private final RelDataType type;
    private final String name;
    private final int index;

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

    public String getName()
    {
        return name;
    }

    public int getIndex()
    {
        return index;
    }

    public RelDataType getType()
    {
        return type;
    }

    public Object get(Object o)
    {
        throw new UnsupportedOperationException();
    }

    public void set(
        Object o,
        Object value)
    {
        throw new UnsupportedOperationException();
    }
}

// End RelDataTypeFieldImpl.java
