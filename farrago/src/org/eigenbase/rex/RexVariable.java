/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;


/**
 * A row-expression which references a field.
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public abstract class RexVariable extends RexNode
{
    //~ Instance fields -------------------------------------------------------

    protected final String name;
    protected final RelDataType type;

    //~ Constructors ----------------------------------------------------------

    protected RexVariable(
        String name,
        RelDataType type)
    {
        assert type != null;
        assert name != null;
        this.name = name;
        this.digest = name;
        this.type = type;
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataType getType()
    {
        return type;
    }

    /**
     * Returns the name of this variable.
     */
    public String getName()
    {
        return name;
    }
}


// End RexVariable.java
