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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * Dynamic parameter reference in a row-expression.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RexDynamicParam extends RexVariable
{
    //~ Instance fields -------------------------------------------------------

    public final int index;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a dynamic parameter.
     *
     * @param type inferred type of parameter
     *
     * @param index 0-based index of dynamic parameter in statement
     */
    public RexDynamicParam(
        RelDataType type,
        int index)
    {
        super("?" + index, type);
        this.index = index;
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new RexDynamicParam(type, index);
    }

    public RexKind getKind()
    {
        return RexKind.DynamicParam;
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitDynamicParam(this);
    }
}


// End RexDynamicParam.java
