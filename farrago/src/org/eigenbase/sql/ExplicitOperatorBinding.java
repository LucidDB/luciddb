/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql;

import org.eigenbase.reltype.*;


/**
 * <code>ExplicitOperatorBinding</code> implements {@link SqlOperatorBinding}
 * via an underlying array of known operand types.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class ExplicitOperatorBinding
    extends SqlOperatorBinding
{

    //~ Instance fields --------------------------------------------------------

    private final RelDataType [] types;

    //~ Constructors -----------------------------------------------------------

    public ExplicitOperatorBinding(
        SqlOperatorBinding delegate,
        RelDataType [] types)
    {
        this(
            delegate.getTypeFactory(),
            delegate.getOperator(),
            types);
    }

    public ExplicitOperatorBinding(
        RelDataTypeFactory typeFactory,
        SqlOperator operator,
        RelDataType [] types)
    {
        super(typeFactory, operator);
        this.types = types;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperatorBinding
    public int getOperandCount()
    {
        return types.length;
    }

    // implement SqlOperatorBinding
    public RelDataType getOperandType(int ordinal)
    {
        return types[ordinal];
    }

    public boolean isOperandNull(int ordinal, boolean allowCast)
    {
        // NOTE jvs 1-May-2006:  This call is only relevant
        // for SQL validation, so anywhere else, just say
        // everything's OK.
        return false;
    }
}

// End ExplicitOperatorBinding.java
