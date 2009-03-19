/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


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
    private final SqlOperatorBinding delegate;

    //~ Constructors -----------------------------------------------------------

    public ExplicitOperatorBinding(
        SqlOperatorBinding delegate,
        RelDataType [] types)
    {
        this(
            delegate,
            delegate.getTypeFactory(),
            delegate.getOperator(),
            types);
    }

    public ExplicitOperatorBinding(
        RelDataTypeFactory typeFactory,
        SqlOperator operator,
        RelDataType [] types)
    {
        this(null, typeFactory, operator, types);
    }

    private ExplicitOperatorBinding(
        SqlOperatorBinding delegate,
        RelDataTypeFactory typeFactory,
        SqlOperator operator,
        RelDataType [] types)
    {
        super(typeFactory, operator);
        this.types = types;
        this.delegate = delegate;
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

    public EigenbaseException newError(
        SqlValidatorException e)
    {
        if (delegate != null) {
            return delegate.newError(e);
        } else {
            return SqlUtil.newContextException(SqlParserPos.ZERO, e);
        }
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
