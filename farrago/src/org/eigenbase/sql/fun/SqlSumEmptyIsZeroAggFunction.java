/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package org.eigenbase.sql.fun;

import openjava.mop.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * <code>Sum0</code> is an aggregator which returns the sum of the values which
 * go into it like <code>Sum</code>. It differs in that when no non null values
 * are applied zero is returned instead of null. Can be used along with <code>
 * Count</code> to implement <code>Sum</code>.
 *
 * @author jhahn
 * @version $Id$
 */
public class SqlSumEmptyIsZeroAggFunction
    extends SqlAggFunction
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    public SqlSumEmptyIsZeroAggFunction(RelDataType type)
    {
        super(
            "$SUM0",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType[] { type };
    }

    public RelDataType getType()
    {
        return type;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return type;
    }

    public OJClass [] getStartParameterTypes()
    {
        return new OJClass[0];
    }
}

// End SqlSumEmptyIsZeroAggFunction.java
