/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package org.eigenbase.sql.fun;

import openjava.mop.OJClass;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;

/**
 * <code>Count</code> is an aggregator which returns the number of rows
 * which have gone into it. With one argument (or more), it returns the
 * number of rows for which that argument (or all) is not
 * <code>null</code>.
 */
public class SqlCountAggFunction extends SqlAggFunction
{
    public static final RelDataType type = null; // TODO:

    public SqlCountAggFunction()
    {
        super(
            "COUNT", SqlKind.Function, ReturnTypeInferenceImpl.useFirstArgType,
            null, OperandsTypeChecking.typeNumeric,
            SqlFunction.SqlFuncTypeName.Numeric);
    }

    public RelDataType[] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType[] {type};
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

// End SqlCountAggFunction.java
