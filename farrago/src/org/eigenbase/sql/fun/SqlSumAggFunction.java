/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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
package org.eigenbase.sql.fun;

import openjava.mop.OJClass;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

/**
 * <code>Sum</code> is an aggregator which returns the sum of the values
 * which go into it. It has precisely one argument of numeric type
 * (<code>int</code>, <code>long</code>, <code>float</code>,
 * <code>double</code>), and the result is the same type.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlSumAggFunction extends SqlAggFunction
{
    public final RelDataType type;

    public SqlSumAggFunction(RelDataType type)
    {
        super(
            "SUM", SqlKind.Function,
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeNumeric,
            SqlFunction.SqlFuncTypeName.Numeric);
        this.type = type;
    }

    public RelDataType[] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType [] { type };
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

// End SqlSumAggFunction.java
