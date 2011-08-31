/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.SqlTypeStrategies;


/**
 * <code>ANY</code> and <code>EVERY>/code are aggregators far evaluating
 * the associated boolean operators.
  *
 * @author jhahn
 * @version $Id$
 */
public class SqlAnyEveryAggFunction
    extends SqlAggFunction
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;

    private final boolean isAny;

    //~ Constructors -----------------------------------------------------------

    public SqlAnyEveryAggFunction(RelDataType type, boolean isAny)
    {
        super(
            isAny ? "ANY" : "EVERY",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcBool,
            SqlFunctionCategory.Numeric);
        this.isAny = isAny;
        this.type = type;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isAny()
    {
        return isAny;
    }

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

// End SqlAnyEveryAggFunction.java
