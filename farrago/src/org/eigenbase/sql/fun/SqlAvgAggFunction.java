/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
 * <code>Avg</code> is an aggregator which returns the average of the values
 * which go into it. It has precisely one argument of numeric type
 * (<code>int</code>, <code>long</code>, <code>float</code>, <code>
 * double</code>), and the result is the same type.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlAvgAggFunction
    extends SqlAggFunction
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;
    private final Subtype subtype;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlAvgAggFunction
     *
     *
     * @param type Data type
     * @param subtype Specific function, e.g. AVG or STDDEV_POP
     */
    public SqlAvgAggFunction(
        RelDataType type,
        Subtype subtype)
    {
        super(
            subtype.name(),
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);
        this.type = type;
        this.subtype = subtype;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType[] { type };
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return type;
    }

    public OJClass [] getStartParameterTypes()
    {
        return new OJClass[0];
    }

    /**
     * Returns the specific function, e.g. AVG or STDDEV_POP.
     *
     * @return Subtype
     */
    public Subtype getSubtype()
    {
        return subtype;
    }

    public enum Subtype {
        AVG,
        STDDEV_POP,
        STDDEV_SAMP,
        VAR_POP,
        VAR_SAMP
    }
}

// End SqlAvgAggFunction.java
