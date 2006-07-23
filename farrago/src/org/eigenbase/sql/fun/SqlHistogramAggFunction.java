/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import openjava.mop.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * <code>HistogramAgg</code> is base operator which supports the Histogram
 * MIN/MAX aggregatoin functions. which returns the sum of the values which go
 * into it. It has precisely one argument of numeric type (<code>int</code>,
 * <code>long</code>, <code>float</code>, <code>double</code>) results are
 * retrived with (<code>HistogramMin</code>) and (<code>HistogramMax</code>)
 *
 * @author jfrost
 * @version $Id$
 */
public class SqlHistogramAggFunction
    extends SqlAggFunction
{

    //~ Instance fields --------------------------------------------------------

    private final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    public SqlHistogramAggFunction(RelDataType type)
    {
        super(
            "$HISTOGRAM",
            SqlKind.Function,

            //            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.rtiHistogram,
            null,
            SqlTypeStrategies.otcNumericOrString,
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

// End SqlHistogramAggFunction.java
