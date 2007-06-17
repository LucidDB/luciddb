/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Definition of the <code>MIN</code> and <code>MAX</code> aggregate functions,
 * returning the returns the smallest/largest of the values which go into it.
 *
 * <p>There are 3 forms:
 *
 * <dl>
 * <dt>sum(<em>primitive type</em>)</dt>
 * <dd>values are compared using &lt;</dd>
 *
 * <dt>sum({@link java.lang.Comparable})</dt>
 * <dd>values are compared using {@link java.lang.Comparable#compareTo}</dd>
 *
 * <dt>sum({@link java.util.Comparator}, {@link java.lang.Object})</dt>
 * <dd>the {@link java.util.Comparator#compare} method of the comparator is used
 * to compare pairs of objects. The comparator is a startup argument, and must
 * therefore be constant for the duration of the aggregation.</dd>
 * </dl>
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlMinMaxAggFunction
    extends SqlAggFunction
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int MINMAX_INVALID = -1;
    public static final int MINMAX_PRIMITIVE = 0;
    public static final int MINMAX_COMPARABLE = 1;
    public static final int MINMAX_COMPARATOR = 2;

    //~ Instance fields --------------------------------------------------------

    public final RelDataType [] argTypes;
    private final boolean isMin;
    private final int kind;

    //~ Constructors -----------------------------------------------------------

    public SqlMinMaxAggFunction(
        RelDataType [] argTypes,
        boolean isMin,
        int kind)
    {
        super(
            isMin ? "MIN" : "MAX",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcComparableOrdered,
            SqlFunctionCategory.System);
        this.argTypes = argTypes;
        this.isMin = isMin;
        this.kind = kind;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isMin()
    {
        return isMin;
    }

    public int getMinMaxKind()
    {
        return kind;
    }

    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        switch (kind) {
        case MINMAX_PRIMITIVE:
        case MINMAX_COMPARABLE:
            return argTypes;
        case MINMAX_COMPARATOR:
            return new RelDataType[] { argTypes[1] };
        default:
            throw Util.newInternal("bad kind: " + kind);
        }
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        switch (kind) {
        case MINMAX_PRIMITIVE:
        case MINMAX_COMPARABLE:
            return argTypes[0];
        case MINMAX_COMPARATOR:
            return argTypes[1];
        default:
            throw Util.newInternal("bad kind: " + kind);
        }
    }

    public OJClass [] getStartParameterTypes()
    {
        switch (kind) {
        case MINMAX_PRIMITIVE:
        case MINMAX_COMPARABLE:
            return new OJClass[0];
        case MINMAX_COMPARATOR:
            return new OJClass[] { OJUtil.clazzComparator };
        default:
            throw Util.newInternal("bad kind: " + kind);
        }
    }
}

// End SqlMinMaxAggFunction.java
