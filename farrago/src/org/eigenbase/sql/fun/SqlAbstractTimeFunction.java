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
package org.eigenbase.sql.fun;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Base class for time functions such as "LOCALTIME", "LOCALTIME(n)".
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlAbstractTimeFunction
    extends SqlFunction
{

    //~ Static fields/initializers ---------------------------------------------

    private static final SqlOperandTypeChecker otcCustom =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.OR,
            SqlTypeStrategies.otcPositiveIntLit,
            SqlTypeStrategies.otcNiladic);

    //~ Instance fields --------------------------------------------------------

    private final SqlTypeName typeName;

    //~ Constructors -----------------------------------------------------------

    protected SqlAbstractTimeFunction(String name, SqlTypeName typeName)
    {
        super(name,
            SqlKind.Function,
            null,
            null,
            otcCustom,
            SqlFunctionCategory.TimeDate);
        this.typeName = typeName;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.FunctionId;
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        // REVIEW jvs 20-Feb-2005: Need to take care of time zones.
        int precision = 0;
        if (opBinding.getOperandCount() == 1) {
            RelDataType type = opBinding.getOperandType(0);
            if (SqlTypeUtil.isNumeric(type)) {
                precision = opBinding.getIntLiteralOperand(0);
            }
        }
        assert (precision >= 0);
        if (precision > SqlTypeName.MAX_DATETIME_PRECISION) {
            throw EigenbaseResource.instance().ArgumentMustBeValidPrecision.ex(
                opBinding.getOperator().getName(),
                "0",
                String.valueOf(SqlTypeName.MAX_DATETIME_PRECISION));
        }
        return opBinding.getTypeFactory().createSqlType(typeName, precision);
    }

    // All of the time functions are monotonic.
    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
    {
        return true;
    }

    // Plans referencing context variables should never be cached
    public boolean isDynamicFunction()
    {
        return true;
    }
}

// End SqlAbstractTimeFunction.java
