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

import org.eigenbase.resource.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * Base class for time functions such as "LOCALTIME", "LOCALTIME(n)".
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SqlAbstractTimeFunction extends SqlFunction
{
    private final SqlTypeName typeName;

    protected SqlAbstractTimeFunction(String name, SqlTypeName typeName) {
        super(name, SqlKind.Function, null, null, null,
            SqlFunctionCategory.TimeDate);
        this.typeName = typeName;
    }
    
    // no argTypeInference, so must override these methods.
    // Probably need a niladic version of that.
    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return new OperandsCountDescriptor(0, 1);
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.FunctionId;
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope, boolean throwOnFailure)
    {
        if (null != operandsCheckingRule) {
            return super.checkArgTypes(
                call, validator, scope, throwOnFailure);
        } else if (1==call.operands.length) {
            if (!OperandsTypeChecking.typePositiveIntegerLiteral.check(
                    validator,  scope, call, false)) {
                if (throwOnFailure) {
                    throw EigenbaseResource.instance().
                        newArgumentMustBePositiveInteger(
                            call.getOperator().getName());
                }
                return false;
            }
        }
        return true;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        // REVIEW jvs 20-Feb-2005:  SqlTypeName says Time and Timestamp
        // don't take precision, but they should (according to the
        // standard). Also, need to take care of time zones.
        int precision = 0;
        if (callOperands.size() == 1) {
            RelDataType type = callOperands.getType(0);
            if (SqlTypeUtil.isNumeric(type)) {
                precision = callOperands.getIntLiteral(0);
            }
        }
        assert(precision >= 0);
        return typeFactory.createSqlType(typeName, precision);
    }
    
    // All of the time functions are monotonic.
    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope) {
        return true;
    }
}

// End SqlAbstractTimeFunction.java
