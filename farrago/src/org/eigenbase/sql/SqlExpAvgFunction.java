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
package org.eigenbase.sql;

import openjava.mop.OJClass;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.type.SqlTypeStrategies;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

/**
 * Operator which computes exponentially decaying weighted average.
 *
 * @author jhahn
 * @version $Id$
 * @since Jul 21, 2011
 */
public class SqlExpAvgFunction extends SqlAggFunction
{

    @Override
    public void validateWindowedAggregate(
        SqlCall call, SqlValidator validator,
        SqlValidatorScope scope, SqlValidatorScope operandScope,
        SqlWindow window)
    {
        super.validateWindowedAggregate(
            call, validator, scope, operandScope, window);
        if (window.isRows()) {
            throw validator.newValidationError(
                call,
                EigenbaseResource.instance()
                    .NotSupportedInRowsBasedWindow.ex(getName()));
        }
    }

    private final RelDataType type;

    public SqlExpAvgFunction(RelDataType type)
    {
        super(
            "EXP_AVG",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiFirstArgTypeForceNullable,
            null,
            SqlTypeStrategies.otcNumericDayTimeIntervalLit,
            SqlFunctionCategory.Numeric);
        this.type = type;
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
// End SqlExpAvgFunction.java