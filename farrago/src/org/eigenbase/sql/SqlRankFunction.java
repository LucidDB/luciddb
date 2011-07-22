/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Operator which aggregates sets of values into a result.
 *
 * @author jack
 * @version $Id$
 * @since Jun 3, 2005
 */
public class SqlRankFunction
    extends SqlAggFunction
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType type = null;

    //~ Constructors -----------------------------------------------------------

    public SqlRankFunction(String name)
    {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiInteger,
            null,
            SqlTypeStrategies.otcNiladic,
            SqlFunctionCategory.Numeric);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Zero;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return type;
    }

    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return new RelDataType[] { type };
    }

    public boolean isAggregator()
    {
        return true;
    }

    @Override
    public void validateWindowedAggregate(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope,
        SqlWindow window)
    {
        super.validateWindowedAggregate(
            call, validator, scope, operandScope, window);
        if (call.isName("RANK") || call.isName("DENSE_RANK")) {
            // 6.10 rule 6a Function RANk & DENSE_RANK require OBC
            SqlNodeList orderList = window.getOrderList();
            if ((null == orderList || orderList.size() == 0)
                && !SqlWindowOperator.isTableSorted(scope))
            {
                throw validator.newValidationError(
                    call,
                    EigenbaseResource.instance().FuncNeedsOrderBy.ex());
            }

            // Run framing checks if there are any
            if ((window.getUpperBound() != null)
                || (window.getLowerBound() != null))
            {
                // 6.10 Rule 6a
                    throw validator.newValidationError(
                        window.getOperands()[SqlWindow.IsRows_OPERAND],
                        EigenbaseResource.instance().RankWithFrame.ex());
            }
        }
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        final SqlParserPos pos = call.getParserPosition();
        throw SqlUtil.newContextException(
            pos,
            EigenbaseResource.instance().FunctionUndefined.ex(
                call.toString()));
    }
}

// End SqlRankFunction.java
