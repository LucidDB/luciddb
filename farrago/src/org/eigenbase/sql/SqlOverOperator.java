/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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

package org.eigenbase.sql;

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.util.SqlBasicVisitor;
import org.eigenbase.reltype.RelDataType;

/**
 * An operator describing a window function specification.
 *
 * <p>Operands are as follows:<ul>
 * <li>0: name of window function ({@link org.eigenbase.sql.SqlCall})</li>
 *
 * <li>1: window name ({@link org.eigenbase.sql.SqlLiteral})
 * or window in-line specification ({@link SqlWindowOperator})</li>
 *
 * </ul></p>
 *
 * @author klo
 * @since Nov 4, 2004
 * @version $Id$
 **/
public class SqlOverOperator extends SqlBinaryOperator
{
    public SqlOverOperator()
    {
        super("OVER", SqlKind.Over, 10, true,
            SqlTypeStrategies.rtiFirstArgType, null,
            SqlTypeStrategies.otcAnyX2);
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        assert call.getOperator() == this;
        final SqlNode[] operands = call.getOperands();
        assert operands.length == 2;
        SqlCall aggCall = (SqlCall) operands[0];
        if (!aggCall.getOperator().isAggregator()) {
            throw validator.newValidationError(aggCall,
                EigenbaseResource.instance().OverNonAggregate.ex());
        }
        validator.validateWindow(operands[1], scope, aggCall);
    }

    public RelDataType deriveType(
        SqlValidator validator, SqlValidatorScope scope, SqlCall call)
    {
        // Do not try to derive the types of the operands. We will do that
        // later, top down.
        return validateOperands(validator, scope, call);
    }

    /*
     * Accepts a {@link SqlVisitor}, and tells it to visit each child.
     *
     * @param visitor Visitor.
     */
    public <R> void acceptCall(
        SqlVisitor<R> visitor, SqlCall call, boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];
                // if the second parm is an Identifier then it's supposed to
                // be a name from a window clause and isn't part of the
                // group by check
                if (operand == null) {
                    continue;
                }
                if (i == 1 && operand instanceof SqlIdentifier) {
                    continue;
                }
                argHandler.visitChild(visitor, call, i, operand);
            }
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }
}

// End SqlOverOperator.java
