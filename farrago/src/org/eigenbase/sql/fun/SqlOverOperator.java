/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;

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
            ReturnTypeInferenceImpl.useFirstArgType, null,
            OperandsTypeChecking.typeAnyAny);
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlValidator.Scope operandScope)
    {
        assert call.operator == this;
        final SqlNode[] operands = call.getOperands();
        assert operands.length == 2;
        SqlCall aggCall = (SqlCall) operands[0];
        if (!aggCall.operator.isAggregator()) {
            throw validator.newValidationError(aggCall,
                EigenbaseResource.instance().newOverNonAggregate());
        }
        final SqlNode windowOrRef = operands[1];
        validator.validateWindow(windowOrRef, scope);
    }
}

// End SqlOverOperator.java