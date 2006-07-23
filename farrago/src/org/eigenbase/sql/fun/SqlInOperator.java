/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Definition of the SQL <code>IN</code> operator, which tests for a value's
 * membership in a subquery or a list of values.
 *
 * @author jhyde
 * @version $Id$
 * @since April 17, 2006
 */
class SqlInOperator
    extends SqlBinaryOperator
{

    //~ Constructors -----------------------------------------------------------

    SqlInOperator()
    {
        super(
            "IN",
            SqlKind.In,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            null);
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        final SqlNode [] operands = call.getOperands();
        assert (operands.length == 2);

        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        RelDataType leftType = validator.deriveType(scope, operands[0]);
        RelDataType rightType;

        // Derive type for RHS.
        if (call.operands[1] instanceof SqlNodeList) {
            // Handle the 'IN (expr, ...)' form.
            List<RelDataType> rightTypeList = new ArrayList<RelDataType>();
            SqlNodeList nodeList = (SqlNodeList) call.operands[1];
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                RelDataType nodeType = validator.deriveType(scope, node);
                rightTypeList.add(nodeType);
            }
            RelDataType [] rightTypes =
                (RelDataType []) rightTypeList.toArray(
                    new RelDataType[rightTypeList.size()]);
            rightType = typeFactory.leastRestrictive(rightTypes);

            // First check that the expressions in the IN list are compatible
            // with each other. Same rules as the VALUES operator (per
            // SQL:2003 Part 2 Section 8.4, <in predicate>).
            if (null == rightType) {
                throw validator.newValidationError(
                    call.operands[1],
                    EigenbaseResource.instance().IncompatibleTypesInList.ex());
            }

            // Record the RHS type for use by SqlToRelConverter.
            validator.setValidatedNodeType(
                nodeList,
                rightType);
        } else {
            // Handle the 'IN (query)' form.
            rightType = validator.deriveType(scope, operands[1]);
        }

        // Now check that the left expression is compatible with the
        // type of the list. Same strategy as the '=' operator.
        // Normalize the types on both sides to be row types
        // for the purposes of compatibility-checking.
        RelDataType leftRowType =
            SqlTypeUtil.promoteToRowType(
                typeFactory,
                leftType,
                null);
        RelDataType rightRowType =
            SqlTypeUtil.promoteToRowType(
                typeFactory,
                rightType,
                null);

        final ComparableOperandTypeChecker checker =
            (ComparableOperandTypeChecker)
            SqlTypeStrategies.otcComparableUnorderedX2;
        if (!checker.checkOperandTypes(
                new ExplicitOperatorBinding(
                    typeFactory,
                    this,
                    new RelDataType[] { leftRowType, rightRowType }))) {
            throw validator.newValidationError(
                call,
                EigenbaseResource.instance().IncompatibleValueType.ex(
                    SqlStdOperatorTable.inOperator.getName()));
        }

        // Result is a boolean, nullable if there are any nullable types
        // on either side.
        RelDataType type = typeFactory.createSqlType(SqlTypeName.Boolean);
        if (leftType.isNullable() || rightType.isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true);
        }

        return type;
    }
}

// End SqlInOperator.java
