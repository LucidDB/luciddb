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

import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.type.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;

import java.util.List;
import java.util.ArrayList;

/**
 * Definition of the SQL <code>IN</code> operator, which tests for a value's
 * membership in a subquery or a list of values.
 *
 * @author jhyde
 * @since April 17, 2006
 * @version $Id$
 */
class SqlInOperator extends SqlBinaryOperator
{
    SqlInOperator()
    {
        super(
            "IN", SqlKind.In, 15, true, SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown, null);
    }

    public RelDataType deriveType(
        SqlValidator validator, SqlValidatorScope scope, SqlCall call)
    {
        final SqlNode[] operands = call.getOperands();
        if (operands.length == 2 &&
            call.operands[1] instanceof SqlNodeList) {
            // Validate the 'IN (expr, ...)' form.
            RelDataType leftType = validator.deriveType(scope, operands[0]);
            List<RelDataType> rightTypeList = new ArrayList<RelDataType>();
            SqlNodeList nodeList = (SqlNodeList) call.operands[1];
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                RelDataType rightType = validator.deriveType(scope, node);
                rightTypeList.add(rightType);
            }
            RelDataType[] rightTypes =
                (RelDataType[])
                rightTypeList.toArray(new RelDataType[rightTypeList.size()]);
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType rightType =
                typeFactory.leastRestrictive(rightTypes);

            // First check that the expressions in the IN list are compatible
            // with each other. Same rules as the VALUES operator (per
            // "SQL:2003 section 8.4, <in predicate>").
            if (null == rightType) {
                throw validator.newValidationError(
                    call.operands[1],
                    EigenbaseResource.instance().IncompatibleTypesInList.ex());
            }

            // Now check that the left expression is compatible with the
            // type of the list. Same strategy as the '=' operator.
            final ComparableOperandTypeChecker checker =
                (ComparableOperandTypeChecker)
                SqlTypeStrategies.otcComparableUnorderedX2;
            if (!checker.checkOperandTypes(
                new ExplicitOperatorBinding(
                    typeFactory,
                    this,
                    new RelDataType[] {leftType, rightType}))) {
                throw validator.newValidationError(
                    call,
                    EigenbaseResource.instance().IncompatibleValueType.ex(
                        SqlStdOperatorTable.inOperator.getName()));
            }

            // Result is a boolean, nullable if there are any nullable types
            // on either side.
            RelDataType type = typeFactory.createSqlType(SqlTypeName.Boolean);
            if (leftType.isNullable() ||
                SqlTypeUtil.containsNullable(rightTypes)) {
                type = typeFactory.createTypeWithNullability(type, true);
            }
            return type;
        }

        return super.deriveType(validator, scope, call);
    }
}

// End SqlInOperator.java
