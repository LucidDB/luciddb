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
package org.eigenbase.sql.type;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * Parameter type-checking strategy for a set operator (UNION, INTERSECT,
 * EXCEPT).
 *
 * <p>Both arguments must be records with the same number
 * of fields, and the fields must be union-compatible.
 *
 * @author Jack Frost
 * @version $Id$
 */
public class SetopOperandTypeChecker implements SqlOperandTypeChecker
{
    public boolean check(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlNode node,
        int ruleOrdinal,
        boolean throwOnFailure)
    {
        assert ruleOrdinal == 0;
        return check(validator, scope, call, throwOnFailure);
    }

    public boolean check(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        boolean throwOnFailure)
    {
        assert call.operands.length == 2 : "setops are binary (for now)";
        RelDataType[] argTypes = new RelDataType[call.operands.length];
        int colCount = -1;
        for (int i = 0; i < argTypes.length; i++) {
            final SqlNode operand = call.operands[i];
            final RelDataType argType =
                argTypes[i] =
                validator.getValidatedNodeType(operand);
            Util.permAssert(argType.isStruct(),
                "setop arg must be a struct");
            if (i == 0) {
                colCount = argTypes[0].getFieldList().size();
                continue;
            }
            // Each operand must have the same number of columns.
            final RelDataTypeField[] fields = argType.getFields();
            if (fields.length != colCount) {
                if (throwOnFailure) {
                    throw validator.newValidationError(
                        operand,
                        EigenbaseResource.instance()
                        .newColumnCountMismatchInSetop(
                            call.getOperator().getName()));
                } else {
                    return false;
                }
            }
        }

        // The columns must be pairwise union compatible. For each column
        // ordinal, form a 'slice' containing the types of the ordinal'th
        // column j.
        RelDataType[] colTypes = new RelDataType[call.operands.length];
        for (int i = 0; i < colCount; i++) {
            for (int j = 0; j < argTypes.length; j++) {
                final RelDataTypeField field = argTypes[j].getFields()[i];
                colTypes[j] = field.getType();
            }
            final RelDataType type =
                validator.getTypeFactory().leastRestrictive(colTypes);
            if (type == null) {
                if (throwOnFailure) {
                    SqlNode field = SqlUtil.getSelectListItem(call.operands[0], i);
                    throw validator.newValidationError(
                        field,
                        EigenbaseResource.instance()
                        .newColumnTypeMismatchInSetop(
                            new Integer(i + 1), // 1-based
                            call.getOperator().getName()));
                } else {
                    return false;
                }
            }

        }

        return true;
    }

    public int getArgCount()
    {
        return 2;
    }

    public String getAllowedSignatures(SqlOperator op)
    {
        return "<UNION>"; // todo: Wael, please review.
    }
}

// End SetopOperandTypeChecker.java
