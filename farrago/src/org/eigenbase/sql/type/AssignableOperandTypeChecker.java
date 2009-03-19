/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * AssignableOperandTypeChecker implements {@link SqlOperandTypeChecker} by
 * verifying that the type of each argument is assignable to a predefined set of
 * parameter types (under the SQL definition of "assignable").
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class AssignableOperandTypeChecker
    implements SqlOperandTypeChecker
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType [] paramTypes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Instantiates this strategy with a specific set of parameter types.
     *
     * @param paramTypes parameter types for operands; index in this array
     * corresponds to operand number
     */
    public AssignableOperandTypeChecker(RelDataType [] paramTypes)
    {
        this.paramTypes = paramTypes;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange()
    {
        return new SqlOperandCountRange(paramTypes.length);
    }

    // implement SqlOperandTypeChecker
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        for (int i = 0; i < callBinding.getOperandCount(); ++i) {
            RelDataType argType =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(),
                    callBinding.getCall().operands[i]);
            if (!SqlTypeUtil.canAssignFrom(paramTypes[i], argType)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(opName);
        sb.append("(");
        for (int i = 0; i < paramTypes.length; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("<");
            sb.append(paramTypes[i].getFamily().toString());
            sb.append(">");
        }
        sb.append(")");
        return sb.toString();
    }
}

// End AssignableOperandTypeChecker.java
