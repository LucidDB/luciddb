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
 * Parameter type-checking strategy where all operand types must be
 * either the same or null.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SameOperandTypeChecker implements SqlOperandTypeChecker
{
    protected final int nOperands;

    public SameOperandTypeChecker(
        int nOperands)
    {
        this.nOperands = nOperands;
    }

    // implement SqlOperandTypeChecker
    public boolean checkOperandTypes(
        SqlCallBinding callBinding, boolean throwOnFailure)
    {
        RelDataType [] types = new RelDataType[nOperands];
        for (int i = 0; i < nOperands; ++i) {
            SqlNode operand = callBinding.getCall().operands[i];
            if (SqlUtil.isNullLiteral(operand, false)) {
                if (throwOnFailure) {
                    throw callBinding.getValidator().newValidationError(
                        operand,
                        EigenbaseResource.instance().NullIllegal.ex());
                } else {
                    return false;
                }
            }
            types[i] =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(),
                    operand);
        }
        int prev = -1;
        for (int i = 0; i < nOperands; ++i) {
            if (prev == -1) {
                prev = i;
            } else {
                RelDataTypeFamily family1 = types[i].getFamily();
                RelDataTypeFamily family2 = types[prev].getFamily();
                // REVIEW jvs 2-June-2005:  This is needed to keep
                // the Saffron type system happy.
                if (types[i].getSqlTypeName() != null) {
                    family1 = types[i].getSqlTypeName().getFamily();
                }
                if (types[prev].getSqlTypeName() != null) {
                    family2 = types[prev].getSqlTypeName().getFamily();
                }
                if (family1 == family2) {
                    continue;
                }
                if (!throwOnFailure) {
                    return false;
                }
                // REVIEW jvs 5-June-2005:  Why don't we use
                // newValidationSignatureError() here?  It gives more
                // specific diagnostics.
                throw callBinding.newValidationError(
                    EigenbaseResource.instance().NeedSameTypeParameter.ex());
            }
        }
        return true;
    }
    
    // implement SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange()
    {
        return new SqlOperandCountRange(nOperands);
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        String [] array = new String[nOperands];
        Arrays.fill(array, "EQUIVALENT_TYPE");
        return SqlUtil.getAliasedSignature(op, opName, Arrays.asList(array));
    }
}

// End SameOperandTypeChecker.java
