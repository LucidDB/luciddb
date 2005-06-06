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
 * Type checking strategy which verifies that types have the required
 * attributes to be used as arguments to comparison operators.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ComparableOperandTypeChecker
    extends SameOperandTypeChecker
{
    private final RelDataTypeComparability requiredComparability;
        
    public ComparableOperandTypeChecker(
        int nOperands,
        RelDataTypeComparability requiredComparability)
    {
        super(nOperands);
        this.requiredComparability = requiredComparability;
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding, boolean throwOnFailure)
    {
        boolean b = true;
        for (int i = 0; i < nOperands; ++i) {
            RelDataType type =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(),
                    callBinding.getCall().operands[i]);
            if (!checkType(callBinding, throwOnFailure, type)) {
                b = false;
            }
        }
        if (b) {
            b = super.checkOperandTypes(callBinding, false);
            if (!b && throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
        }
        return b;
    }

    private boolean checkType(
        SqlCallBinding callBinding,
        boolean throwOnFailure,
        RelDataType type)
    {
        if (type.getComparability().getOrdinal() <
            requiredComparability.getOrdinal())
        {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        String [] array = new String[nOperands];
        Arrays.fill(array, "COMPARABLE_TYPE");
        return SqlUtil.getAliasedSignature(op, opName, Arrays.asList(array));
    }
}

// End ComparableOperandTypeChecker.java
