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
    extends ExplicitOperandTypeChecker
{
    private final RelDataTypeComparability requiredComparability;
        
    public ComparableOperandTypeChecker(
        RelDataTypeComparability requiredComparability)
    {
        super(
            new SqlTypeName [][] {
                { SqlTypeName.Any },
                { SqlTypeName.Any }
            });
        this.requiredComparability = requiredComparability;
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding, boolean throwOnFailure)
    {
        assert (callBinding.getOperandCount() == 2);
        SqlCall call = callBinding.getCall();
        RelDataType type1 =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                call.operands[0]);
        RelDataType type2 =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                call.operands[1]);
        boolean b = true;
        if (!checkType(callBinding, throwOnFailure, type1)) {
            b = false;
        }
        if (!checkType(callBinding, throwOnFailure, type2)) {
            b = false;
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
}

// End ComparableOperandTypeChecker.java
