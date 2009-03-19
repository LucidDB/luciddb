/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Type checking strategy which verifies that types have the required attributes
 * to be used as arguments to comparison operators.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ComparableOperandTypeChecker
    extends SameOperandTypeChecker
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataTypeComparability requiredComparability;

    //~ Constructors -----------------------------------------------------------

    public ComparableOperandTypeChecker(
        int nOperands,
        RelDataTypeComparability requiredComparability)
    {
        super(nOperands);
        this.requiredComparability = requiredComparability;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        boolean b = true;
        for (int i = 0; i < nOperands; ++i) {
            RelDataType type = callBinding.getOperandType(i);
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
        if (type.getComparability().ordinal()
            < requiredComparability.ordinal())
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

    /**
     * Similar functionality to {@link #checkOperandTypes(SqlCallBinding,
     * boolean)}, but not part of the interface, and cannot throw an error.
     */
    public boolean checkOperandTypes(
        SqlOperatorBinding callBinding)
    {
        boolean b = true;
        for (int i = 0; i < nOperands; ++i) {
            RelDataType type = callBinding.getOperandType(i);
            boolean result;
            if (type.getComparability().ordinal()
                < requiredComparability.ordinal())
            {
                result = false;
            } else {
                result = true;
            }
            if (!result) {
                b = false;
            }
        }
        if (b) {
            b = super.checkOperandTypes(callBinding);
        }
        return b;
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        String [] array = new String[nOperands];
        Arrays.fill(array, "COMPARABLE_TYPE");
        return SqlUtil.getAliasedSignature(
            op,
            opName,
            Arrays.asList(array));
    }
}

// End ComparableOperandTypeChecker.java
