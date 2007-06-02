/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;


/**
 * Operand type-checking strategy which checks operands for inclusion in type
 * families.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FamilyOperandTypeChecker
    implements SqlSingleOperandTypeChecker
{
    //~ Instance fields --------------------------------------------------------

    protected SqlTypeFamily [] families;

    //~ Constructors -----------------------------------------------------------

    public FamilyOperandTypeChecker(SqlTypeFamily ... families)
    {
        this.families = families;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlSingleOperandTypeChecker
    public boolean checkSingleOperandType(
        SqlCallBinding callBinding,
        SqlNode node,
        int iFormalOperand,
        boolean throwOnFailure)
    {
        SqlTypeFamily family = families[iFormalOperand];
        if (family == SqlTypeFamily.ANY) {
            // no need to check
            return true;
        }
        if (SqlUtil.isNullLiteral(node, false)) {
            if (throwOnFailure) {
                throw callBinding.getValidator().newValidationError(
                    node,
                    EigenbaseResource.instance().NullIllegal.ex());
            } else {
                return false;
            }
        }
        RelDataType type =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                node);
        SqlTypeName typeName = type.getSqlTypeName();
        if (!family.getTypeNames().contains(typeName)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }

    // implement SqlOperandTypeChecker
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        if (families.length != callBinding.getOperandCount()) {
            // assume this is an inapplicable sub-rule of a composite rule;
            // don't throw
            return false;
        }

        for (int i = 0; i < callBinding.getOperandCount(); i++) {
            SqlNode operand = callBinding.getCall().operands[i];
            if (!checkSingleOperandType(
                    callBinding,
                    operand,
                    i,
                    throwOnFailure))
            {
                return false;
            }
        }
        return true;
    }

    // implement SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange()
    {
        return new SqlOperandCountRange(families.length);
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        return SqlUtil.getAliasedSignature(
            op,
            opName,
            Arrays.asList(families));
    }

    // hack for FarragoCalcSystemTest
    public SqlTypeFamily [] getFamilies()
    {
        return families;
    }
}

// End FamilyOperandTypeChecker.java
