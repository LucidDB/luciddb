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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Parameter type-checking strategy where all operand types must be the same.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SameOperandTypeChecker
    implements SqlOperandTypeChecker
{
    //~ Instance fields --------------------------------------------------------

    protected final int nOperands;

    //~ Constructors -----------------------------------------------------------

    public SameOperandTypeChecker(
        int nOperands)
    {
        this.nOperands = nOperands;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SqlOperandTypeChecker
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        return checkOperandTypesImpl(
            callBinding,
            throwOnFailure,
            callBinding);
    }

    private boolean checkOperandTypesImpl(
        SqlOperatorBinding operatorBinding,
        boolean throwOnFailure,
        SqlCallBinding callBinding)
    {
        int nOperandsActual = nOperands;
        if (nOperandsActual == -1) {
            nOperandsActual = operatorBinding.getOperandCount();
        }
        assert !(throwOnFailure && (callBinding == null));
        RelDataType [] types = new RelDataType[nOperandsActual];
        for (int i = 0; i < nOperandsActual; ++i) {
            if (operatorBinding.isOperandNull(i, false)) {
                if (throwOnFailure) {
                    throw callBinding.getValidator().newValidationError(
                        callBinding.getCall().operands[i],
                        EigenbaseResource.instance().NullIllegal.ex());
                } else {
                    return false;
                }
            }
            types[i] = operatorBinding.getOperandType(i);
        }
        for (int i = 1; i < nOperandsActual; ++i) {
            if (!checkTypePair(types[i], types[i - 1])) {
                if (!throwOnFailure) {
                    return false;
                }

                // REVIEW jvs 5-June-2005: Why don't we use
                // newValidationSignatureError() here?  It gives more
                // specific diagnostics.
                throw callBinding.newValidationError(
                    EigenbaseResource.instance().NeedSameTypeParameter.ex());
            }
        }
        return true;
    }

    private boolean checkTypePair(RelDataType type1, RelDataType type2)
    {
        if (type1.isStruct() != type2.isStruct()) {
            return false;
        }

        if (type1.isStruct()) {
            int n = type1.getFieldCount();
            if (n != type2.getFieldCount()) {
                return false;
            }
            for (int i = 0; i < n; ++i) {
                RelDataTypeField field1 =
                    (RelDataTypeField) type1.getFieldList().get(i);
                RelDataTypeField field2 =
                    (RelDataTypeField) type2.getFieldList().get(i);
                if (!checkTypePair(
                        field1.getType(),
                        field2.getType()))
                {
                    return false;
                }
            }
            return true;
        }
        RelDataTypeFamily family1 = null;
        RelDataTypeFamily family2 = null;

        // REVIEW jvs 2-June-2005:  This is needed to keep
        // the Saffron type system happy.
        if (type1.getSqlTypeName() != null) {
            family1 = type1.getSqlTypeName().getFamily();
        }
        if (type2.getSqlTypeName() != null) {
            family2 = type2.getSqlTypeName().getFamily();
        }
        if (family1 == null) {
            family1 = type1.getFamily();
        }
        if (family2 == null) {
            family2 = type2.getFamily();
        }
        if (family1 == family2) {
            return true;
        }
        return false;
    }

    /**
     * Similar functionality to {@link #checkOperandTypes(SqlCallBinding,
     * boolean)}, but not part of the interface, and cannot throw an error.
     */
    public boolean checkOperandTypes(
        SqlOperatorBinding operatorBinding)
    {
        return checkOperandTypesImpl(operatorBinding, false, null);
    }

    // implement SqlOperandTypeChecker
    public SqlOperandCountRange getOperandCountRange()
    {
        if (nOperands == -1) {
            return SqlOperandCountRange.Variadic;
        } else {
            return new SqlOperandCountRange(nOperands);
        }
    }

    // implement SqlOperandTypeChecker
    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        int nOperandsActual = nOperands;
        if (nOperandsActual == -1) {
            nOperandsActual = 3;
        }
        String [] array = new String[nOperandsActual];
        Arrays.fill(array, "EQUIVALENT_TYPE");
        if (nOperands == -1) {
            array[2] = "...";
        }
        return SqlUtil.getAliasedSignature(
            op,
            opName,
            Arrays.asList(array));
    }
}

// End SameOperandTypeChecker.java
