/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Definition of the "SUBSTRING" builtin SQL function.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Sep 5, 2004
 */
public class SqlSubstringFunction
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    SqlSubstringFunction()
    {
        super(
            "SUBSTRING",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableVaryingFirstArgType,
            null,
            null,
            SqlFunctionCategory.String);
    }

    //~ Methods ----------------------------------------------------------------

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} FROM {2})";
        case 3:
            return "{0}({1} FROM {2} FOR {3})";
        }
        assert (false);
        return null;
    }

    public String getAllowedSignatures(String opName)
    {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < SqlTypeName.stringTypes.length; i++) {
            if (i > 0) {
                ret.append(NL);
            }
            ArrayList<SqlTypeName> list = new ArrayList<SqlTypeName>();
            list.add(SqlTypeName.stringTypes[i]);
            list.add(SqlTypeName.INTEGER);
            ret.append(SqlUtil.getAliasedSignature(this, opName, list));
            ret.append(NL);
            list.add(SqlTypeName.INTEGER);
            ret.append(SqlUtil.getAliasedSignature(this, opName, list));
        }
        return ret.toString();
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCall call = callBinding.getCall();
        SqlValidator validator = callBinding.getValidator();
        SqlValidatorScope scope = callBinding.getScope();

        int n = call.operands.length;
        assert ((3 == n) || (2 == n));
        if (!SqlTypeStrategies.otcString.checkSingleOperandType(
                callBinding,
                call.operands[0],
                0,
                throwOnFailure))
        {
            return false;
        }
        if (2 == n) {
            if (!SqlTypeStrategies.otcNumeric.checkSingleOperandType(
                    callBinding,
                    call.operands[1],
                    0,
                    throwOnFailure))
            {
                return false;
            }
        } else {
            RelDataType t1 = validator.deriveType(scope, call.operands[1]);
            RelDataType t2 = validator.deriveType(scope, call.operands[2]);

            if (SqlTypeUtil.inCharFamily(t1)) {
                if (!SqlTypeStrategies.otcString.checkSingleOperandType(
                        callBinding,
                        call.operands[1],
                        0,
                        throwOnFailure))
                {
                    return false;
                }
                if (!SqlTypeStrategies.otcString.checkSingleOperandType(
                        callBinding,
                        call.operands[2],
                        0,
                        throwOnFailure))
                {
                    return false;
                }

                if (!SqlTypeUtil.isCharTypeComparable(
                        validator,
                        scope,
                        call.operands,
                        throwOnFailure))
                {
                    return false;
                }
            } else {
                if (!SqlTypeStrategies.otcNumeric.checkSingleOperandType(
                        callBinding,
                        call.operands[1],
                        0,
                        throwOnFailure))
                {
                    return false;
                }
                if (!SqlTypeStrategies.otcNumeric.checkSingleOperandType(
                        callBinding,
                        call.operands[2],
                        0,
                        throwOnFailure))
                {
                    return false;
                }
            }

            if (!SqlTypeUtil.inSameFamily(t1, t2)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        }
        return true;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.TwoOrThree;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        operands[1].unparse(writer, leftPrec, rightPrec);

        if (3 == operands.length) {
            writer.sep("FOR");
            operands[2].unparse(writer, leftPrec, rightPrec);
        }

        writer.endFunCall(frame);
    }
}

// End SqlSubstringFunction.java
