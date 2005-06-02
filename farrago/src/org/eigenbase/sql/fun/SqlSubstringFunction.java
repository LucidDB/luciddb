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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;

import java.util.ArrayList;

/**
 * Definition of the "SUBSTRING" builtin SQL function.
 *
 * @author Wael Chatila
 * @since Sep 5, 2004
 * @version $Id$
 */
public class SqlSubstringFunction extends SqlFunction {

    SqlSubstringFunction() {
        super("SUBSTRING", SqlKind.Function,
            SqlTypeStrategies.rtiNullableVaryingFirstArgType, null, null,
            SqlFunctionCategory.String);
    }

    protected String getSignatureTemplate(final int operandsCount)
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

    public String getAllowedSignatures(String name)
    {
        StringBuffer ret = new StringBuffer();
        for (int i = 0; i < SqlTypeName.stringTypes.length;i++) {
            if (i > 0) {
                ret.append(NL);
            }
            ArrayList list = new ArrayList();
            list.add(SqlTypeName.stringTypes[i]);
            list.add(SqlTypeName.Integer);
            ret.append(this.getAnonymousSignature(list));
            ret.append(NL);
            list.add(SqlTypeName.Integer);
            ret.append(this.getAnonymousSignature(list));
        }
        return replaceAnonymous(
            ret.toString(),
            name);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        boolean throwOnFailure)
    {
        int n = call.operands.length;
        assert ((3 == n) || (2 == n));
        if (!SqlTypeStrategies.otcNullableString.check(call, validator,
            scope, call.operands[0], 0, throwOnFailure)) {
            return false;
        }
        if (2 == n) {
            if (!SqlTypeStrategies.otcNullableNumeric.check(call, validator,
                scope, call.operands[1], 0, throwOnFailure)) {
                return false;
            }
        } else {
            RelDataType t1 =
                validator.deriveType(scope, call.operands[1]);
            RelDataType t2 =
                validator.deriveType(scope, call.operands[2]);

            if (SqlTypeUtil.inCharFamily(t1)) {
                if (!SqlTypeStrategies.otcNullableString.check(call, validator,
                    scope, call.operands[1], 0, throwOnFailure)) {
                    return false;
                }
                if (!SqlTypeStrategies.otcNullableString.check(call, validator,
                    scope, call.operands[2], 0, throwOnFailure)) {
                    return false;
                }

                if (!SqlTypeUtil.isCharTypeComparable(
                    validator, scope, call.operands, throwOnFailure)) {
                    return false;
                }
            } else {
                if (!SqlTypeStrategies.otcNullableNumeric.check(call, validator,
                    scope, call.operands[1], 0, throwOnFailure)) {
                    return false;
                }
                if (!SqlTypeStrategies.otcNullableNumeric.check(call, validator,
                    scope, call.operands[2], 0, throwOnFailure)) {
                    return false;
                }
            }

            if (!SqlTypeUtil.inSameFamily(t1, t2)) {
                if (throwOnFailure) {
                    throw call.newValidationSignatureError(validator, scope);
                }
                return false;
            }
        }
        return true;
    }

    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return new SqlOperator.OperandsCountDescriptor(2, 3);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print(getName());
        writer.print("(");
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.print(" FROM ");
        operands[1].unparse(writer, leftPrec, rightPrec);

        if (3 == operands.length) {
            writer.print(" FOR ");
            operands[2].unparse(writer, leftPrec, rightPrec);
        }

        writer.print(")");
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testSubstringFunction(tester);

    }
}

// End SqlSubstringFunction.java
