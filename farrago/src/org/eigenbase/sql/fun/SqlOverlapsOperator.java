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

import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.type.*;
import org.eigenbase.reltype.RelDataType;

import java.util.ArrayList;

/**
 * SqlOverlapsOperator represents the SQL:1999 standard OVERLAPS function
 * Determins if two anchored time intervals overlaps.
 * @author Wael Chatila
 * @since Dec 11, 2004
 * @version $Id$
 */
public class SqlOverlapsOperator extends SqlSpecialOperator {

    public SqlOverlapsOperator() {
        super("OVERLAPS", SqlKind.Overlaps, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown, null);
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testOverlapsOperator(tester);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode[] operands,
        int leftPrec,
        int rightPrec) {
        writer.print("(");
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.print(", ");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.print(") ");
        writer.print(getName());
        writer.print(" (");
        operands[2].unparse(writer, leftPrec, rightPrec);
        writer.print(", ");
        operands[3].unparse(writer, leftPrec, rightPrec);
        writer.print(")");
    }

    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRange.Four;
    }

    public String getSignatureTemplate(int operandsCount)
    {
        if (4 == operandsCount) {
            return "({1}, {2}) {0} ({3}, {4})";
        }
        assert(false);
        return null;
    }

    public String getAllowedSignatures(String opName)
    {
        final String d = "DATETIME";
        final String i = "INTERVAL";
        String[] typeNames = {
            d, d,
            d, i,
            i, d,
            i, i
        };

        StringBuffer ret = new StringBuffer();
        for (int y = 0; y < typeNames.length; y+=2) {
            if (y > 0) {
                ret.append(NL);
            }
            ArrayList list = new ArrayList();
            list.add(d);
            list.add(typeNames[y]);
            list.add(d);
            list.add(typeNames[y+1]);
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
        if (!SqlTypeStrategies.otcDatetime.checkSingleOperandType(
            callBinding, call.operands[0], 0, throwOnFailure)) {
            return false;
        }
        if (!SqlTypeStrategies.otcDatetime.checkSingleOperandType(
            callBinding, call.operands[2], 0, throwOnFailure)) {
            return false;
        }

        RelDataType t0 = validator.deriveType(scope, call.operands[0]);
        RelDataType t1 = validator.deriveType(scope, call.operands[1]);
        RelDataType t2 = validator.deriveType(scope, call.operands[2]);
        RelDataType t3 = validator.deriveType(scope, call.operands[3]);

        // t0 must be comparable with t2
        if (!SqlTypeUtil.sameNamedType(t0, t2)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        if (SqlTypeUtil.isDatetime(t1)) {
            // if t1 is of DATETIME,
            // then t1 must be comparable with t0
            if (!SqlTypeUtil.sameNamedType(t0, t1)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        } else if (!SqlTypeUtil.isInterval(t1)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }

        if (SqlTypeUtil.isDatetime(t3)) {
            // if t3 is of DATETIME,
            // then t3 must be comparable with t2
            if (!SqlTypeUtil.sameNamedType(t2, t3)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError();
                }
                return false;
            }
        } else if (!SqlTypeUtil.isInterval(t3)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }
}
