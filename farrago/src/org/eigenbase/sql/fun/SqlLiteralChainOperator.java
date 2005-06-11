/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql.fun;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.BitString;
import org.eigenbase.util.NlsString;

/**
 * Internal operator, by which the parser represents a continued string
 * literal.
 *
 * <p>The string fragments are {@link SqlLiteral} objects, all of the same
 * type, collected as the operands of an {@link SqlCall} using this operator.
 * After validation, the fragments will be concatenated into a single literal.
 *
 * <p>For a chain of {@link org.eigenbase.sql.SqlCharStringLiteral} objects, a
 * {@link SqlCollation} object is attached only to the head of the chain.
 *
 * @author Marc Berkowitz
 * @since Sep 7, 2004
 * @version $Id$
 */
public class SqlLiteralChainOperator extends SqlInternalOperator {

    SqlLiteralChainOperator() {
        super("$LiteralChain", SqlKind.LiteralChain, 40, true,
            // precedence tighter than the * and || operators
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcVariadic);
    }

    // all operands must be the same type
    private boolean argTypesValid(
        SqlCallBinding callBinding)
    {
        if (callBinding.getOperandCount() < 2) {
            return true; // nothing to compare
        }
        SqlNode operand = callBinding.getCall().operands[0];
        RelDataType firstType = callBinding.getValidator().deriveType(
            callBinding.getScope(), operand);
        for (int i = 1; i < callBinding.getCall().operands.length; i++) {
            operand = callBinding.getCall().operands[i];
            RelDataType otherType =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(), operand);
            if (!SqlTypeUtil.sameNamedType(firstType, otherType)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        if (!argTypesValid(callBinding)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }


    // Result type is the same as all the args, but its size is the
    // total size.
    // REVIEW mb 8/8/04: Possibly this can be achieved by combining
    // the strategy useFirstArgType with a new transformer.
    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        // Here we know all the operands have the same type,
        // which has a size (precision), but not a scale.
        RelDataType ret = opBinding.getOperandType(0);
        SqlTypeName typeName = ret.getSqlTypeName();
        assert(typeName.allowsPrecNoScale()) :
            "LiteralChain has impossible operand type " + typeName;
        int size = 0;
        RelDataType[] types = opBinding.collectOperandTypes();
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            size += type.getPrecision();
            assert(type.getSqlTypeName().equals(typeName));
        }
        return opBinding.getTypeFactory().createSqlType(typeName, size);
    }

    public String getAllowedSignatures(String opName)
    {
        return opName + "(...)";
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        // per the SQL std, each string fragment must be on a different line
        for (int i = 1; i < call.operands.length; i++) {
            SqlParserPos prevPos = call.operands[i - 1].getParserPosition();
            final SqlNode operand = call.operands[i];
            SqlParserPos pos = operand.getParserPosition();
            if (pos.getLineNum() <= prevPos.getLineNum()) {
                throw validator.newValidationError(operand,
                    EigenbaseResource.instance().newStringFragsOnSameLine());
            }
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] rands,
        int leftPrec,
        int rightPrec)
    {
        SqlCollation collation = null;
        for (int i = 0; i < rands.length; i++) {
            if (i > 0) {
                writer.print(" ");
            }
            SqlLiteral rand = (SqlLiteral) rands[i];
            if (rand instanceof SqlCharStringLiteral) {
                NlsString nls =
                    ((SqlCharStringLiteral) rand).getNlsString();
                if (i == 0) {
                    collation = nls.getCollation();
                    writer.print(nls.asSql(true, false)); // print with prefix
                } else {
                    writer.print(nls.asSql(false, false)); // print without prefix
                }
            } else if (i == 0) {
                // print with prefix
                rand.unparse(writer, leftPrec, rightPrec);
            } else {
                // print without prefix
                writer.print("'");
                if (rand.getTypeName() == SqlTypeName.Binary) {
                    BitString bs = (BitString) rand.getValue();
                    writer.print(bs.toHexString());
                } else {
                    writer.print(rand.toValue());
                }
                writer.print("'");
            }
        }
        if (collation != null) {
            writer.print(" ");
            writer.print(collation.toString());
        }
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testLiteralChain(tester);
    }

}

// End SqlLiteralChainOperator.java
