/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.UnknownParamInference;
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
        super("$LitChain", SqlKind.LitChain, 40, true,
            // precedence tighter than the * and || operators
            ReturnTypeInference.useFirstArgType, UnknownParamInference.useFirstKnown, null);
    }

    // REVIEW mb 8/8/04: Can't use SqlOperator.OperandsTypeChecking here;
    // it doesn't handle variadicCountDescriptor operators well.
    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.variadicCountDescriptor;
    }

    // all operands must be the same type
    private boolean argTypesValid(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        if (call.operands.length < 2) {
            return true; // nothing to compare
        }
        SqlNode operand = call.operands[0];
        RelDataType firstType = validator.deriveType(scope, operand);
        for (int i = 1; i < call.operands.length; i++) {
            operand = call.operands[i];
            RelDataType otherType =
                validator.deriveType(scope, operand);
            if (!firstType.isSameType(otherType)) {
                return false;
            }
        }
        return true;
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        boolean throwOnFailure)
    {
        if (!argTypesValid(call, validator, scope)) {
            if (throwOnFailure) {
                throw call.newValidationSignatureError(validator, scope);
            }
            return false;
        }
        return true;
    }

    // Result type is the same as all the args, but its size is the
    // total size.
    // REVIEW mb 8/8/04: Possibly this can be achieved by combining
    // the strategy useFirstArgType with a new transformer.
    protected RelDataType inferType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        // Here we know all the operands have the same type,
        // which has a size (precision), but not a scale.
        RelDataType rt =
            validator.getValidatedNodeType(call.operands[0]);
        SqlTypeName tname = rt.getSqlTypeName();
        assert tname.allowsPrecNoScale() :
            "LitChain has impossible operand type " + tname;
        int size = rt.getPrecision();
        for (int i = 1; i < call.operands.length; i++) {
            rt = validator.getValidatedNodeType(call.operands[i]);
            size += rt.getPrecision();
        }
        return validator.typeFactory.createSqlType(tname, size);
    }

    public String getAllowedSignatures(String opName)
    {
        return opName + "(...)";
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        // per the SQL std, each string fragment must be on a different line
        for (int i = 1; i < call.operands.length; i++) {
            ParserPosition prevPos = call.operands[i - 1].getParserPosition();
            final SqlNode operand = call.operands[i];
            ParserPosition pos = operand.getParserPosition();
            if (pos.getBeginLine() <= prevPos.getBeginLine()) {
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
