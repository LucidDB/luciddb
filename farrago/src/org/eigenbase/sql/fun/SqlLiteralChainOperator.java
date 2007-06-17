/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
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
package org.eigenbase.sql.fun;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Internal operator, by which the parser represents a continued string literal.
 *
 * <p>The string fragments are {@link SqlLiteral} objects, all of the same type,
 * collected as the operands of an {@link SqlCall} using this operator. After
 * validation, the fragments will be concatenated into a single literal.
 *
 * <p>For a chain of {@link org.eigenbase.sql.SqlCharStringLiteral} objects, a
 * {@link SqlCollation} object is attached only to the head of the chain.
 *
 * @author Marc Berkowitz
 * @version $Id$
 * @since Sep 7, 2004
 */
public class SqlLiteralChainOperator
    extends SqlInternalOperator
{
    //~ Constructors -----------------------------------------------------------

    SqlLiteralChainOperator()
    {
        super(
            "$LiteralChain",
            SqlKind.LiteralChain,
            80,
            true,

            // precedence tighter than the * and || operators
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcVariadic);
    }

    //~ Methods ----------------------------------------------------------------

    // all operands must be the same type
    private boolean argTypesValid(
        SqlCallBinding callBinding)
    {
        if (callBinding.getOperandCount() < 2) {
            return true; // nothing to compare
        }
        SqlNode operand = callBinding.getCall().operands[0];
        RelDataType firstType =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                operand);
        for (int i = 1; i < callBinding.getCall().operands.length; i++) {
            operand = callBinding.getCall().operands[i];
            RelDataType otherType =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(),
                    operand);
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
        assert (typeName.allowsPrecNoScale()) : "LiteralChain has impossible operand type "
            + typeName;
        int size = 0;
        RelDataType [] types = opBinding.collectOperandTypes();
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            size += type.getPrecision();
            assert (type.getSqlTypeName().equals(typeName));
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
                throw validator.newValidationError(
                    operand,
                    EigenbaseResource.instance().StringFragsOnSameLine.ex());
            }
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] rands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startList("", "");
        SqlCollation collation = null;
        for (int i = 0; i < rands.length; i++) {
            SqlLiteral rand = (SqlLiteral) rands[i];
            if (i > 0) {
                // SQL:2003 says there must be a newline between string
                // fragments.
                writer.newlineAndIndent();
            }
            if (rand instanceof SqlCharStringLiteral) {
                NlsString nls = ((SqlCharStringLiteral) rand).getNlsString();
                if (i == 0) {
                    collation = nls.getCollation();

                    // print with prefix
                    writer.literal(nls.asSql(true, false));
                } else {
                    // print without prefix
                    writer.literal(nls.asSql(false, false));
                }
            } else if (i == 0) {
                // print with prefix
                rand.unparse(writer, leftPrec, rightPrec);
            } else {
                // print without prefix
                if (rand.getTypeName() == SqlTypeName.BINARY) {
                    BitString bs = (BitString) rand.getValue();
                    writer.literal("'" + bs.toHexString() + "'");
                } else {
                    writer.literal("'" + rand.toValue() + "'");
                }
            }
        }
        if (collation != null) {
            collation.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    /**
     * Concatenates the operands of a call to this operator.
     */
    public static SqlLiteral concatenateOperands(SqlCall call)
    {
        assert call.operands.length > 0;
        assert call.operands[0] instanceof SqlLiteral : call.operands[0]
            .getClass();
        SqlLiteral [] fragments =
            (SqlLiteral []) Arrays.asList(call.operands).toArray(
                new SqlLiteral[call.operands.length]);
        SqlLiteral sum = SqlUtil.concatenateLiterals(fragments);
        return sum;
    }
}

// End SqlLiteralChainOperator.java
