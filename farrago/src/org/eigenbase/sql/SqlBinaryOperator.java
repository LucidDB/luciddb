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
package org.eigenbase.sql;

import java.nio.charset.*;
import java.math.BigDecimal;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * <code>SqlBinaryOperator</code> is a binary operator.
 */
public class SqlBinaryOperator
    extends SqlOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlBinaryOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean leftAssoc,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker)
    {
        super(
            name,
            kind,
            leftPrec(prec, leftAssoc),
            rightPrec(prec, leftAssoc),
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Binary;
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        Util.discard(operandsCount);

        //op0 opname op1
        return "{1} {0} {2}";
    }

    boolean needsSpace()
    {
        return !getName().equals(".");
    }

    protected RelDataType adjustType(
        SqlValidator validator,
        final SqlCall call,
        RelDataType type)
    {
        RelDataType operandType1 =
            validator.getValidatedNodeType(call.operands[0]);
        RelDataType operandType2 =
            validator.getValidatedNodeType(call.operands[1]);
        if (SqlTypeUtil.inCharFamily(operandType1)
            && SqlTypeUtil.inCharFamily(operandType2))
        {
            Charset cs1 = operandType1.getCharset();
            Charset cs2 = operandType2.getCharset();
            assert ((null != cs1) && (null != cs2)) : "An implicit or explicit charset should have been set";
            if (!cs1.equals(cs2)) {
                throw EigenbaseResource.instance().IncompatibleCharset.ex(
                    getName(),
                    cs1.name(),
                    cs2.name());
            }

            SqlCollation col1 = operandType1.getCollation();
            SqlCollation col2 = operandType2.getCollation();
            assert ((null != col1) && (null != col2)) : "An implicit or explicit collation should have been set";

            //validation will occur inside getCoercibilityDyadicOperator...
            SqlCollation resultCol =
                SqlCollation.getCoercibilityDyadicOperator(col1,
                    col2);

            if (SqlTypeUtil.inCharFamily(type)) {
                type =
                    validator.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        type,
                        type.getCharset(),
                        resultCol);
            }
        }
        return type;
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        RelDataType type = super.deriveType(validator, scope, call);

        RelDataType operandType1 =
            validator.getValidatedNodeType(call.operands[0]);
        RelDataType operandType2 =
            validator.getValidatedNodeType(call.operands[1]);
        if (SqlTypeUtil.inCharFamily(operandType1)
            && SqlTypeUtil.inCharFamily(operandType2))
        {
            Charset cs1 = operandType1.getCharset();
            Charset cs2 = operandType2.getCharset();
            assert ((null != cs1) && (null != cs2)) : "An implicit or explicit charset should have been set";
            if (!cs1.equals(cs2)) {
                throw EigenbaseResource.instance().IncompatibleCharset.ex(
                    getName(),
                    cs1.name(),
                    cs2.name());
            }

            SqlCollation col1 = operandType1.getCollation();
            SqlCollation col2 = operandType2.getCollation();
            assert ((null != col1) && (null != col2)) : "An implicit or explicit collation should have been set";

            //validation will occur inside getCoercibilityDyadicOperator...
            SqlCollation resultCol =
                SqlCollation.getCoercibilityDyadicOperator(col1,
                    col2);

            if (SqlTypeUtil.inCharFamily(type)) {
                type =
                    validator.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        type,
                        type.getCharset(),
                        resultCol);
            }
        }
        return type;
    }


    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        if (getName().equals("/")) {
            final SqlNode operand0 = call.getOperands()[0];
            final SqlNode operand1 = call.getOperands()[1];
            final SqlMonotonicity mono0 =
                operand0.getMonotonicity(scope);
            final SqlMonotonicity mono1 =
                operand1.getMonotonicity(scope);
            if (mono1 == SqlMonotonicity.Constant) {
                if (operand1 instanceof SqlLiteral) {
                    SqlLiteral literal = (SqlLiteral) operand1;
                    switch (literal.bigDecimalValue().compareTo(
                        BigDecimal.ZERO))
                    {
                    case -1:
                        // mono / -ve constant --> reverse mono, unstrict
                        return mono0.reverse().unstrict();
                    case 0:
                        // mono / zero --> constant (infinity!)
                        return SqlMonotonicity.Constant;
                    default:
                        // mono / +ve constant * mono1 --> mono, unstrict
                        return mono0.unstrict();
                    }
                }
            }
        }

        return super.getMonotonicity(call, scope);
    }
}

// End SqlBinaryOperator.java
