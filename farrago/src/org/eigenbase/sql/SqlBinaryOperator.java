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
package org.eigenbase.sql;

import java.nio.charset.*;

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
            && SqlTypeUtil.inCharFamily(operandType2)) {
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
            && SqlTypeUtil.inCharFamily(operandType2)) {
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
        return
            type; //To change body of overridden methods use File | Settings |
                  //File Templates.
    }
}

// End SqlBinaryOperator.java
