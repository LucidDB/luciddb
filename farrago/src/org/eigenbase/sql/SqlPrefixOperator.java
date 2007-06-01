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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * A unary operator.
 */
public class SqlPrefixOperator
    extends SqlOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlPrefixOperator(
        String name,
        SqlKind kind,
        int prec,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker)
    {
        super(
            name,
            kind,
            leftPrec(0, true),
            rightPrec(prec, true),
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Prefix;
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        Util.discard(operandsCount);
        return "{0}{1}";
    }

    protected RelDataType adjustType(
        SqlValidator validator,
        SqlCall call,
        RelDataType type)
    {
        if (SqlTypeUtil.inCharFamily(type)) {
            // Determine coercibility and resulting collation name of
            // unary operator if needed.
            RelDataType operandType =
                validator.getValidatedNodeType(call.operands[0]);
            if (null == operandType) {
                throw Util.newInternal(
                    "operand's type should have been derived");
            }
            if (SqlTypeUtil.inCharFamily(operandType)) {
                SqlCollation collation = operandType.getCollation();
                assert null != collation : "An implicit or explicit collation should have been set";
                type =
                    validator.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        type,
                        type.getCharset(),
                        new SqlCollation(
                            collation.getCollationName(),
                            collation.getCoercibility()));
            }
        }
        return type;
    }
}

// End SqlPrefixOperator.java
