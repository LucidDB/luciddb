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
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeStrategies;
import org.eigenbase.sql.type.SqlTypeUtil;


/**
 * SqlCastFunction.  Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable.  So you can't
 * story any local info in the class and hence the return type data is maintained
 * in operand[1] through the validation phase.
 *
 * @author lee
 * @since Jun 5, 2004
 * @version $Id$
 **/
public class SqlCastFunction extends SqlFunction
{
    //~ Constructors ----------------------------------------------------------

    public SqlCastFunction()
    {
        super("CAST", SqlKind.Cast, null,
            SqlTypeStrategies.otiFirstKnown,
            null, SqlFunctionCategory.System);
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        assert(opBinding.getOperandCount() == 2);
        RelDataType ret = opBinding.getOperandType(1);
        RelDataType firstType = opBinding.getOperandType(0);
        ret = opBinding.getTypeFactory().createTypeWithNullability(
            ret, firstType.isNullable());
        if (opBinding instanceof SqlCallBinding) {
            SqlCallBinding callBinding = (SqlCallBinding) opBinding;
            callBinding.getValidator().setValidatedNodeType(
                callBinding.getCall().operands[0], ret);
        }
        return ret;
    }

    public String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} AS {2})";
        }
        assert (false);
        return null;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Two;
    }

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments
     * can override this method.
     */
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        if (SqlUtil.isNullLiteral(callBinding.getCall().operands[0], false)) {
            return true;
        }
        RelDataType validatedNodeType =
            callBinding.getValidator().getValidatedNodeType(
                callBinding.getCall().operands[0]);
        RelDataType returnType =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                callBinding.getCall().operands[1]);
        if (!SqlTypeUtil.canCastFrom(returnType, validatedNodeType, true)) {
            if (throwOnFailure) {
                throw EigenbaseResource.instance().CannotCastValue.ex(
                    validatedNodeType.toString(),
                    returnType.toString());
            }
            return false;
        }
        return true;
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert operands.length == 2;
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, 0, 0);
        writer.sep("AS");
        if (operands[1] instanceof SqlIntervalQualifier) {
            writer.sep("INTERVAL");
        }
        operands[1].unparse(writer, 0, 0);
        writer.endFunCall(frame);
    }
}

// End SqlCastFunction.java
