/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;


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
        super("CAST", SqlKind.Cast, null, UnknownParamInference.useFirstKnown,
            null, SqlFunction.SqlFuncTypeName.System);
    }

    //~ Methods ---------------------------------------------------------------

    // private SqlNode returnNode;

    /**
     * Creates a call to this operand with an array of operands.
     */

    /* public SqlCall createCall(SqlNode [] operands) {
    assert(operands.length == 2); // the parser should choke on more than 2 operands? test
    returnNode = operands[1];
    return super.createCall(new SqlNode [] {operands[0]});
    } */
    protected RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        assert(callOperands.size() == 2);
        RelDataType ret = callOperands.getType(1);
        RelDataType firstType = callOperands.getType(0);
        ret = typeFactory.createTypeWithNullability(ret, firstType.isNullable());
        if (null!=validator) {
            SqlCall call = (SqlCall) callOperands.getUnderlyingObject();
            validator.setValidatedNodeType(call.operands[0], ret);
        }
        return ret;
    }

    protected String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} AS {2})";
        }
        assert (false);
        return null;
    }

    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.Two;
    }

    protected void checkNumberOfArg(SqlCall call)
    {
        if (2 != call.operands.length) {
            throw Util.newInternal("todo: Wrong number of arguments to "
                + call);
        }
    }

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments
     * can override this method.
     */
    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        boolean throwOnFailure)
    {
        if (SqlUtil.isNullLiteral(call.operands[0], false)) {
            return true;
        }
        RelDataType validatedNodeType =
            validator.getValidatedNodeType(call.operands[0]);
        RelDataType returnType = validator.deriveType(scope, call.operands[1]);
        if (!SqlTypeUtil.canCastFrom(returnType, validatedNodeType, true)) {
            if (throwOnFailure) {
                throw EigenbaseResource.instance().newCannotCastValue(
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
        writer.print(name);
        writer.print('(');
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            if (i > 0) {
                writer.print(" AS ");
            }
            operand.unparse(writer, 0, 0);
        }
        writer.print(')');
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testCast(tester);
    }
}


// End SqlCastFunction.java
