/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.CallOperands;
import org.eigenbase.util.Util;


/**
 * SqlRowOperator represents the special ROW constructor.
 *
 * <p>TODO: describe usage
 * for row-value construction and row-type construction (SQL supports both).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlRowOperator extends SqlSpecialOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlRowOperator()
    {
        // Precedence of 100 because nothing can pull parentheses apart.
        super("ROW", SqlKind.Row, 100, false, null,
            UnknownParamInference.useReturnType, null);
    }

    //~ Methods ---------------------------------------------------------------

    // implement SqlOperator
    public SqlSyntax getSyntax()
    {
        // Function syntax would work too.
        return SqlSyntax.Special;
    }

    // implement SqlOperator
    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.variadicCountDescriptor;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        RelDataType[] argTypes = callOperands.collectTypes();
        final String [] fieldNames = new String[argTypes.length];
        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = SqlValidator.deriveAliasFromOrdinal(i);
        }
        return typeFactory.createStructType(argTypes, fieldNames);
    }


    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        boolean throwOnFailure)
    {
        // any arguments are fine
        Util.discard(call);
        Util.discard(validator);
        Util.discard(scope);
        Util.discard(throwOnFailure);
        return true;
    }

    protected void checkNumberOfArg(SqlCall call)
    {
        // any number of arguments is fine
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testRow();
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        SqlUtil.unparseFunctionSyntax(this, writer, operands, true);
    }
}


// End SqlRowOperator.java
