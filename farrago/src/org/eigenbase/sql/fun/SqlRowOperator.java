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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.test.*;


/**
 * SqlRowOperator represents the special ROW constructor.  TODO: describe usage
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
            SqlOperatorTable.useReturnForParam, null);
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

    // implement SqlOperator
    public RelDataType getType(
        RelDataTypeFactory typeFactory,
        RelDataType [] argTypes)
    {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        final String [] fieldNames = new String[argTypes.length];
        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = SqlValidator.deriveAliasFromOrdinal(i);
        }
        return typeFactory.createProjectType(argTypes, fieldNames);
    }

    // implement SqlOperator
    protected RelDataType inferType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        final RelDataType [] types = new RelDataType[call.operands.length];
        for (int i = 0; i < call.operands.length; i++) {
            SqlNode operand = call.operands[i];
            types[i] = validator.deriveType(scope, operand);
        }
        return getType(validator.typeFactory, types);
    }

    protected void checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        // any arguments are fine
    }

    protected void checkNumberOfArg(SqlCall call)
    {
        // any number of arguments is fine
    }

    public void test(SqlTester tester)
    {
        /* empty implementation */
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        SqlFunction.unparseFunctionSyntax(this, writer, operands);
    }
}


// End SqlRowOperator.java
