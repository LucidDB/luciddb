/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.sql.fun;

import net.sf.saffron.core.*;
import net.sf.saffron.sql.test.*;
import net.sf.saffron.sql.*;

/**
 * SqlRowOperator represents the special ROW constructor.  TODO: describe usage
 * for row-value construction and row-type construction (SQL supports both).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlRowOperator extends SqlSpecialOperator
{
    public SqlRowOperator()
    {
        // Precedence of 100 because nothing can pull parentheses apart.
        super(
            "ROW",
            SqlKind.Row,
            100,
            false,
            null,
            SqlOperatorTable.useReturnForParam,
            null);
    }

    // implement SqlOperator
    public int getSyntax()
    {
        // Function syntax would work too.
        return SqlOperator.Syntax.Special;
    }

    // implement SqlOperator
    public int getNumOfOperands(int desiredCount)
    {
        // any number of arguments is fine
        return desiredCount;
    }

    // implement SqlOperator
    public SaffronType getType(
        SaffronTypeFactory typeFactory, SaffronType[] argTypes)
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
    protected SaffronType inferType(
        SqlValidator validator,
        SqlValidator.Scope scope, SqlCall call)
    {
        final SaffronType [] types = new SaffronType[call.operands.length];
        for (int i = 0; i < call.operands.length; i++) {
            SqlNode operand = call.operands[i];
            types[i] = validator.deriveType(scope,operand);
        }
        return getType(validator.typeFactory,types);
    }

    protected void checkArgTypes(
        SqlCall call, SqlValidator validator, SqlValidator.Scope scope)
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
        SqlFunction.unparseFunctionSyntax(this,writer,operands);
    }
}

// End SqlRowOperator.java
