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
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
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
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcVariadic);
    }

    //~ Methods ---------------------------------------------------------------

    // implement SqlOperator
    public SqlSyntax getSyntax()
    {
        // Function syntax would work too.
        return SqlSyntax.Special;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        RelDataType[] argTypes = callOperands.collectTypes();
        final String [] fieldNames = new String[argTypes.length];
        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = SqlUtil.deriveAliasFromOrdinal(i);
        }
        return typeFactory.createStructType(argTypes, fieldNames);
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
