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

import org.eigenbase.util.*;


/**
 * Enumeration of possible syntactic types of {@link SqlOperator operators}.
 *
 * @author jhyde
 * @version $Id$
 * @since June 28, 2004
 */
public abstract class SqlSyntax
    extends EnumeratedValues.BasicValue
{

    //~ Static fields/initializers ---------------------------------------------

    public static final int Function_ordinal = 0;

    /**
     * Function syntax, as in "Foo(x, y)".
     */
    public static final SqlSyntax Function =
        new SqlSyntax("Function", Function_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                SqlUtil.unparseFunctionSyntax(operator,
                    writer,
                    operands,
                    true,
                    null);
            }
        };
    public static final int Binary_ordinal = 1;

    /**
     * Binary operator syntax, as in "x + y".
     */
    public static final SqlSyntax Binary =
        new SqlSyntax("Binary", Binary_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                SqlUtil.unparseBinarySyntax(operator,
                    operands,
                    writer,
                    leftPrec,
                    rightPrec);
            }
        };
    public static final int Prefix_ordinal = 2;

    /**
     * Prefix unary operator syntax, as in "- x".
     */
    public static final SqlSyntax Prefix =
        new SqlSyntax("Prefix", Prefix_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                assert (operands.length == 1);
                writer.keyword(operator.getName());
                operands[0].unparse(
                    writer,
                    operator.getLeftPrec(),
                    operator.getRightPrec());
            }
        };
    public static final int Postfix_ordinal = 3;

    /**
     * Postfix unary operator syntax, as in "x ++".
     */
    public static final SqlSyntax Postfix =
        new SqlSyntax("Postfix", Postfix_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                assert (operands.length == 1);
                operands[0].unparse(
                    writer,
                    operator.getLeftPrec(),
                    operator.getRightPrec());
                writer.keyword(operator.getName());
            }
        };
    public static final int Special_ordinal = 4;

    /**
     * Special syntax, such as that of the SQL CASE operator, "CASE x WHEN 1
     * THEN 2 ELSE 3 END".
     */
    public static final SqlSyntax Special =
        new SqlSyntax("Special", Special_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                // You probably need to override the operator's unparse
                // method.
                throw Util.needToImplement(this);
            }
        };
    public static final int FunctionId_ordinal = 5;

    /**
     * Function syntax which takes no parentheses if there are no arguments, for
     * example "CURRENTTIME".
     */
    public static final SqlSyntax FunctionId =
        new SqlSyntax("FunctionId", FunctionId_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                SqlUtil.unparseFunctionSyntax(operator,
                    writer,
                    operands,
                    false,
                    null);
            }
        };
    public static final int Internal_ordinal = 6;

    /**
     * Syntax of an internal operator, which does not appear in the SQL.
     */
    public static final SqlSyntax Internal =
        new SqlSyntax("Internal", Internal_ordinal) {
            public void unparse(SqlWriter writer,
                SqlOperator operator,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                throw Util.newInternal(
                    "Internal operator '" + operator
                    + "' cannot be un-parsed");
            }
        };
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(
            new SqlSyntax[] {
                Function, Binary, Prefix, Postfix, Special, FunctionId, Internal
            });

    //~ Constructors -----------------------------------------------------------

    private SqlSyntax(
        String name,
        int ordinal)
    {
        super(name, ordinal, null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up a syntax from its ordinal.
     */
    public static SqlSyntax get(int ordinal)
    {
        return (SqlSyntax) enumeration.getValue(ordinal);
    }

    /**
     * Looks up a syntax from its name.
     */
    public static SqlSyntax get(String name)
    {
        return (SqlSyntax) enumeration.getValue(name);
    }

    /**
     * Converts a call to an operator of this syntax into a string.
     */
    public abstract void unparse(
        SqlWriter writer,
        SqlOperator operator,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec);
}

// End SqlSyntax.java
