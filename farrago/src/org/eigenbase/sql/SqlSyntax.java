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

import org.eigenbase.util.*;


/**
 * Enumeration of possible syntactic types of {@link SqlOperator operators}.
 *
 * @author jhyde
 * @version $Id$
 * @since June 28, 2004
 */
public enum SqlSyntax
{
    /**
     * Function syntax, as in "Foo(x, y)".
     */
    Function {
        public void unparse(
            SqlWriter writer,
            SqlOperator operator,
            SqlNode [] operands,
            int leftPrec,
            int rightPrec)
        {
            SqlUtil.unparseFunctionSyntax(
                operator,
                writer,
                operands,
                true,
                null);
        }
    },

    /**
     * Binary operator syntax, as in "x + y".
     */
    Binary {
        public void unparse(
            SqlWriter writer,
            SqlOperator operator,
            SqlNode [] operands,
            int leftPrec,
            int rightPrec)
        {
            SqlUtil.unparseBinarySyntax(
                operator,
                operands,
                writer,
                leftPrec,
                rightPrec);
        }
    },

    /**
     * Prefix unary operator syntax, as in "- x".
     */
    Prefix {
        public void unparse(
            SqlWriter writer,
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
    },

    /**
     * Postfix unary operator syntax, as in "x ++".
     */
    Postfix {
        public void unparse(
            SqlWriter writer,
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
    },

    /**
     * Special syntax, such as that of the SQL CASE operator, "CASE x WHEN 1
     * THEN 2 ELSE 3 END".
     */
    Special {
        public void unparse(
            SqlWriter writer,
            SqlOperator operator,
            SqlNode [] operands,
            int leftPrec,
            int rightPrec)
        {
            // You probably need to override the operator's unparse
            // method.
            throw Util.needToImplement(this);
        }
    },

    /**
     * Function syntax which takes no parentheses if there are no arguments, for
     * example "CURRENTTIME".
     */
    FunctionId {
        public void unparse(
            SqlWriter writer,
            SqlOperator operator,
            SqlNode [] operands,
            int leftPrec,
            int rightPrec)
        {
            SqlUtil.unparseFunctionSyntax(
                operator,
                writer,
                operands,
                false,
                null);
        }
    },

    /**
     * Syntax of an internal operator, which does not appear in the SQL.
     */
    Internal {
        public void unparse(
            SqlWriter writer,
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
