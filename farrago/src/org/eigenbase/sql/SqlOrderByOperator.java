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

/**
 * SqlOrderByOperator is used to represent an ORDER BY on a query
 * other than a SELECT (e.g. VALUES or UNION).  It is a purely syntactic
 * operator, and is eliminated by SqlValidator.performUnconditionalRewrites
 * and replaced with the ORDER_OPERAND of SqlSelect.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlOrderByOperator extends SqlSpecialOperator
{
    //~ Static fields/initializers --------------------------------------------

    // constants representing operand positions
    public static final int QUERY_OPERAND = 0;
    public static final int ORDER_OPERAND = 1;

    //~ Constructors ----------------------------------------------------------

    public SqlOrderByOperator()
    {
        // NOTE:  make precedence lower then SELECT to avoid extra parens
        super("ORDER BY", SqlKind.OrderBy, 0);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Postfix;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert (operands.length == 2);
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameType.OrderBy);
        operands[QUERY_OPERAND].unparse(writer, getLeftPrec(), getRightPrec());
        writer.sep(getName());
        final SqlWriter.Frame listFrame =
            writer.startList(SqlWriter.FrameType.OrderByList);
        operands[ORDER_OPERAND].unparse(writer, 0, 0);
        writer.endList(listFrame);
        writer.endList(frame);
    }
}

// End SqlOrderByOperator.java
