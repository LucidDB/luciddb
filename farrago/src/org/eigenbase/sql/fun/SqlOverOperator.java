/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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

import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.*;

/**
 * An operator describing a window function specification.
 *
 * <p>
 * Operands are as follows:
 *
 * <ul>
 * <li>
 * 0: name of window function ({@link org.eigenbase.sql.SqlCall})
 * </li>
 * <li>
 * 1: window name ({@link org.eigenbase.sql.SqlLiteral}) or window in-line specification (@link SqlWindowOperator})
 * </li>
 * <li>
 * </p>
 *
 * @author klo
 * @since Nov 4, 2004
 * @version $Id$
 **/
public class SqlOverOperator extends SqlOperator
{

    public SqlOverOperator()
    {
        super("over", SqlKind.WindowFun, 1, true, null, null, null);
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }


    public SqlCall createCall(
            SqlNode[] operands,
            ParserPosition pos)
    {
        return new SqlOver(this, operands, pos);
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec)
    {
        SqlCall windowFunction = (SqlCall) operands[SqlOver.WINDOW_FUNCITON_OPERAND];
        windowFunction.unparse(writer, 0, 0);

        writer.print(" OVER ");
        SqlNode windowSpecification = operands[SqlOver.WINDOW_SPEC_OPERARND];
        windowSpecification.unparse(writer, 0, 0);
    }

}

// End SqlOverOperator.java