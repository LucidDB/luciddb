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

package org.eigenbase.sql;

import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.fun.SqlOverOperator;

/**
 * Sees {@link SqlWindow} for detail
 *
 * @author klo
 * @since Nov 5, 2004
 * @version $Id$
 **/
public class SqlOver extends SqlCall
{
    public static final int WINDOW_FUNCITON_OPERAND = 0;
    public static final int WINDOW_SPEC_OPERARND = 1;

    public SqlOver(SqlOverOperator op, SqlNode[] operands, ParserPosition pos)
    {
        super(op, operands, pos);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        // Override, so we don't print extra parentheses.
        operator.unparse(writer, operands, 0, 0);
    }

}

// End SqlOver.java