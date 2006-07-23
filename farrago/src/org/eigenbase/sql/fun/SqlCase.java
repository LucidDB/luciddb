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

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * A <code>SqlCase</code> is a node of a parse tree which represents a case
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 *
 * @author wael
 * @version $Id$
 * @since Mar 14, 2004
 */
public class SqlCase
    extends SqlCall
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * WHEN_OPERANDS = 0
     */
    public static final int WHEN_OPERANDS = 0;

    /**
     * THEN_OPERANDS = 1
     */
    public static final int THEN_OPERANDS = 1;

    /**
     * ELSE_OPERAND = 2
     */
    public static final int ELSE_OPERAND = 2;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlCase expression.
     *
     * <p>The operands are an array of SqlNodes where
     *
     * <ul>
     * <li>operands[0] is a SqlNodeList of all WHEN expressions
     * <li>operands[1] is a SqlNodeList of all THEN expressions
     * <li>operands[2] is a SqlNode representing the implicit or explicit ELSE
     * expression
     * </ul>
     *
     * <p>See {@link #WHEN_OPERANDS}, {@link #THEN_OPERANDS}, {@link
     * #ELSE_OPERAND}.
     */
    SqlCase(
        SqlCaseOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        super(operator, operands, pos);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNodeList getWhenOperands()
    {
        return (SqlNodeList) operands[WHEN_OPERANDS];
    }

    public SqlNodeList getThenOperands()
    {
        return (SqlNodeList) operands[THEN_OPERANDS];
    }

    public SqlNode getElseOperand()
    {
        return operands[ELSE_OPERAND];
    }
}

// End SqlCase.java
