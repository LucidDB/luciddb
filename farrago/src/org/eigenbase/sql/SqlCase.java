/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package org.eigenbase.sql;

import org.eigenbase.sql.parser.ParserPosition;

import java.util.List;

/**
 * A <code>SqlCase</code> is a node of a parse tree which represents a
 * case statement. It warrants its own node type just because we have a lot
 * of methods to put somewhere.

 * @author wael
 * @since Mar 14, 2004
 * @version $Id$
 **/


public class SqlCase extends SqlCall
{
    //~ Static fields/initializers --------------------------------------------

    /** WHEN_OPERANDS = 0 */
    public static final int WHEN_OPERANDS = 0;
    /** THEN_OPERANDS = 1 */
    public static final int THEN_OPERANDS = 1;
    /** ELSE_OPERAND = 2 */
    public static final int ELSE_OPERAND = 2;

    //~ Constructors ----------------------------------------------------------

    /**
     * @param operator
     * @param operands Must be an array of SqlNodes where
     * operands[0] is a SqlNodeList of all when expressions
     * operands[1] is a SqlNodeList of all then expressions
     * operands[2] is a SqlNode representing the implicit or explicit ELSE expression
     * see {@link #WHEN_OPERANDS}, {@link #THEN_OPERANDS}, {@link #ELSE_OPERAND}
     */
    SqlCase(SqlCaseOperator operator,SqlNode [] operands, ParserPosition parserPosition)
    {
        super(operator,operands, parserPosition);
    }

    //~ Methods ---------------------------------------------------------------

    public List getWhenOperands() {
        return ((SqlNodeList) operands[WHEN_OPERANDS]).getList();
    }

    public List getThenOperands() {
        return ((SqlNodeList) operands[THEN_OPERANDS]).getList();
    }

    public SqlNode getElseOperand() {
        return (SqlNode) operands[ELSE_OPERAND];
    }

    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        writer.print("(CASE");
        List whenList = getWhenOperands();
        List thenList = getThenOperands();
        assert(whenList.size()==thenList.size());
        for (int i = 0; i < whenList.size(); i++) {
            writer.print(" WHEN ");
            SqlNode e = (SqlNode) whenList.get(i);
            e.unparse(writer,leftPrec,rightPrec);
            writer.print(" THEN ");
            e = (SqlNode) thenList.get(i);
            e.unparse(writer,leftPrec,rightPrec);
        }

       writer.print(" ELSE ");
       getElseOperand().unparse(writer,leftPrec,rightPrec);
       writer.print(" END)");
    }
}


// End SqlCase.java
