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

package net.sf.saffron.sql;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in
 * practice, every non-leaf node in a SQL parse tree is a
 * <code>SqlCall</code> of some kind.)
 */
public class SqlCall extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    public SqlOperator operator;
    SqlNode [] operands;

    //~ Constructors ----------------------------------------------------------

    SqlCall(SqlOperator operator,SqlNode [] operands)
    {
        this.operator = operator;
        this.operands = operands;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isA(SqlKind kind)
    {
        return operator.isA(kind);
    }

    public SqlKind getKind()
    {
        return operator.kind;
    }

    // REVIEW jvs 10-Sept-2003:  I added this to allow for some rewrite by
    // SqlValidator.  Is mutability OK?
    public void setOperand(int i,SqlNode operand)
    {
        operands[i] = operand;
    }

    public SqlNode [] getOperands()
    {
        return operands;
    }

    public void addOperand(SqlNode operand)
    {
        SqlNode [] oldOperands = operands;
        operands = new SqlNode[operands.length + 1];
        System.arraycopy(oldOperands,0,operands,0,oldOperands.length);
        operands[operands.length - 1] = operand;
    }

    public Object clone()
    {
        return operator.createCall(SqlNode.cloneArray(operands));
    }

    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        if (
            (leftPrec > operator.leftPrec)
                || (operator.rightPrec <= rightPrec)
                || (writer.alwaysUseParentheses
                && isA(SqlKind.Expression))) {
            writer.print('(');
            operator.unparse(writer,operands,0,0);
            writer.print(')');
        } else {
            operator.unparse(writer,operands,leftPrec,rightPrec);
        }
    }
}


// End SqlCall.java
