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

import net.sf.saffron.sql.type.SqlTypeName;

/**
 * <code>SqlBinaryOperator</code> is a binary operator.
 */
public class SqlBinaryOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    SqlBinaryOperator(
        String name,SqlKind kind,int prec,boolean isLeftAssoc,
        SqlOperator.TypeInference typeInference,
        SqlOperator.ParamTypeInference paramTypeInference,
        SqlOperator.AllowdArgInference argTypes)
    {
        super(
            name,
            kind,
            (2 * prec) + (isLeftAssoc ? 0 : 1),
            (2 * prec) + (isLeftAssoc ? 1 : 0),
            typeInference,
            paramTypeInference,
            argTypes);
    }

    //~ Methods ---------------------------------------------------------------

    public int getSyntax()
    {
        return Syntax.Binary;
    }

    protected String getSignatureTemplate() {
        //op0 opname op1
        return "{1} {0} {2}";
    }

    boolean needsSpace()
    {
        return !name.equals(".");
    }

    void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert(operands.length == 2);
        operands[0].unparse(writer,leftPrec,this.leftPrec);
        if (needsSpace()) {
            writer.print(' ');
            writer.print(name);
            writer.print(' ');
        } else {
            writer.print(name);
        }
        operands[1].unparse(writer,this.rightPrec,rightPrec);
    }
}


// End SqlBinaryOperator.java
