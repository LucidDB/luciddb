/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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
 * A unary operator.
 */
public abstract class SqlPrefixOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlPrefixOperator(
        String name,SqlKind kind,int precedence,
        TypeInference typeInference,
        ParamTypeInference paramTypeInference,
        AllowedArgInference argInference)
    {
        super(name, kind, 0, precedence * 2, typeInference, paramTypeInference, argInference);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Prefix;
    }

    protected String getSignatureTemplate() {
        return "{0}{1}";
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert(operands.length == 1);
        writer.print(name);
        writer.print(' ');
        operands[0].unparse(writer,this.leftPrec,this.rightPrec);
    }
}


// End SqlPrefixOperator.java
