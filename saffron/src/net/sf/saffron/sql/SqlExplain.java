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

import net.sf.saffron.util.Util;


/**
 * A <code>SqlExplain</code> is a node of a parse tree which represents
 * an EXPLAIN PLAN statement.
 */
public class SqlExplain extends SqlCall
{

    // constants representing operand positions
    public static final int EXPLICANDUM_OPERAND = 0;
    public static final int WITH_IMPLEMENTATION_OPERAND = 1;
    public static final int OPERAND_COUNT = 2;
    
    //~ Constructors ----------------------------------------------------------

    public SqlExplain(
        SqlSpecialOperator operator,
        SqlNode explicandum,
        SqlLiteral withImplementation)
    {
        super(operator,new SqlNode[OPERAND_COUNT]);
        operands[EXPLICANDUM_OPERAND] = explicandum;
        operands[WITH_IMPLEMENTATION_OPERAND] = withImplementation;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return the underlying SQL statement to be explained
     */
    public SqlNode getExplicandum()
    {
        return operands[EXPLICANDUM_OPERAND];
    }

    /**
     * Get the source SELECT expression for the data to be inserted.  It is
     * only safe to call this after non-SELECT source expressions (e.g. VALUES)
     * have been expanded by SqlValidator.createInternalSelect
     *
     * @return the source SELECT for the data to be inserted
     */
    public boolean withImplementation()
    {
        return SqlLiteral.booleanValue(operands[WITH_IMPLEMENTATION_OPERAND]);
    }

    // implement SqlNode
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print("EXPLAIN PLAN ");
        if (!withImplementation()) {
            writer.print("WITHOUT IMPLEMENTATION ");
        }
        writer.print("FOR");
        writer.println();
        getExplicandum().unparse(writer,operator.leftPrec,operator.rightPrec);
    }
}

// End SqlExplain.java
