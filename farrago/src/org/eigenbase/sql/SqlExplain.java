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

import org.eigenbase.sql.parser.SqlParserPos;


/**
 * A <code>SqlExplain</code> is a node of a parse tree which represents
 * an EXPLAIN PLAN statement.
 */
public class SqlExplain extends SqlCall
{
    //~ Static fields/initializers --------------------------------------------

    // constants representing operand positions
    public static final int EXPLICANDUM_OPERAND = 0;
    public static final int WITH_IMPLEMENTATION_OPERAND = 1;
    private static final int AS_XML_OPERAND = 2;
    public static final int OPERAND_COUNT = 3;

    //~ Constructors ----------------------------------------------------------

    public SqlExplain(
        SqlSpecialOperator operator,
        SqlNode explicandum,
        SqlLiteral withImplementation,
        SqlLiteral asXml,
        SqlParserPos pos)
    {
        super(operator, new SqlNode[OPERAND_COUNT], pos);
        operands[EXPLICANDUM_OPERAND] = explicandum;
        operands[WITH_IMPLEMENTATION_OPERAND] = withImplementation;
        operands[AS_XML_OPERAND] = asXml;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the underlying SQL statement to be explained
     */
    public SqlNode getExplicandum()
    {
        return operands[EXPLICANDUM_OPERAND];
    }

    /**
     * Gets the source SELECT expression for the data to be inserted.  It is
     * only safe to call this after non-SELECT source expressions (e.g. VALUES)
     * have been expanded by SqlValidator.performUnconditionalRewrites.
     *
     * @return the source SELECT for the data to be inserted
     */
    public boolean withImplementation()
    {
        return SqlLiteral.booleanValue(operands[WITH_IMPLEMENTATION_OPERAND]);
    }

    /**
     * Returns whether result is to be in XML format.
     */
    public boolean isXml()
    {
        return SqlLiteral.booleanValue(operands[AS_XML_OPERAND]);
    }

    // implement SqlNode
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.keyword("EXPLAIN PLAN");
        if (!withImplementation()) {
            writer.keyword("WITHOUT IMPLEMENTATION");
        }
        if (isXml()) {
            writer.keyword("AS XML");
        }
        writer.keyword("FOR");
        writer.newlineAndIndent();
        getExplicandum().unparse(
            writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
    }

}


// End SqlExplain.java
