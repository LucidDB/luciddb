/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import org.eigenbase.sql.parser.*;


/**
 * A <code>SqlExplain</code> is a node of a parse tree which represents an
 * EXPLAIN PLAN statement.
 */
public class SqlExplain
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    // constants representing operand positions
    public static final int EXPLICANDUM_OPERAND = 0;
    public static final int DETAIL_LEVEL_OPERAND = 1;
    public static final int DEPTH_OPERAND = 2;
    public static final int AS_XML_OPERAND = 3;
    public static final int OPERAND_COUNT = 4;

    //~ Enums ------------------------------------------------------------------

    /**
     * The level of abstraction with which to display the plan.
     */
    public static enum Depth
        implements SqlLiteral.SqlSymbol
    {
        Type, Logical, Physical,
    }

    //~ Instance fields --------------------------------------------------------

    private final int nDynamicParams;

    //~ Constructors -----------------------------------------------------------

    public SqlExplain(
        SqlSpecialOperator operator,
        SqlNode explicandum,
        SqlLiteral detailLevel,
        SqlLiteral depth,
        SqlLiteral asXml,
        int nDynamicParams,
        SqlParserPos pos)
    {
        super(
            operator,
            new SqlNode[OPERAND_COUNT],
            pos);
        operands[EXPLICANDUM_OPERAND] = explicandum;
        operands[DETAIL_LEVEL_OPERAND] = detailLevel;
        operands[DEPTH_OPERAND] = depth;
        operands[AS_XML_OPERAND] = asXml;
        this.nDynamicParams = nDynamicParams;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying SQL statement to be explained
     */
    public SqlNode getExplicandum()
    {
        return operands[EXPLICANDUM_OPERAND];
    }

    /**
     * @return detail level to be generated
     */
    public SqlExplainLevel getDetailLevel()
    {
        return (SqlExplainLevel) SqlLiteral.symbolValue(
            operands[DETAIL_LEVEL_OPERAND]);
    }

    /**
     * Returns the level of abstraction at which this plan should be displayed.
     */
    public Depth getDepth()
    {
        return (Depth) SqlLiteral.symbolValue(operands[DEPTH_OPERAND]);
    }

    /**
     * @return the number of dynamic parameters in the statement
     */
    public int getDynamicParamCount()
    {
        return nDynamicParams;
    }

    /**
     * @return whether physical plan implementation should be returned
     */
    public boolean withImplementation()
    {
        return getDepth() == Depth.Physical;
    }

    /**
     * @return whether type should be returned
     */
    public boolean withType()
    {
        return getDepth() == Depth.Type;
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
        switch (getDetailLevel()) {
        case NO_ATTRIBUTES:
            writer.keyword("EXCLUDING ATTRIBUTES");
            break;
        case EXPPLAN_ATTRIBUTES:
            writer.keyword("INCLUDING ATTRIBUTES");
            break;
        case ALL_ATTRIBUTES:
            writer.keyword("INCLUDING ALL ATTRIBUTES");
            break;
        }
        switch (getDepth()) {
        case Type:
            writer.keyword("WITH TYPE");
            break;
        case Logical:
            writer.keyword("WITHOUT IMPLEMENTATION");
            break;
        case Physical:
            writer.keyword("WITH IMPLEMENTATION");
            break;
        default:
            throw new UnsupportedOperationException();
        }
        if (isXml()) {
            writer.keyword("AS XML");
        }
        writer.keyword("FOR");
        writer.newlineAndIndent();
        getExplicandum().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
    }
}

// End SqlExplain.java
