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

package org.eigenbase.sql.parser;

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.SqlNode;


/**
 * SqlParserPos represents the position of a parsed token within SQL statement
 * text.
 *
 * @author Kinkoi Lo
 * @since Jun 1, 2004
 * @version $Id$
 **/
public class SqlParserPos
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * SqlParserPos representing line one, character one. Use this if the
     * node doesn't correspond to a position in piece of SQL text.
     */
    public static final SqlParserPos ZERO = new SqlParserPos(0, 0);

    //~ Instance fields -------------------------------------------------------

    private int lineNumber;
    private int columnNumber;
    private int endLineNumber;
    private int endColumnNumber;

    //~ Constructors ----------------------------------------------------------

    /**
    * Creates a new parser position.
    */
    public SqlParserPos(
        int lineNumber,
        int columnNumber)
    {
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
        this.endLineNumber = lineNumber;
        this.endColumnNumber = columnNumber;
    }
    
    /**
    * Creates a new parser range.
    */
    public SqlParserPos(
        int startLineNumber,
        int startColumnNumber,
        int endLineNumber,
        int endColumnNumber)
    {
        this.lineNumber = startLineNumber;
        this.columnNumber = startColumnNumber;
        this.endLineNumber = endLineNumber;
        this.endColumnNumber = endColumnNumber;
    }


    //~ Methods ---------------------------------------------------------------

    /**
     * @return 1-based starting line number
     */
    public int getLineNum()
    {
        return lineNumber;
    }

    /**
     * @return 1-based starting column number
     */
    public int getColumnNum()
    {
        return columnNumber;
    }

    /**
     * @return 1-based end line number (same as starting line number if the 
     * ParserPos is a point, not a range)
     */
    public int getEndLineNum()
    {
        return endLineNumber;
    }
    
    /**
     * @return 1-based end column number (same as starting column number if the 
     * ParserPos is a point, not a range)
     */
    public int getEndColumnNum()
    {
        return endColumnNumber;
    }

    // implements Object
    public String toString()
    {
        return EigenbaseResource.instance().getParserContext(
            new Integer(lineNumber),
            new Integer(columnNumber));
    }

    /**
     * Combines this parser position with another to create a position which
     * spans from the first point in the first to the last point in the other.
     */
    public SqlParserPos plus(SqlParserPos pos)
    {
        return new SqlParserPos(
            getLineNum(),
            getColumnNum(),
            pos.getEndLineNum(),
            pos.getEndColumnNum());
    }

    /**
     * Combines this parser position with an array of positions to create a
     * position which spans from the first point in the first to the last point
     * in the other.
     */
    public SqlParserPos plusAll(SqlNode[] nodes)
    {
        int line = getLineNum();
        int column = getColumnNum();
        int endLine = getEndLineNum();
        int endColumn = getEndColumnNum();
        return sum(nodes, line, column, endLine, endColumn);
    }

    /**
     * Combines the parser positions of an array of nodes to create a
     * position which spans from the beginning of the first to the end of the
     * last.
     */
    public static SqlParserPos sum(
        SqlNode[] nodes)
    {
        return sum(nodes, -1, -1, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private static SqlParserPos sum(
        SqlNode[] nodes,
        int line,
        int column,
        int endLine,
        int endColumn)
    {
        for (int i = 0; i < nodes.length; i++) {
            SqlNode node = nodes[i];
            SqlParserPos pos = node.getParserPosition();
            if (pos.getLineNum() < line ||
                pos.getLineNum() == line &&
                pos.getColumnNum() < column) {
                line = pos.getLineNum();
                column = pos.getColumnNum();
            }
            if (pos.getEndLineNum() > endLine ||
                pos.getEndLineNum() == endColumn &&
                pos.getEndColumnNum() > endColumn) {
                endLine = pos.getLineNum();
                endColumn = pos.getColumnNum();
            }
        }
        return new SqlParserPos(line, column, endLine, endColumn);
    }

}


// End SqlParserPos.java
