/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
    }

    //~ Methods ---------------------------------------------------------------

    /**
     *
     * @return 1-based line number
     */
    public int getLineNum()
    {
        return lineNumber;
    }

    /**
     *
     * @return 1-based column number
     */
    public int getColumnNum()
    {
        return columnNumber;
    }

    // implements Object
    public String toString()
    {
        return EigenbaseResource.instance().getParserContext(
            new Integer(lineNumber),
            new Integer(columnNumber));
    }
}


// End SqlParserPos.java
