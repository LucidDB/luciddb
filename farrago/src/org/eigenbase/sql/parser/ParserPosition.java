/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package org.eigenbase.sql.parser;

import org.eigenbase.resource.EigenbaseResource;

/**
 * ParserPosition represents the position of a parsed
 * token within SQL statement text.
 *
 * @author Kinkoi Lo
 * @since Jun 1, 2004
 * @version $Id$
 **/
public class ParserPosition
{
    private int beginLine;

    private int beginColumn;

    /**
     * ParserPosition representing line one, character one. Use this if the
     * node doesn't correspond to a position in piece of SQL text. 
     */
    public static final ParserPosition ZERO = new ParserPosition(0,0);

    /**
    * Creates a new parser position.
    */
    public ParserPosition(int beginLine, int beginColumn)
    {
        this.beginLine = beginLine;
        this.beginColumn = beginColumn;
    }

    /**
     *
     * @return  line position of token beginning
     */
    public int getBeginLine()
    {
        return beginLine;
    }

    /**
     *
     * @param beginLine line position of token beginning
     */
    public void setBeginLine(int beginLine)
    {
        this.beginLine = beginLine;
    }

    /**
     *
     * @return column position of token beginning
     */
    public int getBeginColumn()
    {
        return beginColumn;
    }

    /**
     *
     * @param beginColumn column position of token beginning
     */
    public void setBeginColumn(int beginColumn)
    {
        this.beginColumn = beginColumn;
    }

    // implements Object
    public String toString()
    {
        return EigenbaseResource.instance().getParserContext(
                new Integer(beginLine),
                new Integer(beginColumn));
    }
}

// End ParserPosition.java
