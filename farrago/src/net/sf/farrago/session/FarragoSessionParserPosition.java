/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.session;

import net.sf.farrago.resource.*;

import org.eigenbase.resource.*;


/**
 * FarragoSessionParserPosition represents the position of a parsed
 * token within SQL statement text.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionParserPosition
{
    //~ Instance fields -------------------------------------------------------

    private final int beginLine;
    private final int beginColumn;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new parser position.
     */
    public FarragoSessionParserPosition(
        int beginLine,
        int beginColumn)
    {
        this.beginLine = beginLine;
        this.beginColumn = beginColumn;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return line position of token beginning
     */
    public int getBeginLine()
    {
        return beginLine;
    }

    /**
     * @return column position of token beginning
     */
    public int getBeginColumn()
    {
        return beginColumn;
    }

    // implement Object
    public String toString()
    {
        return EigenbaseResource.instance().getParserContext(
            new Integer(beginLine),
            new Integer(beginColumn));
    }
}


// End FarragoSessionParserPosition.java
