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

package net.sf.farrago.parser;

import net.sf.farrago.resource.*;


/**
 * ParserContext records position information from the parser for use in error
 * messages during validation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ParserContext
{
    //~ Instance fields -------------------------------------------------------

    /** Column at which parser recorded token */
    private final int beginColumn;
    /** Line on which parser recorded token */
    private final int beginLine;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new ParserContext object.
     *
     * @param beginLine .
     * @param beginColumn .
     */
    ParserContext(int beginLine,int beginColumn)
    {
        this.beginLine = beginLine;
        this.beginColumn = beginColumn;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Object
    public String toString()
    {
        return FarragoResource.instance().getParserContext(
            new Integer(beginLine),
            new Integer(beginColumn));
    }
}


// End ParserContext.java
