/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import java.io.*;
import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;
import net.sf.farrago.parser.impl.*;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;


/**
 * FarragoParser is the public wrapper for the JavaCC-generated
 * FarragoParserImpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoParser extends FarragoAbstractParser
{
    //~ Static fields/initializers --------------------------------------------

    private static final String jdbcKeywords;

    static
    {
        FarragoParserImpl parser = new FarragoParserImpl(new StringReader(""));
        jdbcKeywords = constructReservedKeywordList(
            FarragoParserImplConstants.tokenImage,
            parser);
    }

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new parser.
     */
    public FarragoParser()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionParser
    public String getJdbcKeywords()
    {
        return jdbcKeywords;
    }
    
    // implement FarragoAbstractParser
    protected FarragoAbstractParserImpl newParserImpl(Reader reader)
    {
        return new FarragoParserImpl(reader);
    }
}


// End FarragoParser.java
