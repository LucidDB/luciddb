/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.session.*;

import java.io.*;

/**
 * Abstract base for parsers generated from CommonDdlParser.jj.
 * Most of the methods on this class correspond to specific methods
 * generated on subclasses.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAbstractParserImpl extends SqlAbstractParserImpl
{
    /**
     * Public parser interface.
     */
    protected FarragoSessionParser farragoParser;

    /**
     * Whether a DROP RESTRICT statement is being processed
     */
    protected boolean dropRestrict;

    /**
     * @return repository accessed by this parser
     */
    public FarragoRepos getRepos()
    {
        return farragoParser.getDdlValidator().getRepos();
    }
    
    /**
     * @return current parser position
     */
    public abstract SqlParserPos getCurrentPosition();

    /**
     * @return result of parsing an SQL expression
     */
    public abstract SqlNode SqlExpressionEof() throws Exception;

    /**
     * @return result of parsing a complete statement
     */
    public abstract Object FarragoSqlStmtEof() throws Exception;

    /**
     * Reinitializes parser with new input.
     *
     * @param reader provides new input
     */
    public abstract void ReInit(Reader reader);

    /**
     * Tests whether the current input is a non-reserved keyword.
     *
     * @return token if non-reserved
     *
     * @throws Exception if not a non-reserved keyword
     */
    public abstract String NonReservedKeyWord() throws Exception;
}

// End FarragoAbstractParserImpl.java
