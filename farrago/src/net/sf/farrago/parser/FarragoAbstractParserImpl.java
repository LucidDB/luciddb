/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package net.sf.farrago.parser;

import java.io.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * Abstract base for parsers generated from CommonDdlParser.jj. Most of the
 * methods on this class correspond to specific methods generated on subclasses.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAbstractParserImpl
    extends SqlAbstractParserImpl
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Public parser interface.
     */
    protected FarragoSessionParser farragoParser;

    /**
     * Whether a DROP RESTRICT statement is being processed
     */
    protected boolean dropRestrict;

    //~ Methods ----------------------------------------------------------------

    /**
     * @return repository accessed by this parser
     */
    public FarragoRepos getRepos()
    {
        return farragoParser.getStmtValidator().getRepos();
    }

    /**
     * @return current parser position
     */
    public abstract SqlParserPos getCurrentPosition();

    /**
     * @return result of parsing an SQL expression
     */
    public abstract SqlNode SqlExpressionEof()
        throws Exception;

    /**
     * @return result of parsing a complete statement
     */
    public abstract Object FarragoSqlStmtEof()
        throws Exception;

    /**
     * Tests whether the current input is a non-reserved keyword.
     *
     * @return token if non-reserved
     *
     * @throws Exception if not a non-reserved keyword
     */
    public abstract String NonReservedKeyWord()
        throws Exception;

    /**
     * Tests whether the current input is a reserved function name.
     *
     * @return identifier if a reserved function name
     *
     * @throws Exception if not a reserved function name
     */
    public abstract SqlIdentifier ReservedFunctionName()
        throws Exception;

    /**
     * Tests whether the current input is a context variable name.
     *
     * @return identifier if a context variable name
     *
     * @throws Exception if not a context variable name
     */
    public abstract SqlIdentifier ContextVariable()
        throws Exception;

    /**
     * Converts the SQL representation of a default value into its catalog
     * representation.
     *
     * @param attribute attribute for which default value is being defined
     * @param defaultClause SQL representation
     */
    protected void setDefaultExpression(
        FemAbstractAttribute attribute,
        SqlNode defaultClause)
    {
        CwmExpression defaultExpression = getRepos().newCwmExpression();
        defaultExpression.setBody(defaultClause.toSqlString(null));
        defaultExpression.setLanguage("SQL");
        attribute.setInitialValue(defaultExpression);
    }

    // implement SqlAbstractParserImpl
    public abstract SqlParseException normalizeException(Throwable e);

    /**
     * Returns whether a keyword is a non-reserved word.
     *
     * @param keyword Keyword
     *
     * @return Whether the keyword is a non-reserved word.
     */
    public final boolean isNonReserved(String keyword)
    {
        ReInit(new StringReader(keyword));
        try {
            NonReservedKeyWord();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns whether a keyword is the name of a reserved function.
     *
     * @param keyword Keyword
     *
     * @return Whether the keyword is the name of a reserved function.
     */
    public boolean isReservedFunctionName(String keyword)
    {
        ReInit(new StringReader(keyword));
        try {
            ReservedFunctionName();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns whether a keyword is the name of a context variable.
     *
     * @param keyword Keyword
     *
     * @return Whether the keyword is the name of a context variable.
     */
    public boolean isContextVariable(String keyword)
    {
        ReInit(new StringReader(keyword));
        try {
            ContextVariable();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

// End FarragoAbstractParserImpl.java
