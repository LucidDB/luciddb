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

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.resource.*;

import java.io.*;

import java.util.*;

import javax.jmi.reflect.*;


/**
 * FarragoParser is the public wrapper for the JavaCC-generated
 * FarragoParserImpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoParser
{
    //~ Instance fields -------------------------------------------------------

    /** Validator to use for validating DDL statements as they are parsed. */
    public DdlValidator ddlValidator;

    /** Underlying parser implementation which does the work. */
    private final FarragoParserImpl parserImpl;
    private boolean doneParsing;
    private boolean txn;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new parser which reads input from a string.
     *
     * @param catalog catalog containing metadata affected by parsing
     * @param sql string to be parsed
     */
    public FarragoParser(FarragoCatalog catalog,String sql)
    {
        parserImpl = new FarragoParserImpl(new StringReader(sql));
        parserImpl.farragoParser = this;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return the ParserContext for the current position, or null if done
     *         parsing
     */
    public ParserContext getCurrentContext()
    {
        if (doneParsing) {
            return null;
        } else {
            return parserImpl.getCurrentContext();
        }
    }

    /**
     * Parse the SQL statement whose text was specified at construction.  If it
     * is a DDL statement, this may implicitly performs uncommitted catalog
     * updates.
     *
     * @return for DDL, a DdlStmt; for DML or query, top-level SqlNode
     */
    public Object parseSqlStatement()
    {
        assert (ddlValidator != null);

        try {
            return parserImpl.FarragoSqlStmtEof();
        } catch (ParseException ex) {
            throw ddlValidator.res.newParserError(
                ex.getMessage(),
                ex);
        } finally {
            doneParsing = true;
        }
    }

    /**
     * Rollback any repository transaction initiated by this parser.
     */
    public void rollbackReposTxn()
    {
        if (txn) {
            txn = false;
            ddlValidator.getCatalog().getRepository().endTrans(true);
        }
    }

    /**
     * Commmit any repository transaction initiated by this parser.
     */
    public void commitReposTxn()
    {
        if (txn) {
            txn = false;
            ddlValidator.getCatalog().getRepository().endTrans();
        }
    }

    /**
     * Start a repository write transaction.  This is called by parserImpl when
     * it's sure the statement is DDL and before it starts making any catalog
     * updates.
     */
    void startReposWriteTxn()
    {
        ddlValidator.getCatalog().getRepository().beginTrans(true);
        txn = true;
    }
}


// End FarragoParser.java
