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
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

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
public class FarragoParser implements FarragoSessionParser
{
    //~ Instance fields -------------------------------------------------------

    /** Validator to use for validating DDL statements as they are parsed. */
    FarragoSessionDdlValidator ddlValidator;

    /** Underlying parser implementation which does the work. */
    private FarragoParserImpl parserImpl;
    private boolean doneParsing;

    private FarragoReposTxnContext txnContext;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new parser.
     */
    public FarragoParser()
    {
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionParser
    public FarragoSessionParserPosition getCurrentPosition()
    {
        if (doneParsing) {
            return null;
        } else {
            return parserImpl.getCurrentPosition();
        }
    }

    // implement FarragoSessionParser
    public Object parseSqlStatement(
        FarragoSessionDdlValidator ddlValidator,
        FarragoReposTxnContext txnContext,
        String sql)
    {
        this.ddlValidator = ddlValidator;
        this.txnContext = txnContext;
        
        parserImpl = new FarragoParserImpl(new StringReader(sql));
        parserImpl.farragoParser = this;

        try {
            return parserImpl.FarragoSqlStmtEof();
        } catch (ParseException ex) {
            throw FarragoResource.instance().newParserError(
                ex.getMessage(),
                ex);
        } finally {
            doneParsing = true;
        }
    }

    /**
     * Start a repository write transaction.  This is called by parserImpl when
     * it's sure the statement is DDL and before it starts making any catalog
     * updates.
     */
    void startReposWriteTxn()
    {
        txnContext.beginWriteTxn();
    }
}


// End FarragoParser.java
