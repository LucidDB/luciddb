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
public class FarragoParser implements FarragoSessionParser, FarragoParserWrapper
{
    //~ Static fields/initializers --------------------------------------------

    private static final String STR_FUNC_NAMES;
    private static final String NUMERIC_FUNC_NAMES;
    private static final String TIME_DATE_FUNC_NAMES;
    private static final String SYS_FUNC_NAMES;

    static {
        FarragoParserImpl parser = new FarragoParserImpl(new StringReader(""));

        STR_FUNC_NAMES = constructFuncList(parser.getStringFunctionNames());
        NUMERIC_FUNC_NAMES = constructFuncList(parser.getNumericFunctionNames());
        TIME_DATE_FUNC_NAMES = constructFuncList(parser.getTimeDateFunctionNames());
        SYS_FUNC_NAMES = constructFuncList(parser.getSystemFunctionNames());
    }

    private static String constructFuncList(Set functionNames) {
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (Iterator iterator = functionNames.iterator(); iterator.hasNext();) {
            String funcName = (String) iterator.next();
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(funcName);
        }
        return sb.toString();
    }

    //~ Instance fields -------------------------------------------------------

    /** Validator to use for validating DDL statements as they are parsed. */
    FarragoSessionDdlValidator ddlValidator;

    /** Underlying parser implementation which does the work. */
    private FarragoParserImpl parserImpl;
    private boolean doneParsing;

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
    public String getSQLKeywords()
    {
        String[] tokens = FarragoParserImplConstants.tokenImage;
        StringBuffer sb = new StringBuffer();
        boolean withComma = false;
        for (int i = 0, size = tokens.length; i < size; i++) {
            String tokenVal = getTokenVal(tokens[i]);
            if (tokenVal != null &&
                    !FarragoParserImpl.SQL92ReservedWords.contains(tokenVal) &&
                    !isNonReserved(tokenVal)) {
                if (withComma) {
                    sb.append(",");
                } else {
                    withComma = true;
                }
                sb.append(tokenVal);
            }
        }
        return sb.toString();
    }


    // implement FarragoSessionParser
    public String getStringFunctions() {
       return FarragoParser.STR_FUNC_NAMES;
    }

    // implement FarragoSessionParser
    public String getNumericFunctions() {
        return FarragoParser.NUMERIC_FUNC_NAMES;
    }

    // implement FarragoSessionParser
    public String getTimeDateFunctions() {
        return FarragoParser.TIME_DATE_FUNC_NAMES;
    }

    // implement FarragoSessionParser
    public String getSystemFunctions() {
        return FarragoParser.SYS_FUNC_NAMES;
    }

    // implement FarragoParserWrapper
    public FarragoSessionDdlValidator getDdlValidator() {
        return ddlValidator;
    }

    private String getTokenVal(String token) {

        // We don't care about the token which are not string
        if (!token.startsWith("\"")) {
            return null;
        }
        // Remove the quote from the token
        int startIndex = token.indexOf("\"");
        int endIndex = token.lastIndexOf("\"");
        String tokenVal = token.substring(startIndex + 1, endIndex);
        char c = tokenVal.charAt(0);
        if (Character.isLetter(c)) {
            return tokenVal;
        }
        return null;
    }

    private boolean isNonReserved(String keyword) {
        FarragoParserImpl parserImpl = new FarragoParserImpl(new StringReader(keyword));
        try {
            parserImpl.NonReservedKeyWord();
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    // implement FarragoSessionParser
    public Object parseSqlStatement(
        FarragoSessionDdlValidator ddlValidator,
        String sql)
    {
        this.ddlValidator = ddlValidator;

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
}


// End FarragoParser.java
