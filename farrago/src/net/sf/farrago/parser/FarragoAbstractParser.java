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

import net.sf.farrago.session.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import java.io.*;

import org.eigenbase.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.resource.*;

/**
 * FarragoAbstractParser is an abstract base for implementations
 * of the {@link FarragoSessionParser} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAbstractParser implements FarragoSessionParser
{
    /** Validator to use for validating DDL statements as they are parsed. */
    protected FarragoSessionDdlValidator ddlValidator;

    protected FarragoAbstractParserImpl parserImpl;

    protected String sourceString;

    // implement FarragoSessionParser
    public SqlParserPos getCurrentPosition()
    {
        if (sourceString == null) {
            return null;
        } else {
            return parserImpl.getCurrentPosition();
        }
    }
    
    // implement FarragoSessionParser
    public FarragoException newPositionalError(
        SqlValidatorException ex)
    {
        if (sourceString == null) {
            return FarragoResource.instance().newValidatorNoPositionContext(ex);
        } else {
            String msg = getCurrentPosition().toString();
            return FarragoResource.instance().newValidatorPositionContext(
                msg, ex);
        }
    }
    
    // implement FarragoSessionWrapper
    public FarragoSessionDdlValidator getDdlValidator()
    {
        return ddlValidator;
    }
    
    // implement FarragoSessionParser
    public FarragoSessionStmtValidator getStmtValidator()
    {
        return ddlValidator.getStmtValidator();
    }

    protected abstract FarragoAbstractParserImpl newParserImpl(Reader reader);
    
    // implement FarragoSessionParser
    public Object parseSqlText(
        FarragoSessionDdlValidator ddlValidator,
        String sql,
        boolean expectStatement)
    {
        this.ddlValidator = ddlValidator;

        parserImpl = newParserImpl(new StringReader(sql));
        parserImpl.farragoParser = this;
        sourceString = sql;

        try {
            if (expectStatement) {
                return parserImpl.FarragoSqlStmtEof();
            } else {
                return parserImpl.SqlExpressionEof();
            }
        } catch (Exception ex) {
            throw EigenbaseResource.instance().newParserError(
                ex.getMessage(),
                ex);
        } finally {
            sourceString = null;
        }
    }

    // implement FarragoSessionParser
    public String getSubstring(SqlParserPos start, SqlParserPos end)
    {
        assert(sourceString != null);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            LineNumberReader reader =
                new LineNumberReader(new StringReader(sourceString));
            for (;;) {
                String line = reader.readLine();
                int lineNum = reader.getLineNumber();
                if (lineNum < start.getLineNum()) {
                    continue;
                } else if (lineNum == start.getLineNum()) {
                    if (lineNum == end.getLineNum()) {
                        pw.print(
                            line.substring(
                                start.getColumnNum() - 1,
                                end.getColumnNum() - 1));
                        break;
                    } else {
                        pw.println(
                            line.substring(start.getColumnNum() - 1));
                    }
                } else if (lineNum == end.getLineNum()) {
                    pw.print(
                        line.substring(0, end.getColumnNum() - 1));
                    break;
                } else {
                    pw.println(line);
                }
            }
        } catch (IOException ex) {
            throw Util.newInternal(ex);
        }
        pw.close();
        return sw.toString();
    }

    protected static String constructReservedKeywordList(
        String [] tokens,
        FarragoAbstractParserImpl parserImpl)
    {
        StringBuffer sb = new StringBuffer();
        boolean withComma = false;
        for (int i = 0, size = tokens.length; i < size; i++) {
            String tokenVal = getTokenVal(tokens[i]);
            if ((tokenVal != null)
                && !SqlAbstractParserImpl.getSql92ReservedWords().contains(
                    tokenVal)
                && !isNonReserved(parserImpl, tokenVal))
            {
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

    private static String getTokenVal(String token)
    {
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
    
    private static boolean isNonReserved(
        FarragoAbstractParserImpl parserImpl, String keyword)
    {
        parserImpl.ReInit(new StringReader(keyword));
        try {
            parserImpl.NonReservedKeyWord();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

// End FarragoAbstractParser.java
