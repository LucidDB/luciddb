/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import net.sf.farrago.session.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.parser.impl.*;

import java.io.*;
import java.util.*;

import org.eigenbase.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorException;
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
            if (ex instanceof ParseException) {
                ex = cleanupParseException((ParseException) ex);
            }
            throw EigenbaseResource.instance().newParserError(
                ex.getMessage(),
                ex);
        } finally {
            sourceString = null;
        }
    }

    /**
     * Removes or transforms misleading information from a parse exception.
     *
     *<p>
     *
     * TODO jvs 1-Feb-2005:  figure out how to move this up to SqlParser
     * level in such a way that we can share it.
     *
     * @param ex dirty excn
     *
     * @return clean excn
     */
    private Exception cleanupParseException(ParseException ex)
    {
        if (ex.expectedTokenSequences == null) {
            return ex;
        }
        int iIdentifier = Arrays.asList(ex.tokenImage).indexOf("<IDENTIFIER>");

        boolean id = false;
        for (int i = 0; i < ex.expectedTokenSequences.length; ++i) {
            if (ex.expectedTokenSequences[i][0] == iIdentifier) {
                id = true;
                break;
            }
        }

        if (!id) {
            return ex;
        }

        // Since <IDENTIFIER> was one of the possible productions,
        // we know that the parser will also have included all
        // of the non-reserved keywords (which are treated as
        // identifiers in non-keyword contexts).  So, now we need
        // to clean those out, since they're totally irrelevant.

        List list = new ArrayList();
        for (int i = 0; i < ex.expectedTokenSequences.length; ++i) {
            int [] sequence = ex.expectedTokenSequences[i];
            String token = getTokenVal(ex.tokenImage[sequence[0]]);
            if (token != null) {
                if (isNonReserved(parserImpl, token)) {
                    continue;
                }
                if (isReservedFunctionName(parserImpl, token)) {
                    continue;
                }
                if (isContextVariable(parserImpl, token)) {
                    continue;
                }
            }
            list.add(sequence);
        }

        ex.expectedTokenSequences = (int [][]) list.toArray(new int [0][]);
        return ex;
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

    private static boolean isReservedFunctionName(
        FarragoAbstractParserImpl parserImpl, String keyword)
    {
        parserImpl.ReInit(new StringReader(keyword));
        try {
            parserImpl.ReservedFunctionName();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isContextVariable(
        FarragoAbstractParserImpl parserImpl, String keyword)
    {
        parserImpl.ReInit(new StringReader(keyword));
        try {
            parserImpl.ContextVariable();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

// End FarragoAbstractParser.java
