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

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * FarragoAbstractParser is an abstract base for implementations of the {@link
 * FarragoSessionParser} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAbstractParser
    implements FarragoSessionParser
{
    //~ Instance fields --------------------------------------------------------

    protected FarragoSessionStmtValidator stmtValidator;

    protected FarragoSessionDdlValidator ddlValidator;

    protected FarragoAbstractParserImpl parserImpl;

    protected String sourceString;

    private boolean parsingComplete = false;


    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionParser
    public SqlParserPos getCurrentPosition()
    {
        if ((sourceString == null) || parsingComplete) {
            return null;
        } else {
            return parserImpl.getCurrentPosition();
        }
    }

    // implement FarragoSessionParser
    public EigenbaseException newPositionalError(
        SqlValidatorException ex)
    {
        if ((sourceString == null) || parsingComplete) {
            return FarragoResource.instance().ValidatorNoPositionContext.ex(ex);
        } else {
            String msg = getCurrentPosition().toString();
            return FarragoResource.instance().ValidatorPositionContext.ex(
                msg,
                ex);
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
        return stmtValidator;
    }

    // implement FarragoSessionParser
    public String getSourceString()
    {
        return sourceString;
    }

    /**
     * Factory method to instantiate a dialect-specific generated parser.
     *
     * @param reader Reader that provides the input to the parser
     *
     * @return Dialect-specific generated parser
     */
    protected abstract FarragoAbstractParserImpl newParserImpl(Reader reader);

    // implement FarragoSessionParser
    public String getJdbcKeywords()
    {
        SqlAbstractParserImpl.Metadata metadata =
            newParserImpl(new StringReader("")).getMetadata();
        return metadata.getJdbcKeywords();
    }

    // implement FarragoSessionParser
    public Object parseSqlText(
        FarragoSessionStmtValidator stmtValidator,
        FarragoSessionDdlValidator ddlValidator,
        String sql,
        boolean expectStatement)
    {
        parsingComplete = false;
        this.stmtValidator = stmtValidator;
        this.ddlValidator = ddlValidator;

        parserImpl = newParserImpl(new StringReader(sql));
        parserImpl.setTabSize(1);
        parserImpl.farragoParser = this;
        sourceString = sql;

        try {
            if (expectStatement) {
                return parserImpl.FarragoSqlStmtEof();
            } else {
                return parserImpl.SqlExpressionEof();
            }
        } catch (EigenbaseContextException ex) {
            ex.setOriginalStatement(sql);
            Throwable actualEx = (ex.getCause() == null) ? ex : ex.getCause();
            throw EigenbaseResource.instance().ParserError.ex(
                actualEx.getMessage(),
                ex);
        } catch (SqlParseException spex) {
            Throwable actualEx =
                (spex.getCause() == null) ? spex : spex.getCause();
            Exception x = spex;
            final SqlParserPos pos = spex.getPos();
            if (pos != null) {
                x = SqlUtil.newContextException(pos, actualEx, sql);
            } else {
                x = spex;
            }
            throw EigenbaseResource.instance().ParserError.ex(
                actualEx.getMessage(),
                x);
        } catch (Throwable ex) {
            SqlParseException spex = parserImpl.normalizeException(ex);
            Throwable actualEx = spex;
            Exception x = spex;
            final SqlParserPos pos = spex.getPos();
            if (pos != null) {
                x = SqlUtil.newContextException(pos, actualEx, sql);

            } else {
                x = spex;
            }
            throw EigenbaseResource.instance().ParserError.ex(
                actualEx.getMessage(),
                x);
        } finally {
            parsingComplete = true;
        }
    }

    // implement FarragoSessionParser
    public String getSubstring(SqlParserPos start, SqlParserPos end)
    {
        assert (sourceString != null);

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
}

// End FarragoAbstractParser.java
