/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
    protected FarragoSessionStmtValidator stmtValidator;

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
    public EigenbaseException newPositionalError(
        SqlValidatorException ex)
    {
        if (sourceString == null) {
            return FarragoResource.instance().ValidatorNoPositionContext.ex(ex);
        } else {
            String msg = getCurrentPosition().toString();
            return FarragoResource.instance().ValidatorPositionContext.ex(
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
        return stmtValidator;
    }

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
        this.stmtValidator = stmtValidator;
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
        } catch (EigenbaseContextException ex) {
            Throwable actualEx = (ex.getCause() == null) ? ex : ex.getCause();
            throw EigenbaseResource.instance().ParserError.ex(
                actualEx.getMessage(),
                ex);

        } catch (SqlParseException spex) {
            Throwable actualEx = (spex.getCause() == null) ? spex :
                spex.getCause();
            Exception x = spex;
            final SqlParserPos pos = spex.getPos();
            if (pos != null) {
                x = SqlUtil.newContextException(pos, actualEx);
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
                x = SqlUtil.newContextException(pos, actualEx);
            } else {
                x = spex;
            }
            throw EigenbaseResource.instance().ParserError.ex(
                actualEx.getMessage(),
                x);

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
}

// End FarragoAbstractParser.java
