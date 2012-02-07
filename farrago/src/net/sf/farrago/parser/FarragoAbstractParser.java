/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.parser;

import java.io.*;
import java.util.*;

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
    private enum ParseMethod
    {
        SQL_STMT,
        SQL_EXPRESSION,
        DEPLOYMENT_DESCRIPTOR
    }

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
        return parseSqlTextImpl(
            stmtValidator,
            ddlValidator,
            sql,
            expectStatement
            ? ParseMethod.SQL_STMT : ParseMethod.SQL_EXPRESSION);
    }

    private Object parseSqlTextImpl(
        FarragoSessionStmtValidator stmtValidator,
        FarragoSessionDdlValidator ddlValidator,
        String sql,
        ParseMethod parseMethod)
    {
        parsingComplete = false;
        this.stmtValidator = stmtValidator;
        this.ddlValidator = ddlValidator;

        parserImpl = newParserImpl(new StringReader(sql));
        parserImpl.setTabSize(1);
        parserImpl.farragoParser = this;
        sourceString = sql;

        try {
            switch (parseMethod) {
            case SQL_STMT:
                return parserImpl.FarragoSqlStmtEof();
            case SQL_EXPRESSION:
                return parserImpl.SqlExpressionEof();
            case DEPLOYMENT_DESCRIPTOR:
            default:
                return parserImpl.DeploymentDescriptorEof();
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
    public Map<String, List<String>> parseDeploymentDescriptor(String src)
    {
        return (Map<String, List<String>>)
            parseSqlTextImpl(
                stmtValidator,
                ddlValidator,
                src,
                ParseMethod.DEPLOYMENT_DESCRIPTOR);
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
