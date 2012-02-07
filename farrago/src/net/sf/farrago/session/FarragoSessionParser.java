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
package net.sf.farrago.session;

import java.io.*;
import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * FarragoSessionParser represents an object capable of parsing Farrago SQL
 * statements.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionParser
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Parses an SQL expression. If a DDL statement, implicitly performs
     * uncommitted catalog updates.
     *
     * @param stmtValidator the statement validator to use
     * @param ddlValidator the validator to use for lookup during parsing if
     * this turns out to be a DDL statement; may be null if DDL is not allowed
     * @param sql the SQL text to be parsed
     * @param expectStatement if true, expect a statement; if false, expect a
     * row-expression
     *
     * @return for DDL, a FarragoSessionDdlStmt; for DML or query, top-level
     * SqlNode
     */
    public Object parseSqlText(
        FarragoSessionStmtValidator stmtValidator,
        FarragoSessionDdlValidator ddlValidator,
        String sql,
        boolean expectStatement);

    /**
     * Parses a SQL/J deployment descriptor file.
     *
     * @param src contents of descriptor file
     *
     * @return map from action (INSTALL and/or REMOVE) to
     * list of corresponding SQL statements
     */
    public Map<String, List<String>> parseDeploymentDescriptor(String src);

    /**
     * @return the current position, or null if done parsing
     */
    public SqlParserPos getCurrentPosition();

    /**
     * @return a comma-separated list of all a database's SQL keywords that are
     * NOT also SQL92 keywords (as defined by JDBC getSQLKeywords)
     */
    public String getJdbcKeywords();

    /**
     * @return validator to use for validating DDL statements as they are parsed
     */
    public FarragoSessionDdlValidator getDdlValidator();

    /**
     * @return validator to use for validating statements as they are parsed
     */
    public FarragoSessionStmtValidator getStmtValidator();

    /**
     * @return Last string processed by the parser.
     */
    public String getSourceString();

    /**
     * Wraps a validation error with the current position information of the
     * parser.
     *
     * @param ex exception to be wrapped
     *
     * @return wrapping exception
     */
    public EigenbaseException newPositionalError(
        SqlValidatorException ex);

    /**
     * Gets a substring from the text currently being parsed.
     *
     * @param start start position (inclusive) of substring
     * @param end end position (exclusive) of substring
     *
     * @return substring
     */
    public String getSubstring(SqlParserPos start, SqlParserPos end);
}

// End FarragoSessionParser.java
