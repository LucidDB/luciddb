/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.sql.parser;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;


/**
 * Abstract base for parsers generated from CommonParser.jj.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SqlAbstractParserImpl
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Accept any kind of expression in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_ALL = new ExprContext();

    /**
     * Accept only query expressions in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_QUERY = new ExprContext();

    /**
     * Accept only non-query expressions in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_NONQUERY =
        new ExprContext();

    /**
     * Accept only parenthesized queries or non-query expressions
     * in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_SUBQUERY =
        new ExprContext();

    private static final Set sql92ReservedWords;

    static {
        Set set = new HashSet();
        set.add("ABSOLUTE");
        set.add("ACTION");
        set.add("ADD");
        set.add("ALL");
        set.add("ALLOCATE");
        set.add("ALTER");
        set.add("AND");
        set.add("ANY");
        set.add("ARE");
        set.add("AS");
        set.add("ASC");
        set.add("ASSERTION");
        set.add("AT");
        set.add("AUTHORIZATION");
        set.add("AVG");
        set.add("BEGIN");
        set.add("BETWEEN");
        set.add("BIT");
        set.add("BIT_LENGTH");
        set.add("BOTH");
        set.add("BY");
        set.add("CASCADE");
        set.add("CASCADED");
        set.add("CASE");
        set.add("CAST");
        set.add("CATALOG");
        set.add("CHAR");
        set.add("CHARACTER");
        set.add("CHARACTER_LENGTH");
        set.add("CHAR_LENGTH");
        set.add("CHECK");
        set.add("CLOSE");
        set.add("COALESCE");
        set.add("COLLATE");
        set.add("COLLATION");
        set.add("COLUMN");
        set.add("COMMIT");
        set.add("CONNECT");
        set.add("CONNECTION");
        set.add("CONSTRAINT");
        set.add("CONSTRAINTS");
        set.add("CONTINUE");
        set.add("CONVERT");
        set.add("CORRESPONDING");
        set.add("COUNT");
        set.add("CREATE");
        set.add("CROSS");
        set.add("CURRENT");
        set.add("CURRENT_DATE");
        set.add("CURRENT_TIME");
        set.add("CURRENT_TIMESTAMP");
        set.add("CURRENT_USER");
        set.add("CURSOR");
        set.add("DATE");
        set.add("DAY");
        set.add("DEALLOCATE");
        set.add("DEC");
        set.add("DECIMAL");
        set.add("DECLARE");
        set.add("DEFAULT");
        set.add("DEFERRABLE");
        set.add("DEFERRED");
        set.add("DELETE");
        set.add("DESC");
        set.add("DESCRIBE");
        set.add("DESCRIPTOR");
        set.add("DIAGNOSTICS");
        set.add("DISCONNECT");
        set.add("DISTINCT");
        set.add("DOMAIN");
        set.add("DOUBLE");
        set.add("DROP");
        set.add("ELSE");
        set.add("END");
        set.add("END-EXEC");
        set.add("ESCAPE");
        set.add("EXCEPT");
        set.add("EXCEPTION");
        set.add("EXEC");
        set.add("EXECUTE");
        set.add("EXISTS");
        set.add("EXTERNAL");
        set.add("EXTRACT");
        set.add("FALSE");
        set.add("FETCH");
        set.add("FIRST");
        set.add("FLOAT");
        set.add("FOR");
        set.add("FOREIGN");
        set.add("FOUND");
        set.add("FROM");
        set.add("FULL");
        set.add("GET");
        set.add("GLOBAL");
        set.add("GO");
        set.add("GOTO");
        set.add("GRANT");
        set.add("GROUP");
        set.add("HAVING");
        set.add("HOUR");
        set.add("IDENTITY");
        set.add("IMMEDIATE");
        set.add("IN");
        set.add("INDICATOR");
        set.add("INITIALLY");
        set.add("INNER");
        set.add("INADD");
        set.add("INSENSITIVE");
        set.add("INSERT");
        set.add("INT");
        set.add("INTEGER");
        set.add("INTERSECT");
        set.add("INTERVAL");
        set.add("INTO");
        set.add("IS");
        set.add("ISOLATION");
        set.add("JOIN");
        set.add("KEY");
        set.add("LANGUAGE");
        set.add("LAST");
        set.add("LEADING");
        set.add("LEFT");
        set.add("LEVEL");
        set.add("LIKE");
        set.add("LOCAL");
        set.add("LOWER");
        set.add("MATCH");
        set.add("MAX");
        set.add("MIN");
        set.add("MINUTE");
        set.add("MODULE");
        set.add("MONTH");
        set.add("NAMES");
        set.add("NATIONAL");
        set.add("NATURAL");
        set.add("NCHAR");
        set.add("NEXT");
        set.add("NO");
        set.add("NOT");
        set.add("NULL");
        set.add("NULLIF");
        set.add("NUMERIC");
        set.add("OCTET_LENGTH");
        set.add("OF");
        set.add("ON");
        set.add("ONLY");
        set.add("OPEN");
        set.add("OPTION");
        set.add("OR");
        set.add("ORDER");
        set.add("OUTER");
        set.add("OUTADD");
        set.add("OVERLAPS");
        set.add("PAD");
        set.add("PARTIAL");
        set.add("POSITION");
        set.add("PRECISION");
        set.add("PREPARE");
        set.add("PRESERVE");
        set.add("PRIMARY");
        set.add("PRIOR");
        set.add("PRIVILEGES");
        set.add("PROCEDURE");
        set.add("PUBLIC");
        set.add("READ");
        set.add("REAL");
        set.add("REFERENCES");
        set.add("RELATIVE");
        set.add("RESTRICT");
        set.add("REVOKE");
        set.add("RIGHT");
        set.add("ROLLBACK");
        set.add("ROWS");
        set.add("SCHEMA");
        set.add("SCROLL");
        set.add("SECOND");
        set.add("SECTION");
        set.add("SELECT");
        set.add("SESSION");
        set.add("SESSION_USER");
        set.add("SET");
        set.add("SIZE");
        set.add("SMALLINT");
        set.add("SOME");
        set.add("SPACE");
        set.add("SQL");
        set.add("SQLCODE");
        set.add("SQLERROR");
        set.add("SQLSTATE");
        set.add("SUBSTRING");
        set.add("SUM");
        set.add("SYSTEM_USER");
        set.add("TABLE");
        set.add("TEMPORARY");
        set.add("THEN");
        set.add("TIME");
        set.add("TIMESTAMP");
        set.add("TIMEZONE_HOUR");
        set.add("TIMEZONE_MINUTE");
        set.add("TO");
        set.add("TRAILING");
        set.add("TRANSACTION");
        set.add("TRANSLATE");
        set.add("TRANSLATION");
        set.add("TRIM");
        set.add("TRUE");
        set.add("UNION");
        set.add("UNIQUE");
        set.add("UNKNOWN");
        set.add("UPDATE");
        set.add("UPPER");
        set.add("USAGE");
        set.add("USER");
        set.add("USING");
        set.add("VALUE");
        set.add("VALUES");
        set.add("VARCHAR");
        set.add("VARYING");
        set.add("VIEW");
        set.add("WHEN");
        set.add("WHENEVER");
        set.add("WHERE");
        set.add("WITH");
        set.add("WORK");
        set.add("WRITE");
        set.add("YEAR");
        set.add("ZONE");
        sql92ReservedWords = Collections.unmodifiableSet(set);
    }

    /**
     * @return immutable set of all reserved words defined by SQL-92
     *
     * @sql.92 Section 5.2
     */
    public static Set getSql92ReservedWords()
    {
        return sql92ReservedWords;
    }

    //~ Instance fields -------------------------------------------------------

    /**
     * Operator table containing the standard SQL operators and functions.
     */
    public final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

    protected int nDynamicParams;

    //~ Methods ---------------------------------------------------------------

    protected SqlCall createCall(
        SqlIdentifier funName,
        SqlNode [] operands,
        SqlParserPos pos,
        SqlFunction.SqlFuncTypeName funcType)
    {
        SqlOperator fun = null;

        // First, try a half-hearted resolution as a builtin function.
        // If we find one, use it; this will guarantee that we
        // preserve the correct syntax (i.e. don't quote builtin function
        /// name when regenerating SQL).
        if (funName.isSimple()) {
            List list = opTab.lookupOperatorOverloads(
                funName,
                SqlSyntax.Function);
            if (list.size() == 1) {
                fun = (SqlOperator) list.get(0);
            }
        }

        // Otherwise, just create a placeholder function.  Later, during
        // validation, it will be resolved into a real function reference.
        if (fun == null) {
            fun = new SqlFunction(funName, null, null, null, null, funcType);
        }
        
        return fun.createCall(operands, pos);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Type-safe enum for context of acceptable expressions.
     */
    protected static class ExprContext
    {
    }
}


// End SqlAbstractParserImpl.java
