/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.sql.parser;

import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract base for parsers generated from CommonParser.jj.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CommonParserBase
{
    public SqlStdOperatorTable opTab = SqlOperatorTable.std();

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
    protected static final ExprContext EXPR_ACCEPT_NONQUERY = new ExprContext();

    /**
     * Accept only parenthesized queries or non-query expressions
     * in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_SUBQUERY = new ExprContext();


    protected int nDynamicParams;

    /**
     * Type-safe enum for context of acceptable expressions.
     */
    protected static class ExprContext
    {
    }

    // A list of SQL 92 reserved words. Please see http://www.postgresql.org/docs/7.4/static/sql-keywords-appendix.html
    // for detail.
    public static final Set SQL92ReservedWords = new HashSet();

    static {
        SQL92ReservedWords.add("absolute");
        SQL92ReservedWords.add("action");
        SQL92ReservedWords.add("add");
        SQL92ReservedWords.add("all");
        SQL92ReservedWords.add("allocate");
        SQL92ReservedWords.add("alter");
        SQL92ReservedWords.add("and");
        SQL92ReservedWords.add("any");
        SQL92ReservedWords.add("are");
        SQL92ReservedWords.add("as");
        SQL92ReservedWords.add("asc");
        SQL92ReservedWords.add("assertion");
        SQL92ReservedWords.add("at");
        SQL92ReservedWords.add("authorization");
        SQL92ReservedWords.add("avg");
        SQL92ReservedWords.add("begin");
        SQL92ReservedWords.add("between");
        SQL92ReservedWords.add("bit");
        SQL92ReservedWords.add("bit_length");
        SQL92ReservedWords.add("both");
        SQL92ReservedWords.add("by");
        SQL92ReservedWords.add("cascade");
        SQL92ReservedWords.add("cascaded");
        SQL92ReservedWords.add("case");
        SQL92ReservedWords.add("cast");
        SQL92ReservedWords.add("catalog");
        SQL92ReservedWords.add("char");
        SQL92ReservedWords.add("character");
        SQL92ReservedWords.add("character_length");
        SQL92ReservedWords.add("char_length");
        SQL92ReservedWords.add("check");
        SQL92ReservedWords.add("close");
        SQL92ReservedWords.add("coalesce");
        SQL92ReservedWords.add("collate");
        SQL92ReservedWords.add("collation");
        SQL92ReservedWords.add("column");
        SQL92ReservedWords.add("commit");
        SQL92ReservedWords.add("connect");
        SQL92ReservedWords.add("connection");
        SQL92ReservedWords.add("constraint");
        SQL92ReservedWords.add("constraints");
        SQL92ReservedWords.add("continue");
        SQL92ReservedWords.add("convert");
        SQL92ReservedWords.add("corresponding");
        SQL92ReservedWords.add("count");
        SQL92ReservedWords.add("create");
        SQL92ReservedWords.add("cross");
        SQL92ReservedWords.add("current");
        SQL92ReservedWords.add("current_date");
        SQL92ReservedWords.add("current_time");
        SQL92ReservedWords.add("current_timestamp");
        SQL92ReservedWords.add("current_user");
        SQL92ReservedWords.add("cursor");
        SQL92ReservedWords.add("date");
        SQL92ReservedWords.add("day");
        SQL92ReservedWords.add("deallocate");
        SQL92ReservedWords.add("dec");
        SQL92ReservedWords.add("decimal");
        SQL92ReservedWords.add("declare");
        SQL92ReservedWords.add("default");
        SQL92ReservedWords.add("deferrable");
        SQL92ReservedWords.add("deferred");
        SQL92ReservedWords.add("delete");
        SQL92ReservedWords.add("desc");
        SQL92ReservedWords.add("describe");
        SQL92ReservedWords.add("descriptor");
        SQL92ReservedWords.add("diagnostics");
        SQL92ReservedWords.add("disconnect");
        SQL92ReservedWords.add("distinct");
        SQL92ReservedWords.add("domain");
        SQL92ReservedWords.add("double");
        SQL92ReservedWords.add("drop");
        SQL92ReservedWords.add("else");
        SQL92ReservedWords.add("end");
        SQL92ReservedWords.add("end-exec");
        SQL92ReservedWords.add("escape");
        SQL92ReservedWords.add("except");
        SQL92ReservedWords.add("exception");
        SQL92ReservedWords.add("exec");
        SQL92ReservedWords.add("execute");
        SQL92ReservedWords.add("exists");
        SQL92ReservedWords.add("external");
        SQL92ReservedWords.add("extract");
        SQL92ReservedWords.add("false");
        SQL92ReservedWords.add("fetch");
        SQL92ReservedWords.add("first");
        SQL92ReservedWords.add("float");
        SQL92ReservedWords.add("for");
        SQL92ReservedWords.add("foreign");
        SQL92ReservedWords.add("found");
        SQL92ReservedWords.add("from");
        SQL92ReservedWords.add("full");
        SQL92ReservedWords.add("get");
        SQL92ReservedWords.add("global");
        SQL92ReservedWords.add("go");
        SQL92ReservedWords.add("goto");
        SQL92ReservedWords.add("grant");
        SQL92ReservedWords.add("group");
        SQL92ReservedWords.add("having");
        SQL92ReservedWords.add("hour");
        SQL92ReservedWords.add("identity");
        SQL92ReservedWords.add("immediate");
        SQL92ReservedWords.add("in");
        SQL92ReservedWords.add("indicator");
        SQL92ReservedWords.add("initially");
        SQL92ReservedWords.add("inner");
        SQL92ReservedWords.add("inadd");
        SQL92ReservedWords.add("insensitive");
        SQL92ReservedWords.add("insert");
        SQL92ReservedWords.add("int");
        SQL92ReservedWords.add("integer");
        SQL92ReservedWords.add("intersect");
        SQL92ReservedWords.add("interval");
        SQL92ReservedWords.add("into");
        SQL92ReservedWords.add("is");
        SQL92ReservedWords.add("isolation");
        SQL92ReservedWords.add("join");
        SQL92ReservedWords.add("key");
        SQL92ReservedWords.add("language");
        SQL92ReservedWords.add("last");
        SQL92ReservedWords.add("leading");
        SQL92ReservedWords.add("left");
        SQL92ReservedWords.add("level");
        SQL92ReservedWords.add("like");
        SQL92ReservedWords.add("local");
        SQL92ReservedWords.add("lower");
        SQL92ReservedWords.add("match");
        SQL92ReservedWords.add("max");
        SQL92ReservedWords.add("min");
        SQL92ReservedWords.add("minute");
        SQL92ReservedWords.add("module");
        SQL92ReservedWords.add("month");
        SQL92ReservedWords.add("names");
        SQL92ReservedWords.add("national");
        SQL92ReservedWords.add("natural");
        SQL92ReservedWords.add("nchar");
        SQL92ReservedWords.add("next");
        SQL92ReservedWords.add("no");
        SQL92ReservedWords.add("not");
        SQL92ReservedWords.add("null");
        SQL92ReservedWords.add("nullif");
        SQL92ReservedWords.add("numeric");
        SQL92ReservedWords.add("octet_length");
        SQL92ReservedWords.add("of");
        SQL92ReservedWords.add("on");
        SQL92ReservedWords.add("only");
        SQL92ReservedWords.add("open");
        SQL92ReservedWords.add("option");
        SQL92ReservedWords.add("or");
        SQL92ReservedWords.add("order");
        SQL92ReservedWords.add("outer");
        SQL92ReservedWords.add("outadd");
        SQL92ReservedWords.add("overlaps");
        SQL92ReservedWords.add("pad");
        SQL92ReservedWords.add("partial");
        SQL92ReservedWords.add("position");
        SQL92ReservedWords.add("precision");
        SQL92ReservedWords.add("prepare");
        SQL92ReservedWords.add("preserve");
        SQL92ReservedWords.add("primary");
        SQL92ReservedWords.add("prior");
        SQL92ReservedWords.add("privileges");
        SQL92ReservedWords.add("procedure");
        SQL92ReservedWords.add("public");
        SQL92ReservedWords.add("read");
        SQL92ReservedWords.add("real");
        SQL92ReservedWords.add("references");
        SQL92ReservedWords.add("relative");
        SQL92ReservedWords.add("restrict");
        SQL92ReservedWords.add("revoke");
        SQL92ReservedWords.add("right");
        SQL92ReservedWords.add("rollback");
        SQL92ReservedWords.add("rows");
        SQL92ReservedWords.add("schema");
        SQL92ReservedWords.add("scroll");
        SQL92ReservedWords.add("second");
        SQL92ReservedWords.add("section");
        SQL92ReservedWords.add("select");
        SQL92ReservedWords.add("session");
        SQL92ReservedWords.add("session_user");
        SQL92ReservedWords.add("set");
        SQL92ReservedWords.add("size");
        SQL92ReservedWords.add("smallint");
        SQL92ReservedWords.add("some");
        SQL92ReservedWords.add("space");
        SQL92ReservedWords.add("sql");
        SQL92ReservedWords.add("sqlcode");
        SQL92ReservedWords.add("sqlerror");
        SQL92ReservedWords.add("sqlstate");
        SQL92ReservedWords.add("substring");
        SQL92ReservedWords.add("sum");
        SQL92ReservedWords.add("system_user");
        SQL92ReservedWords.add("table");
        SQL92ReservedWords.add("temporary");
        SQL92ReservedWords.add("then");
        SQL92ReservedWords.add("time");
        SQL92ReservedWords.add("timestamp");
        SQL92ReservedWords.add("timezone_hour");
        SQL92ReservedWords.add("timezone_minute");
        SQL92ReservedWords.add("to");
        SQL92ReservedWords.add("trailing");
        SQL92ReservedWords.add("transaction");
        SQL92ReservedWords.add("translate");
        SQL92ReservedWords.add("translation");
        SQL92ReservedWords.add("trim");
        SQL92ReservedWords.add("true");
        SQL92ReservedWords.add("union");
        SQL92ReservedWords.add("unique");
        SQL92ReservedWords.add("unknown");
        SQL92ReservedWords.add("update");
        SQL92ReservedWords.add("upper");
        SQL92ReservedWords.add("usage");
        SQL92ReservedWords.add("user");
        SQL92ReservedWords.add("using");
        SQL92ReservedWords.add("value");
        SQL92ReservedWords.add("values");
        SQL92ReservedWords.add("varchar");
        SQL92ReservedWords.add("varying");
        SQL92ReservedWords.add("view");
        SQL92ReservedWords.add("when");
        SQL92ReservedWords.add("whenever");
        SQL92ReservedWords.add("where");
        SQL92ReservedWords.add("with");
        SQL92ReservedWords.add("work");
        SQL92ReservedWords.add("write");
        SQL92ReservedWords.add("year");
        SQL92ReservedWords.add("zone");
    }

    /**
     *
     * @return a set of string function name
     */
    public Set getStringFunctionNames()
    {
        return opTab.stringFuncNames;
    }

    /**
     *
     * @return  a set of numberic function name
     */
    public Set getNumericFunctionNames()
    {
        return opTab.numericFuncNames;
    }

    /**
     *
     * @return a set of time and date function name
     */
    public Set getTimeDateFunctionNames()
    {
        return opTab.timeDateFuncNames;
    }

    /**
     *
     * @return  a set of system function name
     */
    public Set getSystemFunctionNames()
    {
        return opTab.systemFuncNames;
    }

}

// End CommonParserBase.java
