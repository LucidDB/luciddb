/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj.stmt;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.saffron.oj.OJConnectionRegistry;
import net.sf.saffron.oj.OJPlannerFactory;
import net.sf.saffron.oj.OJValidator;
import net.sf.saffron.oj.xlat.OJQueryExpander;
import net.sf.saffron.oj.xlat.OJSchemaExpander;
import net.sf.saffron.oj.xlat.SqlToOpenjavaConverter;
import net.sf.saffron.trace.SaffronTrace;

import openjava.mop.*;
import openjava.ojc.JavaCompiler;
import openjava.ojc.JavaCompilerArgs;
import openjava.ptree.*;
import openjava.ptree.util.*;

import org.eigenbase.oj.OJTypeFactoryImpl;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.ClassCollector;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * An <code>OJStatement</code> is used to execute a saffron (or regular Java)
 * expression dynamically.
 */
public class OJStatement extends OJPreparingStmt
{
    private static final Logger tracer = SaffronTrace.getStatementTracer();
    int executionCount = 0;

    /**
     * Creates a statement
     *
     * @param connection Connection statement belongs to; may be null, but only
     *   if this statement implements {@link RelOptConnection}
     *
     * @pre connection != null || this instanceof RelOptConnection
     */
    public OJStatement(RelOptConnection connection)
    {
        super(connection);
    }

    /**
     * Evaluates an expression represented by a parse tree. The parse tree
     * must not contain any non-standard java syntax.
     */
    public Object evaluate(
        ClassDeclaration decl,
        ParseTree parseTree,
        Argument [] arguments)
    {
        BoundMethod thunk = compileAndBind(decl, parseTree, arguments);
        try {
            return thunk.call();
        } catch (IllegalAccessException e) {
            throw Toolbox.newInternal(e);
        } catch (InvocationTargetException e) {
            throw Toolbox.newInternal(e);
        }
    }

    /**
     * Executes a query string, passing in a set of arguments, and returns the
     * result.
     *
     * <p>
     * Example:
     * <blockquote>
     * <pre>Sales sales = new Sales("jdbc:odbc:Northwind");
     * int maxSalary = 10000;
     * Statement statement = new Statement();
     * statement.execute(
     *     "select &#42; from sales.emp where emp.sal > maxSalary",
     *     this.class,
     *     new Argument[] {
     *         new Argument("sales", sales),
     *         new Argument("maxSalary", maxSalary)});</pre>
     * </blockquote>
     * </p>
     *
     * @param queryString expression to execute. (Although this method is
     *        intended to allow execution of relational expressions,
     *        <code>queryString</code> can actually be any valid Java
     *        expression, for example <code>1 + 2</code>.)
     * @param arguments a set of name/value pairs to pass to the expression.
     *
     * @return the result of evaluating the expression. If the expression is
     *         relational, the return is a {@link java.util.Enumeration}
     */
    public Object execute(
        String queryString,
        Argument [] arguments)
    {
        // (re)load trace level etc. from saffron.properties
        if (shouldReloadTrace()) {
            SaffronProperties.instance().apply();
        }
        ClassDeclaration decl = init(arguments);
        ParseTree parseTree = parse(queryString);
        OJQueryExpander queryExpander = new OJQueryExpander(env, connection);
        parseTree = validate(parseTree, queryExpander);
        return evaluate(decl, parseTree, arguments);
    }

    public ResultSet executeSql(String queryString)
    {
        PreparedResult plan = prepareSql(queryString);
        assert (!plan.isDml());
        return (ResultSet) plan.execute();
    }

    protected void initSub()
    {
        // Ensure that the thread has factories for types and planners. (We'd
        // rather that the client sets these.)
        setupFactories();

        // Register the connection so that it can be retrieved by the generated
        // java class (provided it runs inside the same JVM). By default, the
        // connection is in the variable "connection". But don't assign
        // connectionInfo.jdbcExprFunctor; someone else may have set it.
        if (shouldSetConnectionInfo()) {
            final OJConnectionRegistry.ConnectionInfo connectionInfo =
                OJConnectionRegistry.instance.get(connection, true);
            connectionInfo.expr = new Variable(connectionVariable);
            connectionInfo.env = env;
        }
    }

    public static void setupFactories()
    {
        RelDataTypeFactory typeFactory =
            RelDataTypeFactoryImpl.threadInstance();
        if (typeFactory == null) {
            typeFactory = new OJTypeFactoryImpl();
            RelDataTypeFactoryImpl.setThreadInstance(typeFactory);
        }
        if (OJPlannerFactory.threadInstance() == null) {
            OJPlannerFactory.setThreadInstance(new OJPlannerFactory());
        }
    }

    public Expression parse(String queryString)
    {
        try {
            return PartialParser.makeExpression(env, "(" + queryString + ")");
        } catch (MOPException e) {
            throw Util.newInternal(e, "while parsing [" + queryString + "]");
        }
    }

    /**
     * Prepares a statement for execution, starting from a SQL string and
     * using the standard validator.
     */
    public PreparedResult prepareSql(String queryString)
    {
        SqlParser parser = new SqlParser(queryString);
        final SqlNode sqlQuery;
        try {
            sqlQuery = parser.parseStmt();
        } catch (ParseException e) {
            throw Util.newInternal(e,
                "Error while parsing SQL '" + queryString + "'");
        }
        RelOptSchema schema = connection.getRelOptSchema();
        SqlValidator.CatalogReader catalogReader;
        if (schema instanceof SqlValidator.CatalogReader) {
            catalogReader = (SqlValidator.CatalogReader) schema;
        } else {
            catalogReader =
                new SqlToOpenjavaConverter.SchemaCatalogReader(schema, false);
        }
        setupFactories();
        final SqlValidator validator =
            new SqlValidator(
                SqlOperatorTable.instance(),
                catalogReader,
                schema.getTypeFactory());
        return prepareSql(sqlQuery, null, validator, true);
    }

    // implement OJPreparingStmt
    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        final RelOptConnection connection)
    {
        return new SqlToRelConverter(
            validator,
            connection.getRelOptSchema(),
            env,
            OJPlannerFactory.threadInstance().newPlanner(),
            connection,
            new JavaRexBuilder(connection.getRelOptSchema().getTypeFactory()));
    }

    // implement OJPreparingStmt
    protected JavaRelImplementor getRelImplementor(RexBuilder rexBuilder)
    {
        return new JavaRelImplementor(rexBuilder);
    }

    /**
     * Validates and transforms an expression: expands schemas, validates, and
     * possibly expands queries.
     */
    public ParseTree validate(
        ParseTree parseTree,
        QueryExpander queryExpander)
    {
        MemberAccessCorrector corrector = new MemberAccessCorrector(env);
        parseTree = Util.go(corrector, parseTree);
        OJSchemaExpander schemaExpander = new OJSchemaExpander(env);
        parseTree = Util.go(schemaExpander, parseTree);
        OJValidator validator = new OJValidator(env);
        parseTree = Util.go(validator, parseTree);
        if (queryExpander == null) {
            return parseTree;
        }
        try {
            parseTree = Util.go(queryExpander, parseTree);
        } catch (Throwable e) {
            throw Util.newInternal(e,
                "while validating parse tree " + parseTree);
        }
        return parseTree;
    }

    // implement OJPreparingStmt
    protected String getClassRoot()
    {
        return SaffronProperties.instance().classDir.get(true);
    }

    // implement OJPreparingStmt
    protected String getCompilerClassName()
    {
        return SaffronProperties.instance().javaCompilerClass.get();
    }

    // implement OJPreparingStmt
    protected String getJavaRoot()
    {
        return SaffronProperties.instance().javaDir.get(true);
    }

    // implement OJPreparingStmt
    protected String getTempPackageName()
    {
        return SaffronProperties.instance().packageName.get();
    }

    // implement OJPreparingStmt
    protected String getTempMethodName()
    {
        return "dummy";
    }

    // implement OJPreparingStmt
    protected String getTempClassName()
    {
        return "Dummy_"
        + Integer.toHexString(this.hashCode() + executionCount++);
    }

    // implement OJPreparingStmt
    protected boolean shouldAlwaysWriteJavaFile()
    {
        return false;
    }

    // implement OJPreparingStmt
    protected boolean shouldSetConnectionInfo()
    {
        return false;
    }

    // implement OJPreparingStmt
    protected boolean shouldReloadTrace()
    {
        return true;
    }
}


// End OJStatement.java
