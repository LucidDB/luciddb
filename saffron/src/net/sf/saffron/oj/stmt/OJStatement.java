/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2005 Disruptive Tech
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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.util.logging.Logger;

import net.sf.saffron.oj.OJConnectionRegistry;
import net.sf.saffron.oj.OJPlannerFactory;
import net.sf.saffron.oj.OJValidator;
import net.sf.saffron.oj.xlat.OJQueryExpander;
import net.sf.saffron.oj.xlat.OJSchemaExpander;
import net.sf.saffron.oj.xlat.SqlToOpenjavaConverter;
import net.sf.saffron.trace.SaffronTrace;

import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.*;
import openjava.tools.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.SqlParseException;
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
            throw Util.newInternal(e);
        } catch (InvocationTargetException e) {
            throw Util.newInternal(e);
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
        reloadTrace();
        ClassDeclaration decl = init(arguments);
        ParseTree parseTree = parse(queryString);
        OJQueryExpander queryExpander = new OJQueryExpander(env, connection);
        parseTree = validate(parseTree, queryExpander);
        return evaluate(decl, parseTree, arguments);
    }

    private void reloadTrace()
    {
        SaffronProperties props = SaffronProperties.instance();
        int debugLevel = props.debugLevel.get();
        String debugOut = props.debugOut.get();
        if (debugLevel >= 0) {
            DebugOut.setDebugLevel(debugLevel);
            if ((debugOut == null) || debugOut.equals("")) {
                debugOut = "out";
            }
        }
        if ((debugOut != null) && !debugOut.equals("")) {
            if (debugOut.equals("err")) {
                DebugOut.setDebugOut(System.err);
            } else if (debugOut.equals("out")) {
                DebugOut.setDebugOut(System.out);
            } else {
                try {
                    File file = new File(debugOut);
                    PrintStream ps =
                        new PrintStream(new FileOutputStream(file), true);
                    DebugOut.setDebugOut(ps);
                } catch (FileNotFoundException e) {
                    throw Util.newInternal(e, "while setting debug output");
                }
            }
        }
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
        OJTypeFactory typeFactory = OJUtil.threadTypeFactory();
        if (typeFactory == null) {
            typeFactory = new OJTypeFactoryImpl();
            OJUtil.setThreadTypeFactory(typeFactory);
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
        } catch (SqlParseException e) {
            throw Util.newInternal(e,
                "Error while parsing SQL '" + queryString + "'");
        }
        RelOptSchema schema = connection.getRelOptSchema();
        SqlValidatorCatalogReader catalogReader;
        if (schema instanceof SqlValidatorCatalogReader) {
            catalogReader = (SqlValidatorCatalogReader) schema;
        } else {
            catalogReader =
                new SqlToOpenjavaConverter.SchemaCatalogReader(schema, false);
        }
        setupFactories();
        final SqlValidator validator =
            SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
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
        return new JavaRelImplementor(rexBuilder, null);
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
        parseTree = OJUtil.go(corrector, parseTree);
        OJSchemaExpander schemaExpander = new OJSchemaExpander(env);
        parseTree = OJUtil.go(schemaExpander, parseTree);
        OJValidator validator = new OJValidator(env);
        parseTree = OJUtil.go(validator, parseTree);
        if (queryExpander == null) {
            return parseTree;
        }
        try {
            parseTree = OJUtil.go(queryExpander, parseTree);
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

    // override OJPreparingStmt
    public PreparedResult prepareSql(
        SqlNode sqlQuery,
        Class runtimeContextClass,
        SqlValidator validator,
        boolean needValidation)
    {
        reloadTrace();
        return super.prepareSql(
            sqlQuery,runtimeContextClass,validator,needValidation);
    }

    // override OJPreparingStmt
    protected void bindArgument(Argument arg)
    {
        env.bindVariable(arg.getName(),new ArgumentInfo(arg));
    }

    // override OJPreparingStmt
    protected void addDecl(
        openjava.ptree.Statement statement,
        ExpressionList exprList)
    {
        if (exprList == null) {
            return;
        }
        if (statement instanceof VariableDeclaration) {
            VariableDeclaration varDecl = (VariableDeclaration) statement;
            TypeName typeSpecifier = varDecl.getTypeSpecifier();
            String qname = env.toQualifiedName(typeSpecifier.getName());
            OJClass clazz =
                env.lookupClass(
                    qname,
                    typeSpecifier.getDimension());
            String varName = varDecl.getVariable();

            // return new VarDecl[] {
            //   new VarDecl("s", "java.lang.String", s),
            //   new VarDecl("i", "int", new Integer(i))};
            exprList.add(
                new AllocationExpression(
                    OJClass.forClass(VarDecl.class),
                    new ExpressionList(
                        Literal.makeLiteral(varName),
                        new FieldAccess(
                            TypeName.forOJClass(clazz),
                            "class"),
                        OJUtil.box(
                            clazz,
                            new Variable(varDecl.getVariable())))));
        }
    }

    private static class ArgumentInfo implements Environment.VariableInfo
    {
        private Argument arg;

        ArgumentInfo(Argument arg)
        {
            this.arg = arg;
        }

        public OJClass getType()
        {
            return arg.getType();
        }

        public RelOptSchema getRelOptSchema()
        {
            if (arg.getValue() == null) {
                return null;
            } else if (arg.getValue() instanceof RelOptSchema) {
                return (RelOptSchema) arg.getValue();
            } else if (arg.getValue() instanceof RelOptConnection) {
                return ((RelOptConnection) arg.getValue()).getRelOptSchema();
            } else {
                return null;
            }
        }
    }
}


// End OJStatement.java
