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

import com.disruptivetech.farrago.volcano.VolcanoPlannerFactory;

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
public class OJStatement
{
    public static final String connectionVariable = "connection";
    private static final Logger tracer = SaffronTrace.getStatementTracer();
    private String queryString_ = null;
    protected Environment env;
    int executionCount = 0;

    /** CallingConvention via which results should be returned by execution. */
    private CallingConvention resultCallingConvention;

    /**
     * Share the same compiler between multiple statements.
     *
     * <p>When we used DynamicJava this was important, because DynamicJava
     * has a class loader which caches class definitions. This may no longer
     * be the case.
     */
    protected JavaCompiler javaCompiler;
    private final RelOptConnection connection;

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
        this.connection =
            ((connection == null) && this instanceof RelOptConnection)
            ? (RelOptConnection) this : connection;
        Util.pre(this.connection != null,
            "connection != null || this instanceof RelOptConnection");
        this.resultCallingConvention = CallingConvention.RESULT_SET;
    }

    public Environment getEnvironment()
    {
        return env;
    }

    public void setResultCallingConvention(
        CallingConvention resultCallingConvention)
    {
        this.resultCallingConvention = resultCallingConvention;
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

    private BoundMethod compileAndBind(
        ClassDeclaration decl,
        ParseTree parseTree,
        Argument [] arguments)
    {
        BoundMethod thunk = compile(decl, env, parseTree, arguments);
        Object [] args = new Object[thunk.parameterNames.length];
        for (int i = 0; i < thunk.parameterNames.length; i++) {
            String parameterName = thunk.parameterNames[i];
            Argument argument = null;
            for (int j = 0; j < arguments.length; j++) {
                if (arguments[j].name.equals(parameterName)) {
                    argument = arguments[j];
                    break;
                }
            }
            if (argument == null) {
                throw Toolbox.newInternal("variable '" + parameterName
                    + "' not found");
            }
            args[i] = argument.value;
        }
        thunk.args = args;
        return thunk;
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

    public ClassDeclaration init(Argument [] arguments)
    {
        env = OJSystem.env;

        // compiler and class map must have same life-cycle, because
        // DynamicJava's compiler contains a class loader
        if (ClassMap.instance() == null) {
            ClassMap.setInstance(new ClassMap(SyntheticObject.class));
        }
        javaCompiler = createCompiler();
        String packageName = getTempPackageName();
        String className = getTempClassName();
        env = new FileEnvironment(env, packageName, className);
        ClassDeclaration decl =
            new ClassDeclaration(new ModifierList(ModifierList.PUBLIC),
                className, null, null, new MemberDeclarationList());
        OJClass clazz = new OJClass(env, null, decl);
        env.record(
            clazz.getName(),
            clazz);
        env = new ClosedEnvironment(clazz.getEnvironment());

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

        OJUtil.threadDeclarers.set(clazz);
        if ((arguments != null) && (arguments.length > 0)) {
            for (int i = 0; i < arguments.length; i++) {
                final Argument argument = arguments[i];
                if (argument.value instanceof Enumeration) {
                    argument.value =
                        new EnumerationIterator((Enumeration) argument.value);
                    argument.clazz = argument.value.getClass();
                }
                if (argument.value instanceof Iterator
                        && !(argument.value instanceof Iterable)) {
                    argument.value =
                        new BufferedIterator((Iterator) argument.value);
                    argument.clazz = argument.value.getClass();
                }
                if (RelOptConnection.class.isInstance(argument.value)) {
                    // Don't fix up the type of connections. (The mapping to
                    // schema is made via static typing, so changing the type
                    // destroys the mapping.)
                } else {
                    // If the argument's type is a private class, change its
                    // type to the nearest base class which is public. Otherwise
                    // the generated code won't compile.
                    argument.clazz =
                        visibleBaseClass(argument.clazz, packageName);
                }
                env.bindVariable(argument.name, argument);
            }
        }
        return decl;
    }

    public static void setupFactories()
    {
        RelDataTypeFactory typeFactory =
            RelDataTypeFactoryImpl.threadInstance();
        if (typeFactory == null) {
            typeFactory = new OJTypeFactoryImpl();
            RelDataTypeFactoryImpl.setThreadInstance(typeFactory);
        }
        if (VolcanoPlannerFactory.threadInstance() == null) {
            VolcanoPlannerFactory.setThreadInstance(new OJPlannerFactory());
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

    /**
     * Prepares a statement for execution, starting from a parse tree and
     * using a user-supplied validator.
     */
    public PreparedResult prepareSql(
        SqlNode sqlQuery,
        Class runtimeContextClass,
        SqlValidator validator,
        boolean needValidation)
    {
        queryString_ = sqlQuery.toString();

        // (re)load trace level etc. from saffron.properties
        if (shouldReloadTrace()) {
            SaffronProperties.instance().apply();
        }

        if (runtimeContextClass == null) {
            runtimeContextClass = connection.getClass();
        }

        final Argument [] arguments =
            new Argument [] {
                new Argument(connectionVariable, runtimeContextClass,
                    connection)
            };
        ClassDeclaration decl = init(arguments);

        boolean explain = false;
        boolean explainWithImplementation = false;
        if (sqlQuery.isA(SqlKind.Explain)) {
            explain = true;

            // dig out the underlying SQL statement
            SqlExplain sqlExplain = (SqlExplain) sqlQuery;
            sqlQuery = sqlExplain.getExplicandum();
            explainWithImplementation = sqlExplain.withImplementation();
        }

        SqlToRelConverter sqlToRelConverter =
            getSqlToRelConverter(validator, connection);
        RelNode rootRel;
        if (needValidation) {
            rootRel = sqlToRelConverter.convertQuery(sqlQuery);
        } else {
            rootRel = sqlToRelConverter.convertValidatedQuery(sqlQuery);
        }

        if (explain && !explainWithImplementation) {
            return new PreparedExplanation(rootRel);
        }

        rootRel = optimize(rootRel);
        if (explain) {
            return new PreparedExplanation(rootRel);
        }
        return implement(
            rootRel,
            sqlQuery.getKind(),
            decl,
            arguments);
    }

    /** optimize a query plan.
     * @param rootRel root of a relational expression
     * @return an equivalent optimized relational expression
     */
    private RelNode optimize(RelNode rootRel)
    {
        RelOptPlanner planner = rootRel.getCluster().getPlanner();
        planner.setRoot(rootRel);
        rootRel = planner.changeConvention(rootRel, resultCallingConvention);
        assert (rootRel != null);
        planner.setRoot(rootRel);
        planner = planner.chooseDelegate();
        rootRel = planner.findBestExp();
        assert (rootRel != null) : "could not implement exp";
        return rootRel;
    }

    /** implement a physical query plan.
     * @param rootRel root of the relational expression.
     * @param sqlKind SqlKind of the original statement.
     * @param decl ClassDeclaration of the generated result.
     * @param args argument list of the generated result.
     * @return an executable plan, a {@link PreparedExecution}.
     */
    private PreparedExecution implement(
        RelNode rootRel,
        SqlKind sqlKind,
        ClassDeclaration decl,
        Argument [] args)
    {
        JavaRelImplementor relImplementor =
            getRelImplementor(rootRel.getCluster().rexBuilder);
        Expression expr = relImplementor.implementRoot((JavaRel) rootRel);
        boolean isDml = sqlKind.isA(SqlKind.Dml);
        ParseTree parseTree = expr;
        BoundMethod boundMethod = compileAndBind(decl, parseTree, args);
        final PreparedExecution plan =
            new PreparedExecution(parseTree,
                rootRel.getRowType(), isDml, boundMethod);
        return plan;
    }

    /**
     * Prepares a statement for execution, starting from a relational expression
     * (ie a logical or a physical query plan).
     * @param rootRel root of the relational expression.
     * @param sqlKind SqlKind for the relational expression: only
     *   SqlKind.Explain and SqlKind.Dml are special cases.
     * @param needOpt true for a logical query plan (still needs to be
     *   optimized), false for a physical plan.
     * @param decl openjava ClassDeclaration for the code generated to implement the
     *   statement.
     * @param args openjava argument list for the generated code.
     */
    public PreparedResult prepareSql(
        RelNode rootRel,
        SqlKind sqlKind,
        boolean needOpt,
        ClassDeclaration decl,
        Argument [] args)
    {
        if (needOpt) {
            rootRel = optimize(rootRel);
        }
        return implement(rootRel, sqlKind, decl, args);
    }

    /**
     * Protected method to allow subclasses to override construction of
     * SqlToRelConverter.
     */
    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        final RelOptConnection connection)
    {
        return new SqlToRelConverter(
            validator,
            connection.getRelOptSchema(),
            env,
            connection,
            new JavaRexBuilder(connection.getRelOptSchema().getTypeFactory()));
    }

    /**
     * Protected method to allow subclasses to override construction of
     * JavaRelImplementor.
     */
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

    protected String getClassRoot()
    {
        return SaffronProperties.instance().classDir.get(true);
    }

    protected String getCompilerClassName()
    {
        return SaffronProperties.instance().javaCompilerClass.get();
    }

    protected String getJavaRoot()
    {
        return SaffronProperties.instance().javaDir.get(true);
    }

    protected String getTempPackageName()
    {
        return SaffronProperties.instance().packageName.get();
    }

    protected String getTempMethodName()
    {
        return "dummy";
    }

    protected String getTempClassName()
    {
        return "Dummy_"
        + Integer.toHexString(this.hashCode() + executionCount++);
    }

    protected boolean shouldAlwaysWriteJavaFile()
    {
        return false;
    }

    protected boolean shouldSetConnectionInfo()
    {
        return false;
    }

    protected boolean shouldReloadTrace()
    {
        return true;
    }

    private JavaCompiler createCompiler()
    {
        String compilerClassName = getCompilerClassName();
        try {
            Class compilerClass = Class.forName(compilerClassName);
            return (JavaCompiler) compilerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        } catch (InstantiationException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        } catch (ClassCastException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        }
    }

    /**
     * Returns the lowest ancestor of <code>clazz</code> which is visible from
     * <code>fromPackage</code>&#46;<code>fromClazz</code>.
     */
    private static Class visibleBaseClass(
        Class clazz,
        String fromPackageName)
    {
        //		String fromClassFullName;
        //		if (fromPackageName == null || fromPackageName.equals("")) {
        //			fromClassFullName = fromClassName;
        //		} else {
        //			fromClassFullName = fromPackageName + "." + fromClassFullName;
        //		}
        for (Class c = clazz; c != null; c = c.getSuperclass()) {
            int modifiers = c.getModifiers();
            if (Modifier.isPublic(modifiers)) {
                return c;
            }
            Package pakkage = c.getPackage();
            if (pakkage == null) {
                pakkage = Object.class.getPackage();
            }
            if (!Modifier.isPrivate(modifiers)
                    && pakkage.getName().equals(fromPackageName)) {
                return c;
            }
        }
        return java.lang.Object.class;
    }

    private void addDecl(
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
                        Util.box(
                            clazz,
                            new Variable(varDecl.getVariable())))));
        }
    }

    private BoundMethod compile(
        ClassDeclaration decl,
        Environment env,
        ParseTree parseTree,
        Argument [] arguments)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.log(
                Level.FINE,
                "Before compile, parse tree",
                new Object [] { parseTree });
        }
        ClassCollector classCollector = new ClassCollector(env);
        Util.discard(Util.go(classCollector, parseTree));
        OJClass [] classes = classCollector.getClasses();
        SyntheticClass.addMembers(decl, classes);

        // NOTE jvs 14-Jan-2004:  DynamicJava doesn't correctly handle
        // the FINAL modifier on parameters.  So I made the codegen
        // for the method body copy the parameter to a final local
        // variable instead.  The only side-effect is that the parameter
        // names in the method signature is different.
        // TODO jvs 28-June-2004:  get rid of this if DynamicJava
        // gets tossed
        // form parameter list
        String [] parameterNames = new String[arguments.length];
        String [] javaParameterNames = new String[arguments.length];
        Class [] parameterTypes = new Class[arguments.length];
        OJClass [] parameterOjTypes = new OJClass[arguments.length];
        ExpressionList returnDeclList = new ExpressionList();
        for (int i = 0; i < arguments.length; i++) {
            parameterNames[i] = arguments[i].name;
            javaParameterNames[i] = arguments[i].name + "_p";
            parameterTypes[i] = arguments[i].clazz;
            parameterOjTypes[i] = OJClass.forClass(arguments[i].clazz);
        }

        // form the body of the method, and figure out the return type
        OJClass returnType = Util.clazzVoid;
        StatementList statementList = new StatementList();
        for (int i = 0; i < arguments.length; ++i) {
            statementList.add(
                new VariableDeclaration(
                    new ModifierList(ModifierList.FINAL),
                    TypeName.forOJClass(parameterOjTypes[i]),
                    arguments[i].name,
                    new FieldAccess(javaParameterNames[i])));
        }
        if (parseTree instanceof Expression) {
            Expression expression = (Expression) parseTree;
            returnType = Util.getType(env, expression);
            if (!returnType.isPrimitive()) {
                returnType = Util.clazzObject;
            }
            openjava.ptree.Statement statement;
            if (returnType == OJSystem.VOID) {
                statement = new ExpressionStatement(expression);
            } else {
                statement = new ReturnStatement(expression);
            }
            statementList.add(statement);
            returnDeclList = null;
        } else if (parseTree instanceof openjava.ptree.Statement) {
            openjava.ptree.Statement statement =
                (openjava.ptree.Statement) parseTree;
            statementList.add(statement);
            addDecl(statement, returnDeclList);
        } else if (parseTree instanceof StatementList) {
            StatementList newList = (StatementList) parseTree;
            for (int i = 0, count = newList.size(); i < count; i++) {
                addDecl(
                    newList.get(i),
                    returnDeclList);
            }
            statementList.addAll(newList);
        } else {
            throw Util.newInternal("cannot handle a " + parseTree.getClass());
        }
        if (returnDeclList != null) {
            statementList.add(
                new ReturnStatement(
                    new ArrayAllocationExpression(
                        OJClass.forClass(VarDecl.class),
                        new ExpressionList(null),
                        new ArrayInitializer(returnDeclList))));
            returnType = OJClass.arrayOf(OJClass.forClass(VarDecl.class));
        }
        SyntheticClass.addMethod(
            decl,
            statementList,
            getTempMethodName(),
            javaParameterNames,
            parameterOjTypes,
            returnType);
        String packageName = getTempPackageName();
        CompilationUnit compUnit =
            new CompilationUnit(packageName, new String[0],
                new ClassDeclarationList(decl));

        if (queryString_ != null) {
            compUnit.setComment("/** "
                + queryString_.replaceAll("\n", "\n// ") + "\n */");
        }
        String s;

        // TODO jvs 28-June-2004:  get rid of this if DynamicJava
        // gets tossed
        // hack because DynamicJava cannot resolve fully-qualified inner
        // class names such as "saffron.runtime.Dummy_389838.Ojp_0",
        // and needs dollar signs to help it, but the real Java compiler
        // is strict and does not accept the dollar signs
        if (getCompilerClassName().equals("openjava.ojc.DynamicJavaCompiler")) {
            s = generateDynamicJavaCode(compUnit);
        } else {
            s = compUnit.toString();
        }
        String className = decl.getName();
        packageName = compUnit.getPackage(); // e.g. "abc.def", or null
        return compile(packageName, className, s, parameterTypes,
            parameterNames);
    }

    private String generateDynamicJavaCode(CompilationUnit compUnit)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        SourceCodeWriter writer =
            new SourceCodeWriter(pw) {
                public void visit(TypeName p)
                    throws ParseTreeException
                {
                    out.print(p.getName());
                    int dims = p.getDimension();
                    out.print(TypeName.stringFromDimension(dims));
                }
            };
        try {
            compUnit.accept(writer);
        } catch (ParseTreeException e) {
            throw Util.newInternal(e);
        }
        pw.close();
        return sw.toString();
    }

    private BoundMethod compile(
        String packageName,
        String className,
        String s,
        Class [] parameterTypes,
        String [] parameterNames)
    {
        JavaCompilerArgs args = javaCompiler.getArgs();
        args.clear();
        String initialArgs =
            SaffronProperties.instance().javaCompilerArgs.get();
        if (initialArgs != null) {
            args.setString(initialArgs);
        }
        String fullClassName;
        if (packageName == null) {
            fullClassName = className;
        } else {
            fullClassName = packageName + "." + className;
        }
        String javaFileName =
            Util.replace(fullClassName, ".", Util.fileSeparator) + ".java";
        File javaRoot = new File(getJavaRoot());
        File javaFile = new File(javaRoot, javaFileName);

        boolean writeJavaFile = shouldAlwaysWriteJavaFile();
        javaCompiler.getArgs().setDestdir(javaRoot.getAbsolutePath());
        javaCompiler.getArgs().setFullClassName(fullClassName);
        if (javaCompiler.getArgs().supportsSetSource()) {
            javaCompiler.getArgs().setSource(
                s,
                javaFile.toString());
        } else {
            writeJavaFile = true;
            args.addFile(javaFile.toString());
        }

        if (writeJavaFile) {
            try {
                javaFile.getParentFile().mkdirs(); // make any necessary parent directories
                final boolean print =
                    SaffronProperties.instance().printBeforeCompile.get();
                if (print) {
                    System.out.println("Compiling " + javaFile + ", class "
                        + fullClassName);
                }
                FileWriter fw = new FileWriter(javaFile);
                fw.write(s);
                fw.close();
            } catch (java.io.IOException e2) {
                throw Util.newInternal(e2,
                    "while writing java file '" + javaFile + "'");
            }
        }

        javaCompiler.compile();
        try {
            Class clazz =
                Class.forName(
                    fullClassName,
                    true,
                    javaCompiler.getClassLoader());
            Object o = clazz.newInstance();
            Method method =
                clazz.getDeclaredMethod(
                    getTempMethodName(),
                    parameterTypes);
            return new BoundMethod(o, method, parameterNames);
        } catch (ClassNotFoundException e) {
            throw Toolbox.newInternal(e);
        } catch (InstantiationException e) {
            throw Toolbox.newInternal(e);
        } catch (IllegalAccessException e) {
            throw Toolbox.newInternal(e);
        } catch (NoSuchMethodException e) {
            throw Toolbox.newInternal(e);
        }
    }

    interface Binder
    {
        void declareClass(ClassDeclaration cdecl);

        void declareVariable(
            String name,
            OJClass clazz,
            Object value);
    }

    /**
     * An <code>Argument</code> supplies a name/value pair to a statement. The
     * class of the argument is usually superfluous, but is necessary when
     * the value is a primitive type (such as <code>int</code>, as opposed to
     * {@link Integer}), or is a superclass of the object's runtime type.
     */
    public static class Argument implements Environment.VariableInfo
    {
        Class clazz;
        Object value;
        String name;

        /**
         * Creates an argument.
         */
        public Argument(
            String name,
            Class clazz,
            Object value)
        {
            this.name = name;
            this.clazz = clazz;
            this.value = value;
        }

        /**
         * Creates an argument whose type is the runtime type of
         * <code>value</code>.
         */
        public Argument(
            String name,
            Object value)
        {
            this.name = name;
            this.clazz = value.getClass();
            this.value = value;
        }

        /**
         * Creates an <code>int</code> argument.
         */
        public Argument(
            String name,
            int value)
        {
            this(name, java.lang.Integer.TYPE, new Integer(value));
        }

        public RelOptSchema getRelOptSchema()
        {
            if (value == null) {
                return null;
            } else if (value instanceof RelOptSchema) {
                return (RelOptSchema) value;
            } else if (value instanceof RelOptConnection) {
                return ((RelOptConnection) value).getRelOptSchema();
            } else {
                return null;
            }
        }

        public OJClass getType()
        {
            return OJClass.forClass(clazz);
        }

        public Object getValue()
        {
            return value;
        }
    }
}


// End OJStatement.java
