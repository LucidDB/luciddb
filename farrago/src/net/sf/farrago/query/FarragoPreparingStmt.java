/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.query;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql2rel.*;
import org.eigenbase.util.*;

import java.util.List;
import java.lang.reflect.Modifier;

/**
 * FarragoPreparingStmt subclasses OJPreparingStmt to implement the
 * {@link FarragoSessionPreparingStmt} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPreparingStmt extends OJPreparingStmt
    implements FarragoSessionPreparingStmt,
        RelOptConnection,
        RelOptSchema,
        SqlValidatorCatalogReader
{
    //~ Static fields/initializers --------------------------------------------

    // NOTE jvs 8-June-2004: this tracer is special in that it controls
    // preservation of dynamically generated Java code
    private static final Logger dynamicTracer =
        FarragoTrace.getDynamicTracer();
    private static final Logger streamGraphTracer =
        FarragoTrace.getPreparedStreamGraphTracer();
    private static final Logger planDumpTracer =
        FarragoTrace.getPlanDumpTracer();

    //~ Instance fields -------------------------------------------------------

    private final FarragoSessionStmtValidator stmtValidator;
    private boolean needRestore;
    private SqlToRelConverter sqlToRelConverter;
    private Object savedDeclarer;
    private FarragoAllocation javaCodeDir;
    private FarragoSqlValidator sqlValidator;
    private Set directDependencies;
    private Set allDependencies;
    private Set jarUrlSet;
    private SqlOperatorTable sqlOperatorTable;
    private final FarragoUserDefinedRoutineLookup routineLookup;
    private int expansionDepth;
    private RelDataType originalRowType;
    private SqlIdentifier dmlTarget;
    private PrivilegedAction dmlAction;

    /**
     * Name of Java package containing code generated for this statement.
     */
    private String packageName;

    /**
     * Directory containing code generated for this statement.
     */
    private File packageDir;

    /**
     * Root directory for all generated Java.
     */
    private File classesRoot;

    // attributes of the openjava code generated to implement the statement:
    private ClassDeclaration implementingClassDecl;
    private Argument [] implementingArgs;
    private Set loadedServerClassNameSet;
    private FarragoSessionPlanner planner;
    private FarragoRelImplementor relImplementor;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPreparingStmt object.
     *
     * @param stmtValidator generic stmt validator
     */
    public FarragoPreparingStmt(FarragoSessionStmtValidator stmtValidator)
    {
        super(null);

        this.stmtValidator = stmtValidator;
        stmtValidator.addAllocation(this);

        loadedServerClassNameSet = new HashSet();

        super.setResultCallingConvention(CallingConvention.ITERATOR);

        directDependencies = new HashSet();
        allDependencies = new HashSet();
        jarUrlSet = new LinkedHashSet();

        classesRoot = new File(FarragoProperties.instance().homeDir.get(true));
        classesRoot = new File(classesRoot, "classes");

        // Save some global state for reentrancy
        needRestore = true;
        savedDeclarer = OJUtil.threadDeclarers.get();
        OJSystem.env.pushThreadTempFrame();

        planner = getSession().getPersonality().newPlanner(this, true);
        getSession().getPersonality().definePlannerListeners(planner);

        routineLookup = new FarragoUserDefinedRoutineLookup(
            stmtValidator, this, null);

        clearDmlValidation();
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionPreparingStmt
    public boolean mayCacheImplementation() 
    {
        return true;
    }


    public FarragoSessionStmtValidator getStmtValidator()
    {
        return stmtValidator;
    }

    public void setPlanner(FarragoSessionPlanner planner)
    {
        this.planner = planner;
    }

    public FarragoSessionPlanner getPlanner()
    {
        return planner;
    }

    // implement FarragoSessionPreparingStmt
    public SqlOperatorTable getSqlOperatorTable()
    {
        if (sqlOperatorTable != null) {
            return sqlOperatorTable;
        }

        SqlOperatorTable systemOperators =
            getSession().getPersonality().getSqlOperatorTable(this);

        ChainedSqlOperatorTable table = new ChainedSqlOperatorTable();
        table.add(routineLookup);
        table.add(systemOperators);

        sqlOperatorTable = table;
        return sqlOperatorTable;
    }

    public boolean hasSqlValidator()
    {
        return sqlValidator != null;
    }
    
    // implement FarragoSessionPreparingStmt
    public SqlValidator getSqlValidator()
    {
        if (sqlValidator == null) {
            sqlValidator = new FarragoSqlValidator(this);
        }
        return sqlValidator;
    }

    // implement FarragoSessionPreparingStmt
    public FarragoSessionExecutableStmt prepare(SqlNode sqlNode)
    {
        boolean needValidation = false;
        if (sqlValidator == null) {
            getSqlValidator();
            needValidation = true;
        }

        definePackageName();
        PreparedResult preparedResult =
            super.prepareSql(
                sqlNode,
                getSession().getPersonality().getRuntimeContextClass(
                    this),
                sqlValidator,
                needValidation);
        return implement(preparedResult);
    }

    // implement FarragoSessionPreparingStmt
    public void preImplement()
    {
        definePackageName();
        implementingArgs =
            new Argument [] {
                new Argument(
                    connectionVariable,
                    getSession().getPersonality().getRuntimeContextClass(
                        this),
                    this)
            };
        implementingClassDecl = super.init(implementingArgs);
    }

    // implement FarragoSessionPreparingStmt
    public void postValidate(SqlNode sqlNode)
    {
        analyzeRoutineDependencies(sqlNode);
        
        // Now that we're done with validation, perform any deferred
        // privilege checks.
        stmtValidator.getPrivilegeChecker().checkAccess();
    }
    
    /**
     * Creates a new class declaration to be a container for generated code,
     * a public static inner class of {@link #implementingClassDecl}.
     *
     * @param base Stem for class name. For example, "IterTransform" might
     *    yield "IterTransform1".
     * @param baseClasses Array of base classes. May be null.
     * @param interfaces Array of interfaces. May be null.
     * @pre base != null
     * @return A new class declaration
     */
    public ClassDeclaration createClassDecl(
        String base,
        TypeName[] baseClasses,
        TypeName[] interfaces)
    {
        Util.pre(base != null, "base != null");
        String name = generateUniqueName(implementingClassDecl, base);
        final ClassDeclaration classDecl = new ClassDeclaration(
            new ModifierList(Modifier.PUBLIC | Modifier.STATIC),
            name,
            baseClasses,
            interfaces,
            new MemberDeclarationList(),
            true);
        implementingClassDecl.getBody().add(classDecl);
        return classDecl;
    }

    /**
     * Generates a unique name for a new member of a class.
     */
    private static String generateUniqueName(
        ClassDeclaration classDecl,
        String base)
    {
        for (int i = 1;; ++i) {
            String candidate = base + String.valueOf(i);
            if (!hasMember(classDecl, candidate)) {
                return candidate;
            }
        }
    }

    /**
     * Returns whether a class declaration has a member with a given name.
     */
    private static boolean hasMember(
        ClassDeclaration classDecl,
        String candidate)
    {
        final MemberDeclarationList declList = classDecl.getBody();
        for (int j = 0; j < declList.size(); j++) {
            MemberDeclaration memberDecl = (MemberDeclaration) declList.get(j);
            if (memberDecl instanceof ClassDeclaration) {
                String className = ((ClassDeclaration) memberDecl).getName();
                if (className.equals(candidate)) {
                    return true;
                }
            } else if (memberDecl instanceof FieldDeclaration) {
                String fieldName = ((FieldDeclaration) memberDecl).getName();
                if (fieldName.equals(candidate)) {
                    return true;
                }
            } else {
                // don't care about method names
            }
        }
        return false;
    }


    // implement FarragoSessionPreparingStmt
    public FarragoSessionExecutableStmt implement(
        RelNode rootRel,
        SqlKind sqlKind,
        boolean logical)
    {
        PreparedResult preparedResult =
            super.prepareSql(rootRel, sqlKind, logical, implementingClassDecl,
                implementingArgs);
        return implement(preparedResult);
    }

    /**
     * @return lookup table for user-defined routines
     */
    public FarragoUserDefinedRoutineLookup getRoutineLookup()
    {
        return routineLookup;
    }

    void addJarUrl(String jarUrl)
    {
        try {
            jarUrlSet.add(new URL(jarUrl));
        } catch (MalformedURLException ex) {
            // this shouldn't happen, because the caller is already
            // supposed to have verified the URL
            throw Util.newInternal();
        }
    }

    protected void prepareForCompilation()
    {
        // REVIEW jvs 20-Jan-2005: The idea here is to gather up all jars
        // referenced by external user-defined routines and provide them to the
        // classloader.  However, this loses the associations between jars and
        // routines, meaning if two classes in different jars have the same
        // name, there will be trouble.  The alternative is to always use
        // reflection, which would be bad for UDF performance.  What to do?
        // Also, need to implement jar paths.
        List jarUrlList = new ArrayList(jarUrlSet);
        URL [] urls = (URL []) jarUrlList.toArray(new URL[0]);
        URLClassLoader urlClassLoader = URLClassLoader.newInstance(
            urls, getSession().getPluginClassLoader());
        javaCompiler.getArgs().setClassLoader(urlClassLoader);
    }

    private void definePackageName()
    {
        // TODO:  once and only once
        packageDir = classesRoot;
        packageDir = new File(packageDir, "net");
        packageDir = new File(packageDir, "sf");
        packageDir = new File(packageDir, "farrago");
        packageDir = new File(packageDir, "dynamic");
        try {
            packageDir.mkdirs();
            packageDir = File.createTempFile("stmt", "", packageDir);
        } catch (IOException ex) {
            throw Util.newInternal(ex);
        }
        packageName = "net.sf.farrago.dynamic." + packageDir.getName();

        // Normally, we want to make sure all generated code gets cleaned up.
        // To disable this for debugging, you can explicitly set
        // net.sf.farrago.dynamic.level=FINE.  (This is not inherited via
        // parent logger.)
        if (!shouldAlwaysWriteJavaFile()) {
            javaCodeDir = new FarragoFileAllocation(packageDir);
        }

        // createTempFile created a normal file; we want a directory
        packageDir.delete();
        packageDir.mkdir();
    }

    protected FarragoSessionExecutableStmt implement(
        PreparedResult preparedResult)
    {
        FarragoSessionExecutableStmt executableStmt;
        if (preparedResult instanceof PreparedExecution) {
            PreparedExecution preparedExecution =
                (PreparedExecution) preparedResult;
            RelDataType rowType = preparedExecution.getRowType();
            OJClass ojRowClass =
                OJUtil.typeToOJClass(rowType, getFarragoTypeFactory());
            Class rowClass;
            try {
                String ojRowClassName = ojRowClass.getName();
                int i = ojRowClassName.lastIndexOf('.');
                assert (i != -1);
                ojRowClassName =
                    OJUtil.replaceDotWithDollar(ojRowClassName, i);
                rowClass =
                    Class.forName(
                        ojRowClassName,
                        true,
                        javaCompiler.getClassLoader());
            } catch (ClassNotFoundException ex) {
                throw Util.newInternal(ex);
            }

            RelDataType dynamicParamRowType = getParamRowType();

            String xmiFennelPlan = null;
            Set streamDefSet = relImplementor.getStreamDefSet();
            if (!streamDefSet.isEmpty()) {
                FemCmdPrepareExecutionStreamGraph cmdPrepareStream =
                    getRepos().newFemCmdPrepareExecutionStreamGraph();
                Collection streamDefs = cmdPrepareStream.getStreamDefs();
                streamDefs.addAll(streamDefSet);
                xmiFennelPlan =
                    JmiUtil.exportToXmiString(
                        Collections.singleton(cmdPrepareStream));
                streamGraphTracer.fine(xmiFennelPlan);
            }

            executableStmt =
                new FarragoExecutableJavaStmt(
                    packageDir,
                    rowClass,
                    (originalRowType == null) ? rowType : originalRowType,
                    dynamicParamRowType,
                    preparedExecution.getMethod(),
                    xmiFennelPlan,
                    preparedResult.isDml(),
                    getReferencedObjectIds());
        } else {
            assert (preparedResult instanceof PreparedExplanation);
            executableStmt =
                new FarragoExecutableExplainStmt(
                    getFarragoTypeFactory().createStructType(
                        new RelDataType[0],
                        new String[0]),
                    preparedResult.getCode());
        }

        // generated code is now the responsibility of executableStmt
        if (javaCodeDir != null) {
            executableStmt.addAllocation(javaCodeDir);
            javaCodeDir = null;
        }

        return executableStmt;
    }

    // implement FarragoSessionPreparingStmt
    public void analyzeSql(
        SqlNode sqlNode,
        final FarragoSessionAnalyzedSql analyzedSql)
    {
        getSqlToRelConverter();
        if (analyzedSql.paramRowType == null) {
            // query expression
            RelNode rootRel = sqlToRelConverter.convertValidatedQuery(sqlNode);
            analyzedSql.resultType = rootRel.getRowType();
            analyzedSql.paramRowType = getParamRowType();
        } else {
            // parameterized row expression
            analyzedSql.resultType =
                getSqlValidator().getValidatedNodeType(sqlNode);
        }
        analyzedSql.dependencies =
            Collections.unmodifiableSet(directDependencies);

        // walk the expression looking for dynamic parameters
        SqlVisitor dynamicParamFinder = new SqlBasicVisitor()
            {
                public void visit(SqlDynamicParam param)
                {
                    analyzedSql.hasDynamicParams = true;
                    super.visit(param);
                }
            };
        sqlNode.accept(dynamicParamFinder);
    }

    void analyzeRoutineDependencies(SqlNode sqlNode)
    {
        // Round up all the dependencies on UDF's.  We can't do this during
        // function lookup because overloads need to be resolved first.  And we
        // can't do this during SqlToRelConverter because then we stop
        // collecting direct dependencies.
        SqlVisitor udfInvocationFinder = new SqlBasicVisitor()
            {
                public void visit(SqlCall call)
                {
                    if (call.getOperator()
                        instanceof FarragoUserDefinedRoutine)
                    {
                        FarragoUserDefinedRoutine function =
                            (FarragoUserDefinedRoutine) call.getOperator();
                        addDependency(
                            function.getFemRoutine(),
                            PrivilegedActionEnum.EXECUTE);
                    }
                    super.visit(call);
                }
            };
        sqlNode.accept(udfInvocationFinder);
    }

    private Set getReferencedObjectIds()
    {
        Set set = new HashSet();
        Iterator iter = allDependencies.iterator();
        while (iter.hasNext()) {
            RefObject refObj = (RefObject) iter.next();
            set.add(refObj.refMofId());
        }
        return set;
    }

    // implement FarragoSessionPreparingStmt
    public SqlToRelConverter getSqlToRelConverter()
    {
        return getSqlToRelConverter(sqlValidator, this);
    }

    // implement FarragoSessionPreparingStmt
    public RelOptCluster getRelOptCluster()
    {
        return getSqlToRelConverter().getCluster();
    }

    // override OJPreparingStmt
    protected RelNode optimize(RelNode rootRel)
    {
        boolean dumpPlan = planDumpTracer.isLoggable(Level.FINE);
        if (dumpPlan) {
            planDumpTracer.fine(
                RelOptUtil.dumpPlan(
                    "Plan before flattening",
                    rootRel,
                    false));
        }
        originalRowType = rootRel.getRowType();
        rootRel = flattenTypes(rootRel, true);
        if (dumpPlan) {
            planDumpTracer.fine(
                RelOptUtil.dumpPlan(
                    "Plan after flattening",
                    rootRel,
                    false));
        }
        rootRel = super.optimize(rootRel);
        if (dumpPlan) {
            planDumpTracer.fine(
                RelOptUtil.dumpPlan(
                    "Plan after optimization",
                    rootRel,
                    false));
        }
        return rootRel;
    }

    RelNode flattenTypes(RelNode rootRel, boolean restructure)
    {
        RelStructuredTypeFlattener typeFlattener =
            new RelStructuredTypeFlattener(
                sqlToRelConverter.getRexBuilder());
        rootRel = typeFlattener.rewrite(rootRel, restructure);
        return rootRel;
    }

    protected RelDataType getParamRowType()
    {
        return getFarragoTypeFactory().createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return sqlToRelConverter.getDynamicParamCount();
                }

                public String getFieldName(int index)
                {
                    return "?" + index;
                }

                public RelDataType getFieldType(int index)
                {
                    return sqlToRelConverter.getDynamicParamType(index);
                }
            });
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (!needRestore) {
            // already closed or else never opened
            return;
        }
        OJSystem.env.popThreadTempFrame();
        OJUtil.threadDeclarers.set(savedDeclarer);

        // TODO:  obtain locks to ensure that objects we intend to operate
        // on don't change after we end repository txn.
        if (javaCodeDir != null) {
            javaCodeDir.closeAllocation();
            javaCodeDir = null;
        }
        needRestore = false;
    }

    RelNode expandView(String queryString)
    {
        expansionDepth++;

        FarragoSessionParser parser =
            getSession().getPersonality().newParser(getSession());
        final SqlNode sqlQuery;
        try {
            sqlQuery = (SqlNode) parser.parseSqlText(
                stmtValidator, null, queryString, true);
        } catch (Throwable e) {
            throw Util.newInternal(e,
                "Error while parsing view definition:  " + queryString);
        }
        RelNode relNode = sqlToRelConverter.convertQuery(sqlQuery);
        --expansionDepth;
        return relNode;
    }

    RexNode expandInvocationExpression(
        SqlNode sqlExpr,
        FarragoRoutineInvocation invocation)
    {
        expansionDepth++;

        // NOTE jvs 2-Jan-2005: We already validated the expression during DDL,
        // but we stored the original pre-validation expression, and validation
        // may have involved rewrites relied on by sqlToRelConverter.  So
        // we must recapitulate here.
        sqlExpr = getSqlValidator().validateParameterizedExpression(
            sqlExpr,
            invocation.getParamNameToTypeMap());

        // TODO jvs 1-Jan-2005: support a RexVariableBinding (like "let" in
        // Lisp), and avoid expansion of parameters which are referenced more
        // than once

        RexNode rexNode = sqlToRelConverter.convertExpression(
            sqlExpr, invocation.getParamNameToArgMap());
        --expansionDepth;
        return rexNode;
    }

    void setDmlValidation(SqlIdentifier target, PrivilegedAction action)
    {
        dmlTarget = target;
        dmlAction = action;
    }

    void clearDmlValidation()
    {
        dmlTarget = null;
        dmlAction = null;
    }

    /**
     * @return true iff currently expanding a view or function
     */
    public boolean isExpandingDefinition()
    {
        return expansionDepth > 0;
    }

    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        RelOptConnection connection)
    {
        // REVIEW:  recycling may be dangerous since SqlToRelConverter is
        // stateful
        if (sqlToRelConverter == null) {
            sqlToRelConverter =
                new SqlToRelConverter(
                    validator,
                    connection.getRelOptSchema(),
                    getEnvironment(),
                    planner,
                    connection,
                    new FarragoRexBuilder(this));
            sqlToRelConverter.setDefaultValueFactory(
                new ReposDefaultValueFactory());
            sqlToRelConverter.enableTableAccessConversion(false);
        }
        return sqlToRelConverter;
    }

    // implement FarragoSessionPreparingStmt
    public JavaRelImplementor getRelImplementor(RexBuilder rexBuilder)
    {
        if (relImplementor == null) {
            relImplementor = newRelImplementor(rexBuilder);
        }
        return relImplementor;
    }

    protected FarragoRelImplementor newRelImplementor(RexBuilder rexBuilder)
    {
        return new FarragoRelImplementor(this, rexBuilder);
    }

    // implement FarragoSessionPreparingStmt
    public FarragoRepos getRepos()
    {
        return stmtValidator.getRepos();
    }

    // implement FarragoSessionPreparingStmt
    public FennelDbHandle getFennelDbHandle()
    {
        return stmtValidator.getFennelDbHandle();
    }

    // implement FarragoSessionPreparingStmt
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return stmtValidator.getTypeFactory();
    }

    // implement FarragoSessionPreparingStmt
    public FarragoSessionIndexMap getIndexMap()
    {
        return stmtValidator.getIndexMap();
    }

    // implement FarragoSessionPreparingStmt
    public FarragoSession getSession()
    {
        return stmtValidator.getSession();
    }

    // implement RelOptConnection
    public RelOptSchema getRelOptSchema()
    {
        return this;
    }

    // implement RelOptConnection
    public Object contentsAsArray(
        String qualifier,
        String tableName)
    {
        throw new UnsupportedOperationException(
            "FarragoPreparingStmt.contentsAsArray() should have been replaced");
    }

    // implement RelOptSchema
    public RelOptTable getTableForMember(String [] names)
    {
        FarragoSessionResolvedObject resolved =
            stmtValidator.resolveSchemaObjectName(
                names,
                getRepos().getRelationalPackage().getCwmNamedColumnSet());

        if (resolved.object == null) {
            return getForeignTableFromNamespace(resolved);
        }

        assert (resolved.object instanceof CwmNamedColumnSet);

        CwmNamedColumnSet columnSet = (CwmNamedColumnSet) resolved.object;

        if (columnSet instanceof FemLocalTable) {
            FemLocalTable table = (FemLocalTable) columnSet;

            // REVIEW:  maybe defer this until physical implementation?
            if (table.isTemporary()) {
                getIndexMap().instantiateTemporaryTable(
                    stmtValidator.getDataWrapperCache(), table);
            }
        }

        RelOptTable relOptTable;
        if (columnSet instanceof FemBaseColumnSet) {
            FemBaseColumnSet table = (FemBaseColumnSet) columnSet;
            FemDataServer femServer = table.getServer();
            loadDataServerFromCache(femServer);
            relOptTable =
                stmtValidator.getDataWrapperCache().loadColumnSetFromCatalog(
                    table,
                    getFarragoTypeFactory());
        } else if (columnSet instanceof FemLocalView) {
            RelDataType rowType = createTableRowType(columnSet);
            relOptTable = new FarragoView(columnSet, rowType);
        } else {
            throw Util.needToImplement(columnSet);
        }
        initializeQueryColumnSet(relOptTable, columnSet);
        return relOptTable;
    }

    private void initializeQueryColumnSet(
        RelOptTable relOptTable,
        CwmNamedColumnSet cwmColumnSet)
    {
        if (relOptTable == null) {
            return;
        }
        if (!(relOptTable instanceof FarragoQueryColumnSet)) {
            return;
        }
        FarragoQueryColumnSet queryColumnSet =
            (FarragoQueryColumnSet) relOptTable;
        queryColumnSet.setPreparingStmt(this);
        queryColumnSet.setCwmColumnSet(cwmColumnSet);
    }

    private FarragoMedColumnSet getForeignTableFromNamespace(
        FarragoSessionResolvedObject resolved)
    {
        FemDataServer femServer =
            (FemDataServer) FarragoCatalogUtil.getModelElementByName(
                getRepos().getMedPackage().getFemDataServer().refAllOfType(),
                resolved.catalogName);
        if (femServer == null) {
            return null;
        }

        // TODO jvs 27-Aug-2005:  decide on required privileges for direct
        // access to foreign tables

        // When a foreign table is referenced directly via a namespace, we have
        // nothing to hang a direct dependency on.  Instead, we
        // remember the dependency on the server, so that if the server
        // gets dropped, dependent views will cascade.
        addDependency(femServer, null);

        FarragoMedDataServer server = loadDataServerFromCache(femServer);

        try {
            FarragoMedNameDirectory directory = server.getNameDirectory();
            if (directory == null) {
                return null;
            }
            directory = directory.lookupSubdirectory(resolved.schemaName);
            if (directory == null) {
                return null;
            }
            FarragoMedColumnSet medColumnSet =
                directory.lookupColumnSet(
                    getFarragoTypeFactory(),
                    resolved.objectName,
                    resolved.getQualifiedName());
            initializeQueryColumnSet(medColumnSet, null);
            return medColumnSet;
        } catch (Throwable ex) {
            // TODO:  better name formatting
            throw FarragoResource.instance().
                ValidatorForeignTableLookupFailed.ex(
                    Arrays.asList(resolved.getQualifiedName()).toString(),
                    ex);
        }
    }

    private FarragoMedDataServer loadDataServerFromCache(
        FemDataServer femServer)
    {
        FarragoMedDataServer server =
            stmtValidator.getDataWrapperCache().loadServerFromCatalog(
                femServer);
        if (loadedServerClassNameSet.add(server.getClass().getName())) {
            // This is the first time we've seen this server class, so give it
            // a chance to register any planner info such as calling
            // conventions and rules.  REVIEW: the discrimination is based on
            // class name, on the assumption that it should be unique regardless
            // of classloader, JAR, etc.  Is that correct?
            server.registerRules(planner);
        }
        return server;
    }

    // implement RelOptSchema
    public RelDataTypeFactory getTypeFactory()
    {
        return getFarragoTypeFactory();
    }

    // implement RelOptSchema
    public void registerRules(RelOptPlanner planner)
    {
        // nothing to do
    }

    // implement SqlValidator.CatalogReader
    public SqlValidatorTable getTable(String [] names)
    {
        FarragoSessionResolvedObject resolved =
            stmtValidator.resolveSchemaObjectName(
                names,
                getRepos().getRelationalPackage().getCwmNamedColumnSet());

        if (resolved == null) {
            return null;
        }

        if (resolved.object == null) {
            return getForeignTableFromNamespace(resolved);
        }

        if (!(resolved.object instanceof CwmNamedColumnSet)) {
            // TODO:  give a more helpful error
            // in case a non-relational object is referenced
            return null;
        }

        CwmNamedColumnSet table = (CwmNamedColumnSet) resolved.object;

        PrivilegedAction action = PrivilegedActionEnum.SELECT;
        if (dmlTarget != null) {
            if (Arrays.equals(names, dmlTarget.names)) {
                assert(dmlAction != null);
                action = dmlAction;

                // REVIEW jvs 27-Aug-2005:  This is a hack to handle the case
                // of self-insert, where the same table is both the source and
                // target.  We need to require SELECT for the source role
                // and INSERT for the target role.  It only works because
                // SqlValidatorImpl happens to validate the target first, so
                // this is very brittle.
                clearDmlValidation();
            }
        }

        addDependency(table, action);

        if (table.getVisibility() == null) {
            throw new FarragoUnvalidatedDependencyException();
        }

        RelDataType rowType = createTableRowType(table);
        SqlAccessType allowedAccess = FarragoCatalogUtil.getTableAllowedAccess(table);
        return newValidatorTable(resolved.getQualifiedName(), rowType, allowedAccess);
    }

    /**
     * Creates a row-type for a given table.
     * This row-type includes any system columns which are implicit for this
     * type of type.
     *
     * @param table Repository table
     * @return Row type including system columns
     */
    protected RelDataType createTableRowType(CwmNamedColumnSet table)
    {
        return getFarragoTypeFactory().createStructTypeFromClassifier(
            table);
    }

    /**
     * Factory method, creates a table.
     */
    protected SqlValidatorTable newValidatorTable(
        String[] qualifiedName,
        RelDataType rowType,
        SqlAccessType allowedAccess)
    {
        return new ValidatorTable(qualifiedName, rowType, allowedAccess);
    }

    // implement SqlValidator.CatalogReader
    public RelDataType getNamedType(SqlIdentifier typeName)
    {
        CwmSqldataType cwmType = stmtValidator.findSqldataType(typeName);
        if (!(cwmType instanceof FemSqlobjectType)) {
            // TODO jvs 12-Feb-2005:  throw an excn stating that only
            // user-defined structured type is allowed here
            return null;
        }
        // FIXME jvs 27-Aug-2005:  this should be USAGE, not REFERENCES;
        // need to add to FEM
        addDependency(cwmType, PrivilegedActionEnum.REFERENCES);
        return getFarragoTypeFactory().createCwmType(cwmType);
    }

    // implement SqlValidator.CatalogReader
    public SqlMoniker [] getAllSchemaObjectNames(String [] names)
    {
        return stmtValidator.getAllSchemaObjectNames(names);
    }

    public void addDependency(CwmModelElement supplier, PrivilegedAction action)
    {
        if (!isExpandingDefinition()) {
            directDependencies.add(supplier);
            if (action != null) {
                stmtValidator.requestPrivilege(
                    supplier,
                    action.toString());
            }
        }
        allDependencies.add(supplier);
    }

    public Variable getConnectionVariable()
    {
        return new Variable(connectionVariable);
    }

    // override OJPreparingStmt
    protected String getCompilerClassName()
    {
        return getRepos().getCurrentConfig().getJavaCompilerClassName();
    }

    // override OJPreparingStmt
    protected boolean shouldSetConnectionInfo()
    {
        return false;
    }

    // override OJPreparingStmt
    protected boolean shouldAlwaysWriteJavaFile()
    {
        Level dynamicLevel = dynamicTracer.getLevel();
        if ((dynamicLevel == null) || !dynamicTracer.isLoggable(Level.FINE)) {
            return false;
        } else {
            return true;
        }
    }

    // override OJPreparingStmt
    protected String getClassRoot()
    {
        return classesRoot.getPath();
    }

    // override OJPreparingStmt
    protected String getJavaRoot()
    {
        return classesRoot.getPath();
    }

    // override OJPreparingStmt
    protected String getTempPackageName()
    {
        return packageName;
    }

    // override OJPreparingStmt
    protected String getTempClassName()
    {
        return "ExecutableStmt";
    }

    // override OJPreparingStmt
    protected String getTempMethodName()
    {
        return "execute";
    }

    //~ Inner Classes
    protected static class ValidatorTable implements SqlValidatorTable
    {
        private final String [] qualifiedName;
        private final RelDataType rowType;
        private final SqlAccessType accessType;

        /**
         * Creates a new ValidatorTable object.
         */
        public ValidatorTable(
            String [] qualifiedName,
            RelDataType rowType,
            SqlAccessType accessType)
        {
            this.qualifiedName = qualifiedName;
            this.rowType = rowType;
            this.accessType = accessType;
        }

        // implement SqlValidatorTable
        public String [] getQualifiedName()
        {
            return qualifiedName;
        }

        // implement SqlValidatorTable
        public boolean isMonotonic(String columnName)
        {
            return false;
        }

        // implement SqlValidatorTable
        public SqlAccessType getAllowedAccess()
        {
            return accessType;
        }

        // implement SqlValidatorTable
        public RelDataType getRowType()
        {
            return rowType;
        }
    }

    /**
     * Private implementation for DefaultValueFactory which looks up a default
     * value stored in the catalog, parses it, and converts it to an
     * Expression.  Processed expressions are cached for use by subsequent
     * calls.  The CwmExpression's MofId is used as the cache key.
     */
    private class ReposDefaultValueFactory implements DefaultValueFactory,
        FarragoObjectCache.CachedObjectFactory
    {
        private Map constructorToSqlMap = new HashMap();

        // implement DefaultValueFactory
        public RexNode newColumnDefaultValue(
            RelOptTable table,
            int iColumn)
        {
            if (!(table instanceof FarragoQueryColumnSet)) {
                return sqlToRelConverter.getRexBuilder().constantNull();
            }
            FarragoQueryColumnSet queryColumnSet =
                (FarragoQueryColumnSet) table;
            CwmColumn column =
                (CwmColumn) queryColumnSet.getCwmColumnSet().getFeature().get(iColumn);
            return convertExpression(column.getInitialValue());
        }

        // implement DefaultValueFactory
        public RexNode newAttributeInitializer(
            RelDataType type,
            SqlFunction constructor,
            int iAttribute,
            RexNode [] constructorArgs)
        {
            SqlIdentifier typeName = type.getSqlIdentifier();
            CwmSqldataType cwmType = stmtValidator.findSqldataType(typeName);
            assert(cwmType instanceof FemSqlobjectType);
            FemSqltypeAttribute attribute =
                (FemSqltypeAttribute) cwmType.getFeature().get(iAttribute);
            if (constructor instanceof FarragoUserDefinedRoutine) {
                RexNode initializer = convertConstructorAssignment(
                    (FarragoUserDefinedRoutine) constructor,
                    attribute,
                    constructorArgs);
                if (initializer != null) {
                    return initializer;
                }
            }
            return convertExpression(attribute.getInitialValue());
        }

        private RexNode convertConstructorAssignment(
            FarragoUserDefinedRoutine constructor,
            FemSqltypeAttribute attribute,
            RexNode [] constructorArgs)
        {
            SqlNodeList nodeList = (SqlNodeList)
                constructorToSqlMap.get(constructor.getFemRoutine());
            if (nodeList == null) {
                assert (constructor.hasDefinition());
                FarragoSessionParser parser =
                    getSession().getPersonality().newParser(
                        getSession());
                String body = constructor.getFemRoutine().getBody().getBody();
                nodeList = (SqlNodeList) parser.parseSqlText(
                    stmtValidator,
                    null,
                    body,
                    true);
                constructorToSqlMap.put(constructor.getFemRoutine(), nodeList);
            }
            Iterator iter = nodeList.getList().iterator();
            SqlNode rhs = null;
            while (iter.hasNext()) {
                SqlCall call = (SqlCall) iter.next();
                SqlIdentifier lhs = (SqlIdentifier) call.getOperands()[0];
                if (lhs.getSimple().equals(attribute.getName())) {
                    rhs = call.getOperands()[1];
                    break;
                }
            }
            if (rhs == null) {
                return null;
            }
            FarragoRoutineInvocation invocation = new FarragoRoutineInvocation(
                constructor,
                constructorArgs);
            return expandInvocationExpression(rhs, invocation);
        }

        private RexNode convertExpression(CwmExpression cwmExp)
        {
            if (cwmExp.getBody().equalsIgnoreCase("NULL")) {
                return sqlToRelConverter.getRexBuilder().constantNull();
            }

            FarragoObjectCache.Entry cacheEntry =
                stmtValidator.getCodeCache().pin(
                    cwmExp.refMofId(),
                    this,
                    false);
            RexNode parsedExp = (RexNode) cacheEntry.getValue();
            stmtValidator.getCodeCache().unpin(cacheEntry);
            return parsedExp;
        }

        // implement CachedObjectFactory
        public void initializeEntry(
            Object key,
            FarragoObjectCache.UninitializedEntry entry)
        {
            String mofId = (String) key;
            CwmExpression cwmExp =
                (CwmExpression) getRepos().getMdrRepos().getByMofId(mofId);
            String defaultString = cwmExp.getBody();
            SqlParser sqlParser = new SqlParser(defaultString);
            SqlNode sqlNode;
            try {
                sqlNode = sqlParser.parseExpression();
            } catch (SqlParseException ex) {
                // parsing of expressions already stored in the catalog should
                // always succeed
                throw Util.newInternal(ex);
            }
            RexNode exp = sqlToRelConverter.convertExpression(sqlNode);

            // TODO:  better memory usage estimate
            entry.initialize(exp,
                3 * FarragoUtil.getStringMemoryUsage(defaultString));
        }
    }
}


// End FarragoPreparingStmt.java
