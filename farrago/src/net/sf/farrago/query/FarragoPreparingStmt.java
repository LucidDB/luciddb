/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
package net.sf.farrago.query;


import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.*;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql2rel.*;
import org.eigenbase.util.*;

import java.util.List;

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
        SqlValidator.CatalogReader
{
    //~ Static fields/initializers --------------------------------------------

    // NOTE jvs 8-June-2004: this tracer is special in that it controls
    // preservation of dynamically generated Java code
    private static final Logger dynamicTracer =
        FarragoTrace.getDynamicTracer();
    private static final Logger streamGraphTracer =
        FarragoTrace.getPreparedStreamGraphTracer();

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
    private boolean processingDirectDependencies;
    private Set loadedServerClassNameSet;
    private RelOptPlanner planner;
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
        processingDirectDependencies = true;

        classesRoot = new File(FarragoProperties.instance().homeDir.get(true));
        classesRoot = new File(classesRoot, "classes");

        // Save some global state for reentrancy
        needRestore = true;
        savedDeclarer = OJUtil.threadDeclarers.get();

        planner = getSession().newPlanner(this,true);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionPreparingStmt
    public FarragoSessionStmtValidator getStmtValidator()
    {
        return stmtValidator;
    }

    public void setPlanner(RelOptPlanner planner)
    {
        this.planner = planner;
    }

    public RelOptPlanner getPlanner()
    {
        return planner;
    }

    // implement FarragoSessionPreparingStmt
    public SqlOperatorTable getSqlOperatorTable()
    {
        if (sqlOperatorTable != null) {
            return sqlOperatorTable;
        }
        
        SqlOperatorTable systemOperators = getSession().getSqlOperatorTable();
        SqlOperatorTable userOperators =
            new FarragoUserDefinedRoutineLookup(stmtValidator, this);
        
        // REVIEW jvs 1-Jan-2004:  precedence of UDF's vs. builtins?
        ChainedSqlOperatorTable table = new ChainedSqlOperatorTable();
        table.add(systemOperators);
        table.add(userOperators);

        sqlOperatorTable = table;
        return sqlOperatorTable;
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
                getSession().getRuntimeContextClass(),
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
                    getSession().getRuntimeContextClass(),
                    this)
            };
        implementingClassDecl = super.init(implementingArgs);
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

    void prepareForCompilation()
    {
        // REVIEW jvs 20-Jan-2005: The idea here is to gather up all jars
        // referenced by external user-defined routines and provide them to the
        // classloader.  However, this loses the associations between jars and
        // routines, meaning if two classes in different jars have the same
        // name, there will be trouble.  The alternative is to always use
        // reflection, which would be bad for UDF performance.  What to do?
        // Also, need to implement jar paths.
        if (jarUrlSet.isEmpty()) {
            // don't need to load any jars
            return;
        }
        List jarUrlList = new ArrayList(jarUrlSet);
        URL [] urls = (URL []) jarUrlList.toArray(new URL[0]);
        URLClassLoader urlClassLoader = URLClassLoader.newInstance(
            urls, javaCompiler.getArgs().getClassLoader());
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

    private FarragoSessionExecutableStmt implement(
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
                    rowType,
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
        // Round up all the dependencies on UDF's.  We can't do this
        // during function lookup because overloads need to be resolved
        // first.  And we can't do this any later because we stop
        // collecting direct dependencies during SqlToRelConverter.
        SqlVisitor udfInvocationFinder = new SqlBasicVisitor() 
            {
                public void visit(SqlCall call)
                {
                    if (call.operator instanceof FarragoUserDefinedRoutine) {
                        FarragoUserDefinedRoutine function =
                            (FarragoUserDefinedRoutine) call.operator;
                        addDependency(function.getFemRoutine());
                    }
                    super.visit(call);
                }
            };
        sqlNode.accept(udfInvocationFinder);
        
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

    private RelDataType getParamRowType()
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
        stopCollectingDirectDependencies();

        SqlParser parser = new SqlParser(queryString);
        final SqlNode sqlQuery;
        try {
            sqlQuery = parser.parseStmt();
        } catch (SqlParseException e) {
            throw Util.newInternal(e,
                "Error while parsing view definition:  " + queryString);
        }
        return sqlToRelConverter.convertQuery(sqlQuery);
    }

    RexNode expandFunction(
        String bodyString,
        Map paramNameToArgMap, 
        final Map paramNameToTypeMap)
    {
        stopCollectingDirectDependencies();
        
        SqlParser parser = new SqlParser(bodyString);
        SqlNode sqlExpr;
        try {
            sqlExpr = parser.parseExpression();
        } catch (SqlParseException e) {
            throw Util.newInternal(e,
                "Error while parsing routine definition:  " + bodyString);
        }

        // NOTE jvs 2-Jan-2005: We already validated the expression during DDL,
        // but we stored the original pre-validation expression, and validation
        // may have involved rewrites relied on by sqlToRelConverter.  So
        // we must recapitulate here.
        sqlExpr = getSqlValidator().validateParameterizedExpression(
            sqlExpr,
            paramNameToTypeMap);

        // TODO jvs 1-Jan-2005: support a RexVariableBinding (like "let" in
        // Lisp), and avoid expansion of parameters which are referenced more
        // than once
        
        return sqlToRelConverter.convertExpression(sqlExpr, paramNameToArgMap);
    }

    private void stopCollectingDirectDependencies()
    {
        // once we start expanding views and functions, all objects we
        // encounter should be treated as indirect dependencies
        processingDirectDependencies = false;
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
        }
        return sqlToRelConverter;
    }

    // implement FarragoSessionPreparingStmt
    public JavaRelImplementor getRelImplementor(RexBuilder rexBuilder)
    {
        if (relImplementor == null) {
            relImplementor = new FarragoRelImplementor(this, rexBuilder);
        }
        return relImplementor;
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
            stmtValidator.resolveSchemaObjectName(names);

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
        } else if (columnSet instanceof CwmView) {
            RelDataType rowType =
                getFarragoTypeFactory().createColumnSetType(columnSet);
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

        // When a foreign table is referenced directly via a namespace, we have
        // nothing to hang a direct dependency on.  Instead, we
        // remember the dependency on the server, so that if the server
        // gets dropped, dependent views will cascade.
        addDependency(femServer);

        FarragoMedDataServer server = loadDataServerFromCache(femServer);

        String [] namesWithoutCatalog =
            new String [] { resolved.schemaName, resolved.objectName };
        try {
            FarragoMedNameDirectory directory = server.getNameDirectory();
            if (directory == null) {
                return null;
            }
            FarragoMedColumnSet medColumnSet =
                directory.lookupColumnSet(
                    getFarragoTypeFactory(),
                    namesWithoutCatalog,
                    resolved.getQualifiedName());
            initializeQueryColumnSet(medColumnSet, null);
            return medColumnSet;
        } catch (Throwable ex) {
            // TODO:  better name formatting
            throw FarragoResource.instance()
                .newValidatorForeignTableLookupFailed(
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
    public RelOptTable getTableForMethodCall(MethodCall call)
    {
        return null;
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
    public SqlValidator.Table getTable(String [] names)
    {
        FarragoSessionResolvedObject resolved =
            stmtValidator.resolveSchemaObjectName(names);

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

        addDependency(table);

        if (table.getVisibility() == null) {
            // Oops, we're processing a compound CREATE SCHEMA statement, and
            // this referenced table hasn't been validated yet.  Throw a
            // special exception to terminate processing of the current
            // dependent view definition, and we'll try again later once the
            // table has been validated.
            throw new FarragoUnvalidatedDependencyException();
        }

        RelDataType rowType =
            getFarragoTypeFactory().createColumnSetType(table);
        return new ValidatorTable(
            resolved.getQualifiedName(),
            rowType);
    }
    
    // implement SqlValidator.CatalogReader
    public String [] getAllSchemaObjectNames(String [] names)
    {   
        return stmtValidator.getAllSchemaObjectNames(names);
    }   

    void addDependency(Object supplier)
    {
        if (processingDirectDependencies) {
            directDependencies.add(supplier);
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

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Private implementation for SqlValidator.Table.
     */
    private static class ValidatorTable implements SqlValidator.Table
    {
        private final String [] qualifiedName;
        private final RelDataType rowType;

        /**
         * Creates a new ValidatorTable object.
         */
        ValidatorTable(
            String [] qualifiedName,
            RelDataType rowType)
        {
            this.qualifiedName = qualifiedName;
            this.rowType = rowType;
        }

        // implement SqlValidator.Table
        public String [] getQualifiedName()
        {
            return qualifiedName;
        }

        // implement SqlValidator.Table
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
        // implement DefaultValueFactory
        public RexNode newDefaultValue(
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
            CwmExpression cwmExp = column.getInitialValue();
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
