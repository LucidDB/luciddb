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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.session.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.stmt.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.sql.parser.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql2rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rex.RexNode;

import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.*;

import java.io.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;
import javax.jmi.model.*;
import javax.jmi.reflect.*;

/**
 * FarragoPreparingStmt subclasses OJStatement to implement the
 * {@link FarragoSessionPreparingStmt} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPreparingStmt extends OJStatement
    implements
        FarragoSessionPreparingStmt,
        SaffronConnection,
        SaffronSchema,
        SqlValidator.CatalogReader
{
    // NOTE jvs 8-June-2004: this tracer is special in that it controls
    // preservation of dynamically generated Java code
    private static final Logger dynamicTracer = FarragoTrace.getDynamicTracer();

    private static final Logger streamGraphTracer =
        FarragoTrace.getPreparedStreamGraphTracer();

    //~ Instance fields -------------------------------------------------------

    private final FarragoSessionStmtValidator stmtValidator;

    private boolean needRestore;
    
    private SqlToRelConverter sqlToRelConverter;

    private SaffronTypeFactory savedTypeFactory;

    private VolcanoPlannerFactory savedPlannerFactory;

    private ClassMap savedClassMap;

    private Object savedDeclarer;

    private FarragoAllocation javaCodeDir;

    private FarragoSqlValidator sqlValidator;

    private Set directDependencies;

    private Set allDependencies;

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
    private Argument[] implementingArgs;

    private boolean processingDirectDependencies;

    private Set loadedServerClassNameSet;

    private FarragoPlanner planner;

    private FarragoRelImplementor relImplementor;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPreparingStmt object.
     *
     * @param stmtValidator generic stmt validator
     */
    public FarragoPreparingStmt(
        FarragoSessionStmtValidator stmtValidator)
    {
        super(null);

        this.stmtValidator = stmtValidator;
        stmtValidator.addAllocation(this);

        loadedServerClassNameSet = new HashSet();

        super.setResultCallingConvention(CallingConvention.ITERATOR);

        directDependencies = new HashSet();
        allDependencies = new HashSet();
        processingDirectDependencies = true;

        classesRoot = new File(
            FarragoProperties.instance().homeDir.get(true));
        classesRoot = new File(classesRoot,"classes");

        // Save some global state for reentrancy
        needRestore = true;
        savedTypeFactory = SaffronTypeFactoryImpl.threadInstance();
        savedPlannerFactory = VolcanoPlannerFactory.threadInstance();
        savedClassMap = ClassMap.instance();
        savedDeclarer = OJUtil.threadDeclarers.get();

        SaffronTypeFactoryImpl.setThreadInstance(getFarragoTypeFactory());
        ClassMap.setInstance(new ClassMap(FarragoSyntheticObject.class));
        VolcanoPlannerFactory.setThreadInstance(
            new VolcanoPlannerFactory() {
                public VolcanoPlanner newPlanner()
                {
                    return planner;
                }
            });
        planner = new FarragoPlanner(this);
        planner.init();
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSessionPreparingStmt
    public FarragoSessionStmtValidator getStmtValidator()
    {
        return stmtValidator;
    }
    
    public void setPlanner(FarragoPlanner planner)
    {
        this.planner = planner;
    }

    // implement FarragoSessionPreparingStmt
    public SqlOperatorTable getSqlOperatorTable()
    {
        return getSession().getSqlOperatorTable();
    }

    // implement FarragoSessionPreparingStmt
    public SqlNode validate(SqlNode sqlNode)
    {
        return getSqlValidator().validate(sqlNode);
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
        PreparedResult preparedResult = super.prepareSql(
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
        implementingArgs = new Argument [] {
            new Argument(
                connectionVariable,
                getSession().getRuntimeContextClass(),
                this)
        };
        implementingClassDecl = super.init(implementingArgs);
    }

    // implement FarragoSessionPreparingStmt
    public FarragoSessionExecutableStmt implement(
        SaffronRel rootRel, SqlKind sqlKind, boolean logical)
    {
        PreparedResult preparedResult =
            super.prepareSql(rootRel, sqlKind, logical,
                             implementingClassDecl, implementingArgs);
        return implement(preparedResult);
    }

    private void definePackageName()
    {
        // TODO:  once and only once
        packageDir = classesRoot;
        packageDir = new File(packageDir,"net");
        packageDir = new File(packageDir,"sf");
        packageDir = new File(packageDir,"farrago");
        packageDir = new File(packageDir,"dynamic");
        try {
            packageDir.mkdirs();
            packageDir = File.createTempFile("stmt","",packageDir);
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
            SaffronType rowType =
                preparedExecution.getRowType();
            OJClass ojRowClass = OJUtil.typeToOJClass(rowType);
            Class rowClass;
            try {
                String ojRowClassName = ojRowClass.getName();
                int i = ojRowClassName.lastIndexOf('.');
                assert (i != -1);
                ojRowClassName =
                    OJClass.replaceDotWithDollar(ojRowClassName,i);
                rowClass = Class.forName(
                    ojRowClassName,true,javaCompiler.getClassLoader());
            } catch (ClassNotFoundException ex) {
                throw Util.newInternal(ex);
            }

            SaffronType dynamicParamRowType = getParamRowType();

            String xmiFennelPlan = null;
            Set streamDefSet = relImplementor.getStreamDefSet();
            if (!streamDefSet.isEmpty()) {
                FemCmdPrepareExecutionStreamGraph cmdPrepareStream =
                    getCatalog().newFemCmdPrepareExecutionStreamGraph();
                Collection streamDefs = cmdPrepareStream.getStreamDefs();
                streamDefs.addAll(streamDefSet);
                xmiFennelPlan =
                    JmiUtil.exportToXmiString(
                        Collections.singleton(cmdPrepareStream));
                streamGraphTracer.fine(xmiFennelPlan);
            }

            executableStmt = new FarragoExecutableJavaStmt(
                packageDir,
                rowClass,
                rowType,
                dynamicParamRowType,
                preparedExecution.getMethod(),
                xmiFennelPlan,
                preparedResult.isDml(),
                getReferencedObjectIds());
        } else {
            assert(preparedResult instanceof PreparedExplanation);
            executableStmt = new FarragoExecutableExplainStmt(
                getFarragoTypeFactory().createProjectType(
                    new SaffronType[0],
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
    public void prepareViewInfo(SqlNode sqlNode,FarragoSessionViewInfo info)
    {
        getSqlToRelConverter();
        SaffronRel rootRel = sqlToRelConverter.convertValidatedQuery(sqlNode);
        info.resultMetaData = new FarragoResultSetMetaData(
            rootRel.getRowType());
        info.parameterMetaData = new FarragoParameterMetaData(
            getParamRowType());
        info.dependencies = Collections.unmodifiableSet(directDependencies);
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
        return getSqlToRelConverter(sqlValidator,this);
    }

    // implement FarragoSessionPreparingStmt
    public VolcanoCluster getVolcanoCluster()
    {
        return getSqlToRelConverter().getCluster();
    }

    private SaffronType getParamRowType()
    {
        return getFarragoTypeFactory().createProjectType(
            new SaffronTypeFactory.FieldInfo()
            {
                public int getFieldCount()
                {
                    return sqlToRelConverter.getDynamicParamCount();
                }

                public String getFieldName(int index)
                {
                    return "?" + index;
                }

                public SaffronType getFieldType(int index)
                {
                    return sqlToRelConverter.getDynamicParamType(
                        index);
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
        SaffronTypeFactoryImpl.setThreadInstance(savedTypeFactory);
        VolcanoPlannerFactory.setThreadInstance(savedPlannerFactory);
        ClassMap.setInstance(savedClassMap);
        OJUtil.threadDeclarers.set(savedDeclarer);
        // TODO:  obtain locks to ensure that objects we intend to operate
        // on don't change after we end repository txn.
        if (javaCodeDir != null) {
            javaCodeDir.closeAllocation();
            javaCodeDir = null;
        }
        needRestore = false;
    }

    SaffronRel expandView(String queryString)
    {
        // once we start expanding views, all objects we encounter
        // should be treated as indirect dependencies
        processingDirectDependencies = false;

        SqlParser parser = new SqlParser(queryString);
        final SqlNode sqlQuery;
        try {
            sqlQuery = parser.parseStmt();
        } catch (ParseException e) {
            throw Util.newInternal(
                e,
                "Error while parsing view definition:  " + queryString);
        }
        return sqlToRelConverter.convertQuery(sqlQuery);
    }

    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        SaffronConnection connection)
    {
        // REVIEW:  recycling may be dangerous since SqlToRelConverter is
        // stateful
        if (sqlToRelConverter == null) {
            sqlToRelConverter = new SqlToRelConverter(
                validator,
                connection.getSaffronSchema(),
                getEnvironment(),
                connection,
                new FarragoRexBuilder(getFarragoTypeFactory()));
            sqlToRelConverter.setDefaultValueFactory(
                new CatalogDefaultValueFactory());
        }
        return sqlToRelConverter;
    }

    protected JavaRelImplementor getRelImplementor(RexBuilder rexBuilder)
    {
        if (relImplementor == null) {
            relImplementor = new FarragoRelImplementor(this,rexBuilder);
        }
        return relImplementor;
    }

    // implement FarragoSessionPreparingStmt
    public FarragoCatalog getCatalog()
    {
        return stmtValidator.getCatalog();
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
    public FarragoIndexMap getIndexMap()
    {
        return stmtValidator.getIndexMap();
    }

    // implement FarragoSessionPreparingStmt
    public FarragoSession getSession()
    {
        return stmtValidator.getSession();
    }

    // implement SaffronConnection
    public SaffronSchema getSaffronSchema()
    {
        return this;
    }

    // implement SaffronConnection
    public Object contentsAsArray(String qualifier,String tableName)
    {
        throw new UnsupportedOperationException(
            "FarragoPreparingStmt.contentsAsArray() should have been replaced");
    }

    // implement SaffronSchema
    public SaffronTable getTableForMember(String [] names)
    {
        FarragoCatalog.ResolvedSchemaObject resolved =
            getCatalog().resolveSchemaObjectName(
                stmtValidator.getConnectionDefaults(),names);

        if (resolved.object == null) {
            return getForeignTableFromNamespace(resolved);
        }

        assert(resolved.object instanceof CwmNamedColumnSet);

        CwmNamedColumnSet columnSet = (CwmNamedColumnSet) resolved.object;

        if (columnSet instanceof FemLocalTable) {
            FemLocalTable table = (FemLocalTable) columnSet;

            // REVIEW:  maybe defer this until physical implementation?
            if (table.isTemporary()) {
                getIndexMap().instantiateTemporaryTable(table);
            }
        }

        SaffronTable saffronTable;
        if (columnSet instanceof FemBaseColumnSet) {
            FemBaseColumnSet table = (FemBaseColumnSet) columnSet;
            FemDataServerImpl femServer = (FemDataServerImpl)
                table.getServer();
            loadDataServerFromCache(femServer);
            saffronTable = femServer.loadColumnSetFromCache(
                stmtValidator.getDataWrapperCache(),
                getCatalog(),
                getFarragoTypeFactory(),
                table);
        } else if (columnSet instanceof CwmView) {
            SaffronType rowType =
                getFarragoTypeFactory().createColumnSetType(columnSet);
            saffronTable = new FarragoView(columnSet,rowType);
        } else {
            throw Util.needToImplement(columnSet);
        }
        initializeQueryColumnSet(saffronTable,columnSet);
        return saffronTable;
    }

    private void initializeQueryColumnSet(
        SaffronTable saffronTable,CwmNamedColumnSet cwmColumnSet)
    {
        if (saffronTable == null) {
            return;
        }
        if (!(saffronTable instanceof FarragoQueryColumnSet)) {
            return;
        }
        FarragoQueryColumnSet queryColumnSet =
            (FarragoQueryColumnSet) saffronTable;
        queryColumnSet.setPreparingStmt(this);
        queryColumnSet.setCwmColumnSet(cwmColumnSet);
    }

    private FarragoMedColumnSet getForeignTableFromNamespace(
        FarragoCatalog.ResolvedSchemaObject resolved)
    {
        FemDataServerImpl femServer = (FemDataServerImpl)
            getCatalog().getModelElement(
                getCatalog().medPackage.getFemDataServer().refAllOfType(),
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

        String [] namesWithoutCatalog = new String [] {
            resolved.schemaName,
            resolved.objectName
        };
        try {
            FarragoMedNameDirectory directory = server.getNameDirectory();
            if (directory == null) {
                return null;
            }
            FarragoMedColumnSet medColumnSet = directory.lookupColumnSet(
                getFarragoTypeFactory(),
                namesWithoutCatalog,
                resolved.getQualifiedName());
            initializeQueryColumnSet(medColumnSet,null);
            return medColumnSet;
        } catch (Throwable ex) {
            // TODO:  better name formatting
            throw
                FarragoResource.instance().newValidatorForeignTableLookupFailed(
                    Arrays.asList(resolved.getQualifiedName()).toString(),ex);
        }
    }

    private FarragoMedDataServer loadDataServerFromCache(
        FemDataServerImpl femServer)
    {
        FarragoMedDataServer server =
            femServer.loadFromCache(stmtValidator.getDataWrapperCache());
        if (loadedServerClassNameSet.add(server.getClass().getName())) {
            // This is the first time we've seen this server class, so give it
            // a chance to register any planner info such as calling
            // conventions and rules.  REVIEW: the discrimination is based on
            // class name, on the assumption that it should unique regardless
            // of classloader, JAR, etc.  Is that correct?
            server.registerRules(planner);
        }
        return server;
    }

    // implement SaffronSchema
    public SaffronTable getTableForMethodCall(MethodCall call)
    {
        return null;
    }

    // implement SaffronSchema
    public SaffronTypeFactory getTypeFactory()
    {
        return SaffronTypeFactoryImpl.threadInstance();
    }

    // implement SaffronSchema
    public void registerRules(SaffronPlanner planner)
    {
        // nothing to do; FarragoPlanner does it for us
    }

    // implement SqlValidator.CatalogReader
    public SqlValidator.Table getTable(String [] names)
    {
        FarragoCatalog.ResolvedSchemaObject resolved =
            getCatalog().resolveSchemaObjectName(
                stmtValidator.getConnectionDefaults(),names);

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

        SaffronType rowType =
            getFarragoTypeFactory().createColumnSetType(table);
        return new ValidatorTable(resolved.getQualifiedName(),rowType);
    }

    private void addDependency(Object supplier)
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

    // override OJStatement
    protected String getCompilerClassName()
    {
        return getCatalog().getCurrentConfig().getJavaCompilerClassName();
    }

    // override OJStatement
    protected boolean shouldSetConnectionInfo()
    {
        return false;
    }

    // override OJStatement
    protected boolean shouldAlwaysWriteJavaFile()
    {
        Level dynamicLevel = dynamicTracer.getLevel();
        if ((dynamicLevel == null) || !dynamicTracer.isLoggable(Level.FINE)) {
            return false;
        } else {
            return true;
        }
    }

    // override OJStatement
    protected boolean shouldReloadTrace()
    {
        return false;
    }

    // override OJStatement
    protected String getClassRoot()
    {
        return classesRoot.getPath();
    }

    // override OJStatement
    protected String getJavaRoot()
    {
        return classesRoot.getPath();
    }

    // override OJStatement
    protected String getTempPackageName()
    {
        return packageName;
    }

    // override OJStatement
    protected String getTempClassName()
    {
        return "ExecutableStmt";
    }

    // override OJStatement
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

        private final SaffronType rowType;

        /**
         * Creates a new ValidatorTable object.
         */
        ValidatorTable(String [] qualifiedName,SaffronType rowType)
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
        public SaffronType getRowType()
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
    private class CatalogDefaultValueFactory
        implements DefaultValueFactory, FarragoObjectCache.CachedObjectFactory
    {
        // implement DefaultValueFactory
        public RexNode newDefaultValue(
            SaffronTable table,
            int iColumn)
        {
            if (!(table instanceof FarragoQueryColumnSet)) {
                return sqlToRelConverter.getRexBuilder().constantNull();
            }
            FarragoQueryColumnSet queryColumnSet =
                (FarragoQueryColumnSet) table;
            CwmColumn column = (CwmColumn)
                queryColumnSet.getCwmColumnSet().getFeature().get(iColumn);
            CwmExpression cwmExp = column.getInitialValue();
            if (cwmExp.getBody().equalsIgnoreCase("NULL")) {
                return sqlToRelConverter.getRexBuilder().constantNull();
            }

            FarragoObjectCache.Entry cacheEntry =
                stmtValidator.getCodeCache().pin(
                    cwmExp.refMofId(),this,false);
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
                (CwmExpression) getCatalog().getRepository().getByMofId(mofId);
            String defaultString = cwmExp.getBody();
            SqlParser sqlParser = new SqlParser(defaultString);
            SqlNode sqlNode;
            try {
                sqlNode = sqlParser.parseExpression();
            } catch (ParseException ex) {
                // parsing of expressions already stored in the catalog should
                // always succeed
                throw Util.newInternal(ex);
            }
            RexNode exp = sqlToRelConverter.convertExpression(null,sqlNode);
            // TODO:  better memory usage estimate
            entry.initialize(
                exp,
                3*FarragoUtil.getStringMemoryUsage(defaultString));
        }
    }
}


// End FarragoPreparingStmt.java
