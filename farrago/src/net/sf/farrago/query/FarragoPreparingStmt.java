/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
import net.sf.farrago.session.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.stmt.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.sql.parser.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
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

import java.util.List;

/**
 * FarragoPreparingStmt subclasses OJStatement to manage Farrago-specific
 * preparation of a single SQL statement (it's not a context for executing a
 * series of statements; for that, see {@link
 * net.sf.farrago.session.FarragoSessionStmtContext}).  The result is a
 * {@link FarragoExecutableStmt}.
 *
 *<p>
 *
 * FarragoPreparingStmt has a fleeting lifetime, which is why its name is in
 * the progressive tense.  Once its job is done, it should be discarded (and
 * can't be reused).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPreparingStmt extends OJStatement
    implements
        SaffronConnection,
        SaffronSchema,
        SqlValidator.CatalogReader,
        FarragoAllocation
{
    private static Logger dynamicTracer =
    Logger.getLogger("net.sf.farrago.dynamic");

    private static Logger compilerTracer =
    Logger.getLogger("net.sf.farrago.compiler");

    //~ Instance fields -------------------------------------------------------

    /** Default qualifiers to use for looking up unqualified table names. */
    private FarragoConnectionDefaults connectionDefaults;

    /** Singleton catalog instance. */
    private FarragoCatalog catalog;
    
    /** Type factory for this statement. */
    private FarragoTypeFactory farragoTypeFactory;

    /** Handle to Fennel database affected by this statement */
    private FennelDbHandle fennelDbHandle;

    private FarragoObjectCache codeCache;

    private FarragoDataWrapperCache dataWrapperCache;

    private SqlToRelConverter sqlToRelConverter;

    private FarragoIndexMap indexMap;

    private SaffronTypeFactory savedTypeFactory;
    
    private VolcanoPlannerFactory savedPlannerFactory;
    
    private ClassMap savedClassMap;
    
    private Object savedDeclarer;
    
    private FarragoAllocation javaCodeDir;

    private FarragoSqlValidator validator;

    private Set directDependencies;

    private FarragoCompoundAllocation allocations;

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

    private boolean rememberDependencies;

    private Set loadedServerClassNameSet;

    private FarragoPlanner planner;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPreparingStmt object.
     *
     * @param catalog catalog to use for object definitions
     * @param fennelDbHandle handle to Fennel database to access 
     * @param connectionDefaults default qualifiers for unqualified object
     * references
     * @param codeCache FarragoObjectCache to use for caching code snippets
     * needed during preparation
     * @param dataWrapperCache FarragoObjectCache to use for caching
     * FarragoMedDataWrapper instances
     * @param indexMap FarragoIndexMap to use for index access
     */
    public FarragoPreparingStmt(
        FarragoCatalog catalog,
        FennelDbHandle fennelDbHandle,
        FarragoConnectionDefaults connectionDefaults,
        FarragoObjectCache codeCache,
        FarragoObjectCache dataWrapperCache,
        FarragoIndexMap indexMap)
    {
        super(null);
        this.catalog = catalog;
        this.fennelDbHandle = fennelDbHandle;
        this.connectionDefaults = connectionDefaults;
        this.codeCache = codeCache;
        this.indexMap = indexMap;

        allocations = new FarragoCompoundAllocation();
        farragoTypeFactory = new FarragoTypeFactoryImpl(catalog);
        this.dataWrapperCache = new FarragoDataWrapperCache(
            allocations,dataWrapperCache,catalog);
        loadedServerClassNameSet = new HashSet();
            
        super.setResultCallingConvention(CallingConvention.ITERATOR);

        directDependencies = new HashSet();
        rememberDependencies = true;

        classesRoot = new File(
            System.getProperty(
                FarragoModelLoader.HOME_PROPERTY));
        classesRoot = new File(classesRoot,"classes");

        // Save some global state for reentrancy
        savedTypeFactory = SaffronTypeFactoryImpl.threadInstance();
        savedPlannerFactory = VolcanoPlannerFactory.threadInstance();
        savedClassMap = ClassMap.instance();
        savedDeclarer = OJUtil.threadDeclarers.get();

        SaffronTypeFactoryImpl.setThreadInstance(farragoTypeFactory);
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
        
        // FIXME:  should be beginTrans(false) for read-only, but that prevents
        // creation of transient objects for communication with Fennel
        catalog.getRepository().beginTrans(true);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Perform validation.
     *
     * @param sqlNode unvalidated SQL statement
     *
     * @return validated SQL statement
     */
    public SqlNode validate(SqlNode sqlNode)
    {
        validator = new FarragoSqlValidator(this);
        return validator.validate(sqlNode);
    }

    /**
     * Implement a query or DML statement but do not execute it.
     *
     * @param sqlNode top-level node of parsed statement
     *
     * @return prepared FarragoExecutableStmt
     */
    public FarragoExecutableStmt implement(SqlNode sqlNode)
    {
        boolean needValidation = false;
        if (validator == null) {
            validator = new FarragoSqlValidator(this);
            needValidation = true;
        }
        
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
        
        PreparedResult preparedResult = super.prepareSql(
            sqlNode,
            FarragoRuntimeContext.class,
            validator,
            needValidation);
        FarragoExecutableStmt executableStmt;
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
                
            executableStmt = new FarragoExecutableJavaStmt(
                packageDir,
                rowClass,
                rowType,
                dynamicParamRowType,
                preparedExecution.getMethod(),
                preparedResult.isDml());
        } else {
            assert(preparedResult instanceof PreparedExplanation);
            executableStmt = new FarragoExecutableExplainStmt(
                farragoTypeFactory.createProjectType(
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

    /**
     * Partially prepare this statement for use as a view definition.
     *
     * @param info receives view info
     */
    public void prepareViewInfo(SqlNode sqlNode,FarragoSessionViewInfo info)
    {
        getSqlToRelConverter(validator,this);
        SaffronRel rootRel = sqlToRelConverter.convertValidatedQuery(sqlNode);
        info.resultMetaData = new FarragoResultSetMetaData(
            rootRel.getRowType());
        info.parameterMetaData = new FarragoParameterMetaData(
            getParamRowType());
        info.dependencies = Collections.unmodifiableSet(directDependencies);
    }

    private SaffronType getParamRowType()
    {
        return farragoTypeFactory.createProjectType(
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
        if (catalog == null) {
            // already closed
            return;
        }
        SaffronTypeFactoryImpl.setThreadInstance(savedTypeFactory);
        VolcanoPlannerFactory.setThreadInstance(savedPlannerFactory);
        ClassMap.setInstance(savedClassMap);
        OJUtil.threadDeclarers.set(savedDeclarer);
        // Now that preparation is complete, we can commit.
        // TODO:  obtain locks to ensure that objects we intend to operate
        // on don't change.
        catalog.getRepository().endTrans();
        if (javaCodeDir != null) {
            javaCodeDir.closeAllocation();
            javaCodeDir = null;
        }
        allocations.closeAllocation();
        catalog = null;
    }

    SaffronRel expandView(String queryString)
    {
        // once we start expanding views, we no longer want to keep adding
        // dependencies, since we're only interested in direct
        // references
        rememberDependencies = false;
        
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
        
        // NOTE:  keep around sqlToRelConverterInstance for use during
        // activities like loading default values
        sqlToRelConverter = new SqlToRelConverter(
            validator,
            connection.getSaffronSchema(),
            getEnvironment(),
            connection,
            new FarragoRexBuilder(farragoTypeFactory));
        sqlToRelConverter.setDefaultValueFactory(
            new CatalogDefaultValueFactory());
        return sqlToRelConverter;
    }

    protected RelImplementor getRelImplementor(RexBuilder rexBuilder)
    {
        return new FarragoRelImplementor(this,rexBuilder);
    }

    /**
     * .
     *
     * @return catalog for this stmt
     */
    public FarragoCatalog getCatalog()
    {
        return catalog;
    }

    /**
     * .
     *
     * @return handle to Fennel database accessed by this stmt
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    /**
     * .
     *
     * @return type factory for this stmt
     */
    public FarragoTypeFactory getFarragoTypeFactory()
    {
        return farragoTypeFactory;
    }

    /**
     * .
     *
     * @return FarragoIndexMap to use for accessing index storage
     */
    public FarragoIndexMap getIndexMap()
    {
        return indexMap;
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
            catalog.resolveSchemaObjectName(connectionDefaults,names);

        if (resolved.object == null) {
            return getForeignTableFromNamespace(resolved);
        }

        assert(resolved.object instanceof CwmNamedColumnSet);

        CwmNamedColumnSet columnSet = (CwmNamedColumnSet) resolved.object;

        if (columnSet instanceof FemForeignTable) {
            FemForeignTableImpl table = (FemForeignTableImpl) columnSet;
            loadDataServerFromCache(table.getDataServer(catalog));
            return table.loadFromCache(
                dataWrapperCache,catalog,farragoTypeFactory);
        }
        
        if (columnSet instanceof CwmTable) {
            CwmTable table = (CwmTable) columnSet;
            
            // REVIEW:  maybe defer this until physical implementation?
            if (table.isTemporary()) {
                indexMap.instantiateTemporaryTable(table);
            }
        }
        
        SaffronType rowType =
            getFarragoTypeFactory().createColumnSetType(columnSet);
        
        if (columnSet instanceof CwmTable) {
            return new FennelTable(this,columnSet,rowType);
        } else if (columnSet instanceof CwmView) {
            return new FarragoView(this,columnSet,rowType);
        } else {
            throw Util.needToImplement(columnSet);
        }
    }

    private FarragoMedColumnSet getForeignTableFromNamespace(
        FarragoCatalog.ResolvedSchemaObject resolved)
    {
        FemDataServerImpl femServer = (FemDataServerImpl)
            catalog.getModelElement(
                catalog.medPackage.getFemDataServer().refAllOfType(),
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
            return directory.lookupColumnSet(
                farragoTypeFactory,
                namesWithoutCatalog,
                resolved.getQualifiedName());
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
            femServer.loadFromCache(dataWrapperCache);
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
            catalog.resolveSchemaObjectName(connectionDefaults,names);

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
        if (rememberDependencies) {
            directDependencies.add(supplier);
        }
    }

    public Variable getConnectionVariable()
    {
        return new Variable(connectionVariable);
    }

    // override OJStatement
    protected String getCompilerClassName()
    {
        // TODO: For now default to DynamicJava interpretation.  Eventually, we
        // want the optimizer to be able to tell us when we should definitely
        // compile before the first execution, and also let caching decide
        // based on usage patterns.

        Level compilerLevel = compilerTracer.getLevel();
        if ((compilerLevel == null) || !compilerTracer.isLoggable(Level.FINE)) {
            return "openjava.ojc.DynamicJavaCompiler";
        } else {
            // if trace property net.sf.farrago.compiler is set to FINE,
            // then use the Sun Java compiler, since it produces better
            // error messages
            return super.getCompilerClassName();
        }
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
            // REVIEW:  will we ever get anything else?
            FennelTable fennelTable = (FennelTable) table;
            CwmColumn column = (CwmColumn)
                fennelTable.cwmTable.getFeature().get(iColumn);
            CwmExpression cwmExp = column.getInitialValue();
            if (cwmExp == null) {
                return sqlToRelConverter.getRexBuilder().constantNull();
            }

            FarragoObjectCache.Entry cacheEntry = codeCache.pin(
                cwmExp.refMofId(),this,false);
            RexNode parsedExp = (RexNode) cacheEntry.getValue();
            codeCache.unpin(cacheEntry);
            return parsedExp;
        }

        // implement CachedObjectFactory
        public void initializeEntry(
            Object key,
            FarragoObjectCache.UninitializedEntry entry)
        {
            String mofId = (String) key;
            CwmExpression cwmExp =
                (CwmExpression) catalog.getRepository().getByMofId(mofId);
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
