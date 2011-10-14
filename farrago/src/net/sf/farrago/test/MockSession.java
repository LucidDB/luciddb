/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
package net.sf.farrago.test;

import java.util.*;
import java.util.regex.Pattern;
import java.sql.*;
import javax.sql.DataSource;
import javax.jmi.reflect.RefObject;

import org.netbeans.api.mdr.MDRepository;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.oj.rex.*;
import org.eigenbase.jmi.*;
import org.eigenbase.resgen.ResourceDefinition;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.TableModificationRel;
import org.eigenbase.javac.JaninoCompiler;
import net.sf.farrago.session.*;
import net.sf.farrago.namespace.util.FarragoDataWrapperCache;
import net.sf.farrago.util.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.FennelDbHandle;
import net.sf.farrago.fennel.calc.*;
import net.sf.farrago.db.FarragoDbSessionPrivilegeChecker;
import net.sf.farrago.plugin.FarragoPluginClassLoader;
import net.sf.farrago.query.*;
import net.sf.farrago.ddl.DdlHandler;
import net.sf.farrago.ddl.gen.*;
import net.sf.farrago.type.FarragoTypeFactoryImpl;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.sql2003.FemAbstractColumnSet;
import net.sf.farrago.fem.med.FemLocalIndex;
import net.sf.farrago.fem.config.FemFarragoConfig;
import net.sf.farrago.FarragoPackage;
import net.sf.farrago.runtime.FarragoRuntimeContext;
import net.sf.farrago.parser.FarragoParser;

/**
 * Collection of mock objects to simulate farrago session objects such as
 * {@link net.sf.farrago.query.FarragoPreparingStmt} without having a live
 * repository.
 *
 * @version $Id$
 * @author jhyde
 */
public class MockSession
{
    protected final FarragoSessionStmtValidator stmtValidator;
    private FarragoPreparingStmt preparingStmt;
    private static MockSession instance;

    protected MockSession()
    {
        FarragoDefaultSessionFactory sessionFactory = createSessionFactory();
        FarragoAllocationOwner allocationOwner =
            new FarragoCompoundAllocation();

        FarragoRepos repos = sessionFactory.newRepos(allocationOwner, false);

        FarragoSession session = new MockFarragoSession(sessionFactory);

        stmtValidator = createStmtValidator(repos, session);
    }

    protected MockSessionFactory createSessionFactory()
    {
        return new MockSessionFactory();
    }

    public FarragoPreparingStmt getPreparingStmt()
    {
        if (preparingStmt == null) {
            preparingStmt =
                new FarragoPreparingStmt(
                    null,
                    stmtValidator,
                    "?");
            preparingStmt.setPlanner(
                stmtValidator.getSession().getPersonality().newPlanner(
                    preparingStmt, true));
        }
        return preparingStmt;
    }

    protected OJRexImplementorTable getJavaImplementorTable()
    {
        return OJRexImplementorTableImpl.instance();
    }

    protected CalcRexImplementorTable getFennelImplementorTable()
    {
        return CalcRexImplementorTableImpl.std();
    }

    protected FarragoSessionStmtValidator createStmtValidator(
        FarragoRepos repos,
        FarragoSession session)
    {
        return new FarragoStmtValidator(
            repos,
            null,
            session, null, null, null, null)
        {
            // implement FarragoSessionStmtValidator
            public void validateDataType(SqlDataTypeSpec dataType)
            {
                // intentionally empty
            }
        };
    }

    public static MockSession instance()
    {
        if (instance == null) {
            instance = new MockSession();
        }
        return instance;
    }

    /**
     * Mock implementation of {@link net.sf.farrago.session.FarragoSession} for
     * testing.
     */
    public class MockFarragoSession implements FarragoSession
    {
        private final FarragoSessionFactory sessionFactory;
        private final FarragoSessionPersonality personality;
        private final FarragoSessionVariables sessionVariables =
            new FarragoSessionVariables();

        /**
         * Creates a MockFarragoSession.
         *
         * @param sessionFactory Session factory
         */
        protected MockFarragoSession(FarragoSessionFactory sessionFactory)
        {
            this.sessionFactory = sessionFactory;
            personality = sessionFactory.newSessionPersonality(this, null);
            personality.loadDefaultSessionVariables(sessionVariables);
        }

        public FarragoSessionFactory getSessionFactory()
        {
            return sessionFactory;
        }

        public FarragoSessionPersonality getPersonality()
        {
            return personality;
        }

        public FarragoSessionStmtContext newStmtContext(
            FarragoSessionStmtParamDefFactory paramDefFactory,
            FarragoSessionStmtContext rootStmtContext)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionStmtContext newStmtContext(
            FarragoSessionStmtParamDefFactory paramDefFactory)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionStmtValidator newStmtValidator()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoDataWrapperCache newFarragoDataWrapperCache(
            FarragoAllocationOwner owner,
            FarragoObjectCache sharedCache,
            FarragoRepos repos,
            FennelDbHandle fennelDbHandle,
            DataSource loopbackDataSource)
        {
            return new FarragoDataWrapperCache(
                owner,
                sharedCache,
                getPluginClassLoader(),
                repos,
                fennelDbHandle,
                loopbackDataSource);
        }

        public FarragoSessionPrivilegeChecker newPrivilegeChecker()
        {
            return new FarragoDbSessionPrivilegeChecker(this);
        }

        public FarragoSessionPrivilegeMap getPrivilegeMap()
        {
            throw new UnsupportedOperationException();
        }

        public String getUrl()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoRepos getRepos()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoPluginClassLoader getPluginClassLoader()
        {
            return new FarragoPluginClassLoader();
        }

        public List<FarragoSessionModelExtension> getModelExtensions()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isClone()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isClosed()
        {
            throw new UnsupportedOperationException();
        }

        public boolean wasKilled()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isTxnInProgress()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isAutoCommit()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionVariables getSessionVariables()
        {
            return sessionVariables;
        }

        public DatabaseMetaData getDatabaseMetaData()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionConnectionSource getConnectionSource()
        {
            return null;
        }

        public FarragoSessionIndexMap getSessionIndexMap()
        {
            throw new UnsupportedOperationException();
        }

        public void setDatabaseMetaData(DatabaseMetaData dbMetaData)
        {
            throw new UnsupportedOperationException();
        }

        public void setConnectionSource(FarragoSessionConnectionSource source)
        {
            throw new UnsupportedOperationException();
        }

        public void setSessionIndexMap(FarragoSessionIndexMap sessionIndexMap)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSession cloneSession(
            FarragoSessionVariables inheritedVariables)
        {
            throw new UnsupportedOperationException();
        }

        public void setAutoCommit(boolean autoCommit)
        {
            throw new UnsupportedOperationException();
        }

        public void commit()
        {
            throw new UnsupportedOperationException();
        }

        public void rollback(FarragoSessionSavepoint savepoint)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionSavepoint newSavepoint(String name)
        {
            throw new UnsupportedOperationException();
        }

        public void releaseSavepoint(FarragoSessionSavepoint savepoint)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionAnalyzedSql analyzeSql(
            String sql, RelDataTypeFactory typeFactory,
            RelDataType paramRowType, boolean optimize)
        {
            throw new UnsupportedOperationException();
        }

        public Collection<RefObject> executeLurqlQuery(
            String lurql, Map<String, ?> argMap)
        {
            throw new UnsupportedOperationException();
        }

        public void closeAllocation()
        {
            throw new UnsupportedOperationException();
        }

        public void kill()
        {
            closeAllocation();
        }

        public void cancel()
        {
            throw new UnsupportedOperationException();
        }

        public void endTransactionIfAuto(boolean commit)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionInfo getSessionInfo()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionTxnId getTxnId(boolean createIfNeeded)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionTxnMgr getTxnMgr()
        {
            throw new UnsupportedOperationException();
        }

        public void setOptRuleDescExclusionFilter(Pattern exclusionFilter)
        {
        }

        public Pattern getOptRuleDescExclusionFilter()
        {
            return FarragoReduceExpressionsRule.EXCLUSION_PATTERN;
        }

        public FarragoWarningQueue getWarningQueue()
        {
            throw new UnsupportedOperationException();
        }

        public void disableSubqueryReduction()
        {
            throw new UnsupportedOperationException();
        }

        public Long getSessionLabelCsn()
        {
            throw new UnsupportedOperationException();
        }

        public Timestamp getSessionLabelCreationTimestamp()
        {
            throw new UnsupportedOperationException();
        }

        public void setLoopback()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isLoopback()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isReentrantAlterTableAddColumn()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isReentrantAlterTableRebuild()
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Mock implementation of
     * {@link net.sf.farrago.session.FarragoSessionPersonality} for testing.
     */
    public class MockFarragoSessionPersonality
        implements FarragoSessionPersonality
    {
        public MockFarragoSessionPersonality()
        {
            super();
        }

        public SqlOperatorTable getSqlOperatorTable(
            FarragoSessionPreparingStmt preparingStmt)
        {
            throw new UnsupportedOperationException();
        }

        public OJRexImplementorTable getOJRexImplementorTable(
            FarragoSessionPreparingStmt preparingStmt)
        {
            return getJavaImplementorTable();
        }

        public <C> C newComponentImpl(Class<C> componentClass)
        {
            if (componentClass == CalcRexImplementorTable.class) {
                return componentClass.cast(getFennelImplementorTable());
            }

            return null;
        }

        public String getDefaultLocalDataServerName(
            FarragoSessionStmtValidator stmtValidator)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionParser newParser(FarragoSession session)
        {
            return new FarragoParser();
        }

        public FarragoSessionPreparingStmt newPreparingStmt(
            FarragoSessionStmtContext stmtContext,
            FarragoSessionStmtContext rootStmtContext,
            FarragoSessionStmtValidator stmtValidator)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionPreparingStmt newPreparingStmt(
            FarragoSessionStmtContext stmtContext,
            FarragoSessionStmtValidator stmtValidator)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionPreparingStmt newPreparingStmt(
            FarragoSessionStmtValidator stmtValidator)
        {
            return stmtValidator.getSession().getPersonality().newPreparingStmt(
                null, stmtValidator);
        }

        public FarragoSessionDdlValidator newDdlValidator(
            FarragoSessionStmtValidator stmtValidator)
        {
            throw new UnsupportedOperationException();
        }

        public void defineDdlHandlers(
            FarragoSessionDdlValidator ddlValidator,
            List<DdlHandler> handlerList)
        {
            throw new UnsupportedOperationException();
        }

        public FarragoSessionPlanner newPlanner(
            FarragoSessionPreparingStmt stmt, boolean init)
        {
            // TODO: Create a planner. Use FarragoDefaultHeuristicPlanner?
            return null;
        }

        public void definePlannerListeners(
            FarragoSessionPlanner planner)
        {
        }

        public Class getRuntimeContextClass(
            FarragoSessionPreparingStmt stmt)
        {
            return FarragoRuntimeContext.class;
        }

        public FarragoSessionRuntimeContext newRuntimeContext(
            FarragoSessionRuntimeParams params)
        {
            throw new UnsupportedOperationException();
        }

        public RelDataTypeFactory newTypeFactory(
            FarragoRepos repos)
        {
            return new FarragoTypeFactoryImpl(repos);
        }

        public void loadDefaultSessionVariables(
            FarragoSessionVariables variables)
        {
            variables.set(
                FarragoDefaultSessionPersonality
                    .REDUCE_NON_CORRELATED_SUBQUERIES,
                FarragoDefaultSessionPersonality
                    .REDUCE_NON_CORRELATED_SUBQUERIES_FARRAGO_DEFAULT);
        }

        public FarragoSessionVariables createInheritedSessionVariables(
            FarragoSessionVariables variables)
        {
            throw new UnsupportedOperationException();
        }

        public void validateSessionVariable(
            FarragoSessionDdlValidator ddlValidator,
            FarragoSessionVariables variables,
            String name,
            String value)
        {
            throw new UnsupportedOperationException();
        }

        public boolean isAlterTableAddColumnIncremental()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isJavaUdxRestartable()
        {
            return false;               // imitate AspenSessionPersonality
        }

        public JmiQueryProcessor newJmiQueryProcessor(String language)
        {
            throw new UnsupportedOperationException();
        }

        public void registerStreamFactories(long hStreamGraph)
        {
            throw new UnsupportedOperationException();
        }

        public boolean isSupportedType(SqlTypeName type)
        {
            return true;
        }

        public void definePrivileges(
            FarragoSessionPrivilegeMap map)
        {
            throw new UnsupportedOperationException();
        }

        public boolean supportsFeature(ResourceDefinition feature)
        {
            return true;
        }

        public boolean shouldReplacePreserveOriginalSql()
        {
            return true;
        }

        public void registerRelMetadataProviders(
            ChainedRelMetadataProvider chain)
        {
            // no-op
        }

        public void getRowCounts(
            ResultSet resultSet,
            List<Long> rowCounts,
            TableModificationRel.Operation tableModOp) throws SQLException
        {
            throw new UnsupportedOperationException();
        }

        public long updateRowCounts(
            FarragoSession session,
            List<String> tableName,
            List<Long> rowCounts,
            TableModificationRel.Operation tableModOp,
            FarragoSessionRuntimeContext runningContext)
        {
            throw new UnsupportedOperationException();
        }

        public void resetRowCounts(FemAbstractColumnSet table)
        {
            throw new UnsupportedOperationException();
        }

        public void updateIndexRoot(
            FemLocalIndex index,
            FarragoDataWrapperCache wrapperCache,
            FarragoSessionIndexMap baseIndexMap,
            Long newRoot)
        {
            throw new UnsupportedOperationException();
        }

        public JmiJsonUtil newJmiJsonUtil()
        {
            throw new UnsupportedOperationException();
        }

        public DdlGenerator newDdlGenerator(
            SqlDialect sqlDialect,
            JmiModelView modelView)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Mock implementation of {@link net.sf.farrago.catalog.FarragoRepos} which
     * does not need an underlying MDR repository. Hence it is much faster to
     * start up.
     */
   public class MockRepos extends FarragoReposImpl
    {
        private final FemFarragoConfig config;
        private final MockFarragoMetadataFactory fmf;

        public MockRepos(FarragoAllocationOwner owner)
        {
            super(owner);
            // Create a mock metadata factory and steal its root element.
            fmf = new MockFarragoMetadataFactory();
            super.setRootPackage(fmf.getFarragoPackage());
            this.config = fmf.newFemFarragoConfig();
            this.config.setJavaCompilerClassName(
                JaninoCompiler.class.getName());
        }

        public MDRepository getMdrRepos()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoPackage getTransientFarragoPackage()
        {
            throw new UnsupportedOperationException();
        }

        public FemFarragoConfig getCurrentConfig()
        {
            return config;
        }

        public void beginReposTxn(boolean writable)
        {
            throw new UnsupportedOperationException();
        }

        public void endReposTxn(boolean rollback)
        {
            throw new UnsupportedOperationException();
        }

        public Object getMetadataFactory(String prefix)
        {
            if (prefix.equals("Fem")) {
                return fmf;
            }
            return super.getMetadataFactory(prefix);
        }

        public void closeAllocation()
        {
            throw new UnsupportedOperationException();
        }

        public FarragoModelLoader getModelLoader()
        {
            return null;
        }
    }

    /**
     * Extension of {@link FarragoDefaultSessionFactory}, for testing purposes,
     * which returns mock objects.
     */
    public class MockSessionFactory extends FarragoDefaultSessionFactory
    {
        public FarragoRepos newRepos(
            FarragoAllocationOwner owner,
            boolean userRepos)
        {
            return new MockRepos(owner);
        }

        // implement FarragoSessionPersonalityFactory
        public FarragoSessionPersonality newSessionPersonality(
            FarragoSession session,
            FarragoSessionPersonality defaultPersonality)
        {
            return new MockFarragoSessionPersonality();
        }
    }
}

// End MockSession.java
