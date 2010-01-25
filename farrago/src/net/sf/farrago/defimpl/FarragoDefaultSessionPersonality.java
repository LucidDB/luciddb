/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package net.sf.farrago.defimpl;

import java.io.*;

import java.sql.*;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.db.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fennel.calc.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;

import org.eigenbase.jmi.*;
import org.eigenbase.lurql.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * FarragoDefaultSessionPersonality is a default implementation of the {@link
 * FarragoSessionPersonality} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDefaultSessionPersonality
    implements FarragoSessionPersonality
{
    //~ Static fields/initializers ---------------------------------------------

    // REVIEW jvs 8-May-2007:  These are referenced from various
    // places where it seems like macker should prevent the dependency.
    // Not sure why that is, but figure out a better place for them
    // (probably FarragoSessionVariables), leaving these here as aliases.

    /**
     * Numeric data from external data sources may have a greater precision than
     * Farrago. Whether data of greater precision should be replaced with null
     * when it overflows due to the greater precision.
     */
    public static final String SQUEEZE_JDBC_NUMERIC = "squeezeJdbcNumeric";
    public static final String SQUEEZE_JDBC_NUMERIC_DEFAULT = "true";

    /**
     * Whether statement caching is enabled for a session
     */
    public static final String CACHE_STATEMENTS = "cacheStatements";
    public static final String CACHE_STATEMENTS_DEFAULT = "true";

    /**
     * Whether DDL validation should be done at prepare time
     */
    public static final String VALIDATE_DDL_ON_PREPARE = "validateDdlOnPrepare";
    public static final String VALIDATE_DDL_ON_PREPARE_DEFAULT = "false";

    /**
     * Whether non-correlated subqueries should be converted to constants
     */
    public static final String REDUCE_NON_CORRELATED_SUBQUERIES =
        "reduceNonCorrelatedSubqueries";
    public static final String
        REDUCE_NON_CORRELATED_SUBQUERIES_FARRAGO_DEFAULT = "false";

    /**
     * Degree of parallelism to use for parallel executor; a value of 1 (the
     * default) causes the default non-parallel executor to be used.
     */
    public static final String DEGREE_OF_PARALLELISM = "degreeOfParallelism";
    public static final String DEGREE_OF_PARALLELISM_DEFAULT = "1";

    /**
     * The label for the current session
     */
    public static final String LABEL = "label";
    public static final String LABEL_DEFAULT = null;

    //~ Instance fields --------------------------------------------------------

    protected final FarragoDatabase database;
    protected final ParamValidator paramValidator;

    //~ Constructors -----------------------------------------------------------

    protected FarragoDefaultSessionPersonality(FarragoDbSession session)
    {
        database = session.getDatabase();

        paramValidator = new ParamValidator();
        paramValidator.registerBoolParam(
            SQUEEZE_JDBC_NUMERIC,
            false);
        paramValidator.registerBoolParam(
            CACHE_STATEMENTS,
            false);
        paramValidator.registerBoolParam(
            VALIDATE_DDL_ON_PREPARE,
            false);
        paramValidator.registerBoolParam(
            REDUCE_NON_CORRELATED_SUBQUERIES,
            false);
        paramValidator.registerStringParam(LABEL, true);
        paramValidator.registerIntParam(
            DEGREE_OF_PARALLELISM,
            false,
            1,
            Integer.MAX_VALUE);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPersonality
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init)
    {
        return FarragoDefaultHeuristicPlanner.newInstance(stmt);
    }

    // implement FarragoSessionPersonality
    public void definePlannerListeners(FarragoSessionPlanner planner)
    {
    }

    // implement FarragoStreamFactoryProvider
    public void registerStreamFactories(long hStreamGraph)
    {
        // we used to register C++ plugins for LucidEra and
        // SQLstream here, but now they are part of the
        // main fennel::ExecStreamFactory, so nothing to do
        // for the default personality.
    }

    // implement FarragoSessionPersonality
    public String getDefaultLocalDataServerName(
        FarragoSessionStmtValidator stmtValidator)
    {
        if (stmtValidator.getSession().getRepos().isFennelEnabled()) {
            return "SYS_FTRS_DATA_SERVER";
        } else {
            return "SYS_MOCK_DATA_SERVER";
        }
    }

    // implement FarragoSessionPersonality
    public boolean isAlterTableAddColumnIncremental()
    {
        return false;
    }

    public boolean isJavaUdxRestartable()
    {
        return true;
    }

    // implement FarragoSessionPersonality
    public SqlOperatorTable getSqlOperatorTable(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return SqlStdOperatorTable.instance();
    }

    // implement FarragoSessionPersonality
    public OJRexImplementorTable getOJRexImplementorTable(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return database.getOJRexImplementorTable();
    }

    // implement FarragoSessionPersonality
    public <C> C newComponentImpl(Class<C> componentInterface)
    {
        if (componentInterface == CalcRexImplementorTable.class) {
            return componentInterface.cast(CalcRexImplementorTableImpl.std());
        }

        return null;
    }

    // implement FarragoSessionPersonality
    public FarragoSessionParser newParser(FarragoSession session)
    {
        return new FarragoParser();
    }

    // implement FarragoSessionPersonality
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtValidator stmtValidator)
    {
        return newPreparingStmt(null, stmtValidator);
    }

    // implement FarragoSessionPersonality
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtContext stmtContext,
        FarragoSessionStmtValidator stmtValidator)
    {
        return newPreparingStmt(stmtContext, stmtContext, stmtValidator);
    }

    // implement FarragoSessionPersonality
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtContext stmtContext,
        FarragoSessionStmtContext rootStmtContext,
        FarragoSessionStmtValidator stmtValidator)
    {
        // NOTE: We don't use stmtContext here (except to obtain the SQL text),
        // and we don't pass it on to the
        // preparing statement, because that doesn't need to be aware of its
        // context. However, custom personalities may have a use for it, which
        // is why it is provided in the interface.
        String sql = (stmtContext == null) ? "?" : stmtContext.getSql();
        FarragoPreparingStmt stmt =
            new FarragoPreparingStmt(
                rootStmtContext,
                stmtValidator,
                sql);
        initPreparingStmt(stmt);
        return stmt;
    }

    protected void initPreparingStmt(FarragoPreparingStmt stmt)
    {
        FarragoSessionStmtValidator stmtValidator = stmt.getStmtValidator();
        FarragoSessionPlanner planner =
            stmtValidator.getSession().getPersonality().newPlanner(stmt, true);
        planner.setRuleDescExclusionFilter(
            stmtValidator.getSession().getOptRuleDescExclusionFilter());
        stmt.setPlanner(planner);
    }

    // implement FarragoSessionPersonality
    public FarragoSessionDdlValidator newDdlValidator(
        FarragoSessionStmtValidator stmtValidator)
    {
        return new DdlValidator(stmtValidator);
    }

    // implement FarragoSessionPersonality
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List<DdlHandler> handlerList)
    {
        // NOTE jvs 21-Jan-2005:  handlerList order matters here.
        // DdlRelationalHandler includes some catch-all methods for
        // superinterfaces which we only want to invoke when one of
        // the more specific handlers doesn't satisfied the request.
        DdlMedHandler medHandler = new DdlMedHandler(ddlValidator);
        DdlSecurityHandler securityHandler =
            new DdlSecurityHandler(ddlValidator);
        handlerList.add(medHandler);
        handlerList.add(new DdlRoutineHandler(ddlValidator));
        handlerList.add(securityHandler);
        handlerList.add(new DdlRelationalHandler(medHandler));

        // Define drop rules
        FarragoRepos repos =
            ddlValidator.getStmtValidator().getSession().getRepos();

        // When a table is dropped, all indexes on the table should also be
        // implicitly dropped.
        ddlValidator.defineDropRule(
            repos.getKeysIndexesPackage().getIndexSpansClass(),
            new FarragoSessionDdlDropRule(
                "spannedClass",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Dependencies can never be dropped without CASCADE, but with
        // CASCADE, they go away.
        ddlValidator.defineDropRule(
            repos.getCorePackage().getDependencySupplier(),
            new FarragoSessionDdlDropRule(
                "supplier",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));

        // When a dependency gets dropped, take its owner (the client)
        // down with it.
        ddlValidator.defineDropRule(
            repos.getCorePackage().getElementOwnership(),
            new FarragoSessionDdlDropRule(
                "ownedElement",
                CwmDependency.class,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Without CASCADE, a schema can only be dropped when it is empty.
        // This is not true for other namespaces (e.g. a table's constraints
        // are dropped implicitly), so we specify the superInterface filter.
        ddlValidator.defineDropRule(
            repos.getCorePackage().getElementOwnership(),
            new FarragoSessionDdlDropRule(
                "namespace",
                CwmSchema.class,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));

        // When a UDT is dropped, all routines which realize methods should
        // also be implicitly dropped.
        ddlValidator.defineDropRule(
            repos.getBehavioralPackage().getOperationMethod(),
            new FarragoSessionDdlDropRule(
                "specification",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Grants should be dropped together with any of the grantor, grantee,
        // or granted element
        ddlValidator.defineDropRule(
            repos.getSecurityPackage().getPrivilegeIsGrantedToGrantee(),
            new FarragoSessionDdlDropRule(
                "Grantee",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));
        ddlValidator.defineDropRule(
            repos.getSecurityPackage().getPrivilegeIsGrantedByGrantor(),
            new FarragoSessionDdlDropRule(
                "Grantor",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));
        ddlValidator.defineDropRule(
            repos.getSecurityPackage().getPrivilegeIsGrantedOnElement(),
            new FarragoSessionDdlDropRule(
                "Element",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Drop the corresponding label aliases if the cascade option
        // was specified
        ddlValidator.defineDropRule(
            repos.getMedPackage().getLabelHasAlias(),
            new FarragoSessionDdlDropRule(
                "ParentLabel",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));
    }

    // implement FarragoSessionPersonality
    public void definePrivileges(
        FarragoSessionPrivilegeMap map)
    {
        FarragoRepos repos = database.getSystemRepos();

        PrivilegedAction [] tableActions =
            new PrivilegedAction[] {
                PrivilegedActionEnum.SELECT,
                PrivilegedActionEnum.INSERT,
                PrivilegedActionEnum.DELETE,
                PrivilegedActionEnum.UPDATE,
            };
        defineTypePrivileges(
            map,
            repos.getRelationalPackage().getCwmNamedColumnSet(),
            tableActions);

        PrivilegedAction [] routineActions =
            new PrivilegedAction[] { PrivilegedActionEnum.EXECUTE };
        defineTypePrivileges(
            map,
            repos.getSql2003Package().getFemRoutine(),
            routineActions);
    }

    private void defineTypePrivileges(
        FarragoSessionPrivilegeMap map,
        RefClass refClass,
        PrivilegedAction [] actions)
    {
        for (PrivilegedAction action : actions) {
            map.mapPrivilegeForType(
                refClass,
                action.toString(),
                true,
                true);
        }
    }

    // implement FarragoSessionPersonality
    public Class getRuntimeContextClass(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return FarragoRuntimeContext.class;
    }

    // implement FarragoSessionPersonality
    public FarragoSessionRuntimeContext newRuntimeContext(
        FarragoSessionRuntimeParams params)
    {
        return new FarragoRuntimeContext(params);
    }

    // implement FarragoSessionPersonality
    public RelDataTypeFactory newTypeFactory(
        FarragoRepos repos)
    {
        return new FarragoTypeFactoryImpl(repos);
    }

    // implement FarragoSessionPersonality
    public void loadDefaultSessionVariables(
        FarragoSessionVariables variables)
    {
        variables.setDefault(
            SQUEEZE_JDBC_NUMERIC,
            SQUEEZE_JDBC_NUMERIC_DEFAULT);
        variables.setDefault(
            CACHE_STATEMENTS,
            CACHE_STATEMENTS_DEFAULT);
        variables.setDefault(
            VALIDATE_DDL_ON_PREPARE,
            VALIDATE_DDL_ON_PREPARE_DEFAULT);
        variables.setDefault(
            REDUCE_NON_CORRELATED_SUBQUERIES,
            REDUCE_NON_CORRELATED_SUBQUERIES_FARRAGO_DEFAULT);
        variables.setDefault(LABEL, LABEL_DEFAULT);
        variables.setDefault(
            DEGREE_OF_PARALLELISM,
            DEGREE_OF_PARALLELISM_DEFAULT);
    }

    // implement FarragoSessionPersonality
    public FarragoSessionVariables createInheritedSessionVariables(
        FarragoSessionVariables variables)
    {
        return variables.cloneVariables();
    }

    // implement FarragoSessionPersonality
    public void validateSessionVariable(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSessionVariables variables,
        String name,
        String value)
    {
        String validatedValue =
            paramValidator.validate(ddlValidator, name, value);
        variables.set(name, validatedValue);
    }

    // implement FarragoSessionPersonality
    public JmiQueryProcessor newJmiQueryProcessor(String language)
    {
        if (!language.equals("LURQL")) {
            return null;
        }
        return new LurqlQueryProcessor(
            database.getSystemRepos().getMdrRepos());
    }

    public boolean isSupportedType(SqlTypeName type)
    {
        if (type == null) {
            // Not a SQL type -- may be a structured type, such as MULTISET.
            return true;
        }
        switch (type) {
        case BOOLEAN:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case BIGINT:
        case VARCHAR:
        case VARBINARY:
        case MULTISET:
        case CHAR:
        case BINARY:
        case REAL:
        case FLOAT:
        case DOUBLE:
        case ROW:
        case DECIMAL:
            return true;
        case DISTINCT:
        default:
            return false;
        }
    }

    // implement FarragoSessionPersonality
    public boolean supportsFeature(ResourceDefinition feature)
    {
        // TODO jvs 20-Mar-2006: Fix this non-conforming behavior.  According
        // to the JDBC spec, each statement in an autocommit connection is
        // supposed to execute in its own private transaction.  Farrago's
        // support for this isn't done yet, so for now we prevent
        // multiple active statements on an autocommit connection
        // (unless a personality specifically enables it).
        ResourceDefinition maasFeature =
            EigenbaseResource.instance()
            .SQLConformance_MultipleActiveAutocommitStatements;
        if (feature == maasFeature) {
            return false;
        }

        // Farrago doesn't support MERGE
        if (feature == EigenbaseResource.instance().SQLFeature_F312) {
            return false;
        }

        // Farrago doesn't automatically update row counts
        if (feature
            == EigenbaseResource.instance().PersonalityManagesRowCount)
        {
            return false;
        }

        // Farrago doesn't support snapshots
        if (feature
            == EigenbaseResource.instance().PersonalitySupportsSnapshots)
        {
            return false;
        }

        if (feature == EigenbaseResource.instance().PersonalitySupportsLabels) {
            return false;
        }

        // By default, support everything except the above.
        return true;
    }

    // implement FarragoSessionPersonality
    public boolean shouldReplacePreserveOriginalSql()
    {
        return true;
    }

    // implement FarragoSessionPersonality
    public void registerRelMetadataProviders(ChainedRelMetadataProvider chain)
    {
        // Don't chain in FarragoRelMetadataProvider here; instead,
        // that happens inside of FarragoPreparingStmt so that
        // this provider gets low priority.
    }

    // implement FarragoSessionPersonality
    public void getRowCounts(
        ResultSet resultSet,
        List<Long> rowCounts,
        TableModificationRel.Operation tableModOp)
        throws SQLException
    {
        boolean found = resultSet.next();
        assert (found);
        boolean nextRowCount = addRowCount(resultSet, rowCounts);
        if ((tableModOp == TableModificationRel.Operation.INSERT)
            && nextRowCount)
        {
            // if the insert is on a column store table, a second rowcount
            // may be returned, indicating the number of insert violations
            nextRowCount = addRowCount(resultSet, rowCounts);
        }
        assert (!nextRowCount);
    }

    protected boolean addRowCount(ResultSet resultSet, List<Long> rowCounts)
        throws SQLException
    {
        rowCounts.add(resultSet.getLong(1));
        return resultSet.next();
    }

    // implement FarragoSessionPersonality
    public long updateRowCounts(
        FarragoSession session,
        List<String> tableName,
        List<Long> rowCounts,
        TableModificationRel.Operation tableModOp,
        FarragoSessionRuntimeContext runningContext)
    {
        long count = rowCounts.get(0);
        if (tableModOp == TableModificationRel.Operation.INSERT) {
            if (rowCounts.size() == 2) {
                count -= rowCounts.get(1);
            }
        }
        return count;
    }

    // implement FarragoSessionPersonality
    public void resetRowCounts(FemAbstractColumnSet table)
    {
    }

    // implement FarragoSessionPersonality
    public void updateIndexRoot(
        FemLocalIndex index,
        FarragoDataWrapperCache wrapperCache,
        FarragoSessionIndexMap baseIndexMap,
        Long newRoot)
    {
        // Drop old roots and update references to point to new roots
        baseIndexMap.dropIndexStorage(wrapperCache, index, false);
        baseIndexMap.setIndexRoot(index, newRoot);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * ParamDesc represents a session parameter descriptor
     */
    private class ParamDesc
    {
        int type;
        boolean nullability;
        Long rangeStart, rangeEnd;

        public ParamDesc(int type, boolean nullability)
        {
            this.type = type;
            this.nullability = nullability;
        }

        public ParamDesc(int type, boolean nullability, long start, long end)
        {
            this.type = type;
            this.nullability = nullability;
            rangeStart = start;
            rangeEnd = end;
        }
    }

    /**
     * ParamValidator is a basic session parameter validator
     */
    public class ParamValidator
    {
        private static final int BOOLEAN_TYPE = 1;
        private static final int INT_TYPE = 2;
        private static final int STRING_TYPE = 3;
        private static final int DIRECTORY_TYPE = 4;
        private static final int LONG_TYPE = 5;

        private Map<String, ParamDesc> params;

        public ParamValidator()
        {
            params = new HashMap<String, ParamDesc>();
        }

        public void registerBoolParam(String name, boolean nullability)
        {
            params.put(name, new ParamDesc(BOOLEAN_TYPE, nullability));
        }

        public void registerIntParam(String name, boolean nullability)
        {
            params.put(name, new ParamDesc(INT_TYPE, nullability));
        }

        public void registerIntParam(
            String name,
            boolean nullability,
            int start,
            int end)
        {
            assert (start <= end);
            params.put(name, new ParamDesc(INT_TYPE, nullability, start, end));
        }

        public void registerLongParam(
            String name,
            boolean nullability,
            long start,
            long end)
        {
            assert (start <= end);
            params.put(name, new ParamDesc(LONG_TYPE, nullability, start, end));
        }

        public void registerStringParam(String name, boolean nullability)
        {
            params.put(name, new ParamDesc(STRING_TYPE, nullability));
        }

        public void registerDirectoryParam(String name, boolean nullability)
        {
            params.put(name, new ParamDesc(DIRECTORY_TYPE, nullability));
        }

        public String validate(
            FarragoSessionDdlValidator ddlValidator,
            String name,
            String value)
        {
            if (!params.containsKey(name)) {
                throw FarragoResource.instance().ValidatorUnknownSysParam.ex(
                    ddlValidator.getRepos().getLocalizedObjectName(name));
            }
            ParamDesc paramDesc = params.get(name);
            if (!paramDesc.nullability && (value == null)) {
                throw FarragoResource.instance().ValidatorSysParamTypeMismatch
                .ex(
                    value,
                    ddlValidator.getRepos().getLocalizedObjectName(name));
            } else if (value == null) {
                return null;
            }

            // If this is the label variable, make sure snapshots are enabled.
            if (name.equals(FarragoDefaultSessionPersonality.LABEL)) {
                if (!supportsFeature(
                        EigenbaseResource.instance()
                        .PersonalitySupportsSnapshots))
                {
                    throw EigenbaseResource.instance()
                    .PersonalitySupportsSnapshots.ex();
                }
            }

            Object o;
            switch (paramDesc.type) {
            case BOOLEAN_TYPE:
                o = ConversionUtil.toBoolean(value);
                break;
            case INT_TYPE:
                o = Integer.valueOf(value);
                if (paramDesc.rangeStart != null) {
                    Integer i = (Integer) o;
                    if ((i < paramDesc.rangeStart)
                        || (i > paramDesc.rangeEnd))
                    {
                        throw FarragoResource.instance()
                        .ParameterValueOutOfRange.ex(value, name);
                    }
                }
                break;
            case LONG_TYPE:
                o = Long.valueOf(value);
                if (paramDesc.rangeStart != null) {
                    Long l = (Long) o;
                    if ((l < paramDesc.rangeStart)
                        || (l > paramDesc.rangeEnd))
                    {
                        throw FarragoResource.instance()
                        .ParameterValueOutOfRange.ex(value, name);
                    }
                }
                break;
            case STRING_TYPE:
                o = value;
                break;
            case DIRECTORY_TYPE:
                File dir = new File(value);
                if ((!dir.exists()) || (!dir.isDirectory())) {
                    throw FarragoResource.instance().InvalidDirectory.ex(
                        value);
                }
                if (!dir.canWrite()) {
                    throw FarragoResource.instance().FileWriteFailed.ex(value);
                }
                o = dir.getPath();
                break;
            default:
                throw Util.newInternal("invalid param type");
            }
            return o.toString();
        }
    }
}

// End FarragoDefaultSessionPersonality.java
