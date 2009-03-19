/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.jdbc.engine;

import java.io.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.release.*;
import net.sf.farrago.session.*;

import org.eigenbase.jdbc4.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;


/**
 * FarragoJdbcEngineDatabaseMetaData implements the {@link
 * java.sql.DatabaseMetaData} interface with Farrago specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineDatabaseMetaData
    extends Unwrappable
    implements DatabaseMetaData
{
    //~ Instance fields --------------------------------------------------------

    private FarragoJdbcEngineConnection connection;
    private FarragoRepos repos;
    private String jdbcKeywords;

    //~ Constructors -----------------------------------------------------------

    protected FarragoJdbcEngineDatabaseMetaData(
        FarragoJdbcEngineConnection connection)
    {
        this.connection = connection;
        repos = connection.getSession().getRepos();
    }

    //~ Methods ----------------------------------------------------------------

    // implement DatabaseMetaData
    public boolean allProceduresAreCallable()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean allTablesAreSelectable()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public String getURL()
        throws SQLException
    {
        return connection.getSession().getUrl();
    }

    // implement DatabaseMetaData
    public String getUserName()
        throws SQLException
    {
        // REVIEW jvs 25-June-2005:  should this be the current user
        // or the session user?
        return connection.getSession().getSessionVariables().currentUserName;
    }

    // implement DatabaseMetaData
    public boolean isReadOnly()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedHigh()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedLow()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedAtStart()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedAtEnd()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public String getDatabaseProductName()
        throws SQLException
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.productName.get();
    }

    // implement DatabaseMetaData
    public String getDatabaseProductVersion()
        throws SQLException
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return ""
            + props.productVersionMajor.get()
            + "."
            + props.productVersionMinor.get()
            + "."
            + props.productVersionPoint.get();
    }

    // implement DatabaseMetaData
    public int getDatabaseMajorVersion()
        throws SQLException
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.productVersionMajor.get();
    }

    // implement DatabaseMetaData
    public int getDatabaseMinorVersion()
        throws SQLException
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.productVersionMinor.get();
    }

    // implement DatabaseMetaData
    public int getJDBCMajorVersion()
        throws SQLException
    {
        return 3;
    }

    // implement DatabaseMetaData
    public int getJDBCMinorVersion()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public String getDriverName()
        throws SQLException
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.jdbcDriverName.get();
    }

    // implement DatabaseMetaData
    public String getDriverVersion()
        throws SQLException
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return ""
            + props.jdbcDriverVersionMajor.get()
            + "."
            + props.jdbcDriverVersionMinor.get();
    }

    // implement DatabaseMetaData
    public int getDriverMajorVersion()
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.jdbcDriverVersionMajor.get();
    }

    // implement DatabaseMetaData
    public int getDriverMinorVersion()
    {
        FarragoReleaseProperties props = FarragoReleaseProperties.instance();
        return props.jdbcDriverVersionMinor.get();
    }

    // implement DatabaseMetaData
    public boolean usesLocalFiles()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean usesLocalFilePerTable()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMixedCaseIdentifiers()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesUpperCaseIdentifiers()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean storesLowerCaseIdentifiers()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesMixedCaseIdentifiers()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMixedCaseQuotedIdentifiers()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean storesUpperCaseQuotedIdentifiers()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesLowerCaseQuotedIdentifiers()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesMixedCaseQuotedIdentifiers()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public String getIdentifierQuoteString()
        throws SQLException
    {
        return "\"";
    }

    // implement DatabaseMetaData
    public String getSQLKeywords()
        throws SQLException
    {
        if (jdbcKeywords == null) {
            FarragoSessionParser parser =
                connection.getSession().getPersonality().newParser(
                    connection.getSession());
            jdbcKeywords = parser.getJdbcKeywords();
        }
        return jdbcKeywords;
    }

    // implement DatabaseMetaData
    public String getNumericFunctions()
        throws SQLException
    {
        return SqlJdbcFunctionCall.getNumericFunctions();
    }

    // implement DatabaseMetaData
    public String getStringFunctions()
        throws SQLException
    {
        return SqlJdbcFunctionCall.getStringFunctions();
    }

    // implement DatabaseMetaData
    public String getSystemFunctions()
        throws SQLException
    {
        return SqlJdbcFunctionCall.getSystemFunctions();
    }

    // implement DatabaseMetaData
    public String getTimeDateFunctions()
        throws SQLException
    {
        return SqlJdbcFunctionCall.getTimeDateFunctions();
    }

    // implement DatabaseMetaData
    public String getSearchStringEscape()
        throws SQLException
    {
        return "\\";
    }

    // implement DatabaseMetaData
    public String getExtraNameCharacters()
        throws SQLException
    {
        return "";
    }

    // implement DatabaseMetaData
    public boolean supportsAlterTableWithAddColumn()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsAlterTableWithDropColumn()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsColumnAliasing()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean nullPlusNonNullIsNull()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsConvert()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsConvert(
        int fromType,
        int toType)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsTableCorrelationNames()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsDifferentTableCorrelationNames()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsExpressionsInOrderBy()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOrderByUnrelated()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsGroupBy()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsGroupByUnrelated()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsGroupByBeyondSelect()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsLikeEscapeClause()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsMultipleResultSets()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMultipleTransactions()
        throws SQLException
    {
        return supportsTransactions();
    }

    // implement DatabaseMetaData
    public boolean supportsNonNullableColumns()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsMinimumSQLGrammar()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsCoreSQLGrammar()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsExtendedSQLGrammar()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsANSI92EntryLevelSQL()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsANSI92IntermediateSQL()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsANSI92FullSQL()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsIntegrityEnhancementFacility()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOuterJoins()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsFullOuterJoins()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsLimitedOuterJoins()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public String getSchemaTerm()
        throws SQLException
    {
        return "schema";
    }

    // implement DatabaseMetaData
    public String getProcedureTerm()
        throws SQLException
    {
        return "routine";
    }

    // implement DatabaseMetaData
    public String getCatalogTerm()
        throws SQLException
    {
        return "catalog";
    }

    // implement DatabaseMetaData
    public boolean isCatalogAtStart()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public String getCatalogSeparator()
        throws SQLException
    {
        return ".";
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInDataManipulation()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInProcedureCalls()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInTableDefinitions()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInIndexDefinitions()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInPrivilegeDefinitions()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInDataManipulation()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInProcedureCalls()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInTableDefinitions()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInIndexDefinitions()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInPrivilegeDefinitions()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsPositionedDelete()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsPositionedUpdate()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsSelectForUpdate()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsStoredProcedures()
        throws SQLException
    {
        // TODO jvs 23-Mar-2005:  need to support JDBC escape syntax
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInComparisons()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInExists()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInIns()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInQuantifieds()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCorrelatedSubqueries()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsUnion()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsUnionAll()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenCursorsAcrossCommit()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenCursorsAcrossRollback()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenStatementsAcrossCommit()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenStatementsAcrossRollback()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public int getMaxBinaryLiteralLength()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxCharLiteralLength()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnNameLength()
        throws SQLException
    {
        return repos.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInGroupBy()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInIndex()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInOrderBy()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInSelect()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInTable()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxConnections()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxCursorNameLength()
        throws SQLException
    {
        return repos.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxIndexLength()
        throws SQLException
    {
        // TODO
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxSchemaNameLength()
        throws SQLException
    {
        return repos.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxProcedureNameLength()
        throws SQLException
    {
        return repos.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxCatalogNameLength()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxRowSize()
        throws SQLException
    {
        // TODO
        return 0;
    }

    // implement DatabaseMetaData
    public boolean doesMaxRowSizeIncludeBlobs()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public int getMaxStatementLength()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxStatements()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxTableNameLength()
        throws SQLException
    {
        return repos.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxTablesInSelect()
        throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxUserNameLength()
        throws SQLException
    {
        return repos.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getDefaultTransactionIsolation()
        throws SQLException
    {
        if (supportsTransactions()) {
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        } else {
            return Connection.TRANSACTION_NONE;
        }
    }

    // implement DatabaseMetaData
    public boolean supportsTransactions()
        throws SQLException
    {
        return connection.getSession().getPersonality().supportsFeature(
            EigenbaseResource.instance().SQLFeature_E151);
    }

    // implement DatabaseMetaData
    public boolean supportsTransactionIsolationLevel(int level)
        throws SQLException
    {
        return supportsTransactions()
            && (level == Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    // implement DatabaseMetaData
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsDataManipulationTransactionsOnly()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean dataDefinitionCausesTransactionCommit()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean dataDefinitionIgnoredInTransactions()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public ResultSet getProcedures(
        String catalog,
        String schemaPattern,
        String procedureNamePattern)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.procedures_view");
        queryBuilder.addExact("procedure_cat", catalog);
        queryBuilder.addPattern("procedure_schem", schemaPattern);
        queryBuilder.addPattern("procedure_name", procedureNamePattern);
        queryBuilder.addOrderBy("procedure_schem,procedure_name,procedure_cat");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getProcedureColumns(
        String catalog,
        String schemaPattern,
        String procedureNamePattern,
        String columnNamePattern)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.procedure_columns_view");
        queryBuilder.addExact("procedure_cat", catalog);
        queryBuilder.addPattern("procedure_schem", schemaPattern);
        queryBuilder.addPattern("procedure_name", procedureNamePattern);
        queryBuilder.addPattern("column_name", columnNamePattern);
        queryBuilder.addOrderBy(
            "procedure_schem,procedure_name,column_ordinal,procedure_cat");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String [] types)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.tables_view");
        queryBuilder.addExact("table_cat", catalog);
        queryBuilder.addPattern("table_schem", schemaPattern);
        queryBuilder.addPattern("table_name", tableNamePattern);
        queryBuilder.addInList("table_type", types);
        queryBuilder.addOrderBy("table_type,table_schem,table_name,table_cat");
        return queryBuilder.execute();
    }

    /**
     * Creates a new QueryBuilder.
     */
    protected QueryBuilder createQueryBuilder(String base)
    {
        return new QueryBuilder(base);
    }

    /**
     * Executes a daemon query. Extensions should override this method to
     * provide alternate daemon implementations.
     */
    protected ResultSet executeDaemonQuery(String query)
        throws SQLException
    {
        Statement stmt = connection.createStatement();
        daemonize(stmt);
        return stmt.executeQuery(query);
    }

    private void daemonize(Statement stmt)
    {
        FarragoJdbcEngineStatement farragoStmt =
            (FarragoJdbcEngineStatement) stmt;
        farragoStmt.stmtContext.daemonize();
    }

    // implement DatabaseMetaData
    public ResultSet getSchemas()
        throws SQLException
    {
        return executeDaemonQuery(
            "select * from sys_boot.jdbc_metadata.schemas_view "
            + "order by table_schem,table_catalog");
    }

    // implement DatabaseMetaData
    public ResultSet getCatalogs()
        throws SQLException
    {
        return executeDaemonQuery(
            "select * from sys_boot.jdbc_metadata.catalogs_view "
            + "order by table_cat");
    }

    // implement DatabaseMetaData
    public ResultSet getTableTypes()
        throws SQLException
    {
        return executeDaemonQuery(
            "select * from sys_boot.jdbc_metadata.table_types_view "
            + "order by table_type");
    }

    // implement DatabaseMetaData
    public ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.columns_view");
        queryBuilder.addExact("table_cat", catalog);
        queryBuilder.addPattern("table_schem", schemaPattern);
        queryBuilder.addPattern("table_name", tableNamePattern);
        queryBuilder.addPattern("column_name", columnNamePattern);
        queryBuilder.addOrderBy(
            "table_schem,table_name,ordinal_position,table_cat");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getColumnPrivileges(
        String catalog,
        String schema,
        String table,
        String columnNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException("getColumnPrivileges");
    }

    // implement DatabaseMetaData
    public ResultSet getTablePrivileges(
        String catalog,
        String schemaPattern,
        String tableNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException("getTablePrivileges");
    }

    // implement DatabaseMetaData
    public ResultSet getBestRowIdentifier(
        String catalog,
        String schema,
        String table,
        int scope,
        boolean nullable)
        throws SQLException
    {
        throw new UnsupportedOperationException("getBestRowIdentifier");
    }

    // implement DatabaseMetaData
    public ResultSet getVersionColumns(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        throw new UnsupportedOperationException("getVersionColumns");
    }

    // implement DatabaseMetaData
    public ResultSet getPrimaryKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.primary_keys_view");
        queryBuilder.addExact("table_cat", catalog);
        queryBuilder.addExact("table_schem", schema);
        queryBuilder.addExact("table_name", table);
        queryBuilder.addOrderBy(
            "column_name, table_cat, table_schem, table_name");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getImportedKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.imported_keys_view");

        // For now, ignore all parameters because we always return
        // empty set.
        queryBuilder.addOrderBy(
            "pktable_cat, pktable_schem, pktable_name, key_seq");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getExportedKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.exported_keys_view");

        // For now, ignore all parameters because we always return
        // empty set.
        queryBuilder.addOrderBy(
            "fktable_cat, fktable_schem, fktable_name, key_seq");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getCrossReference(
        String primaryCatalog,
        String primarySchema,
        String primaryTable,
        String foreignCatalog,
        String foreignSchema,
        String foreignTable)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.cross_reference_view");

        // For now, ignore all parameters because we always return
        // empty set.
        queryBuilder.addOrderBy(
            "fktable_cat, fktable_schem, fktable_name, key_seq");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public ResultSet getTypeInfo()
        throws SQLException
    {
        return executeDaemonQuery(
            "select * from sys_boot.jdbc_metadata.type_info_view "
            + "order by data_type");
    }

    // implement DatabaseMetaData
    public ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.index_info_view");
        queryBuilder.addExact("table_cat", catalog);
        queryBuilder.addExact("table_schem", schema);
        queryBuilder.addExact("table_name", table);
        if (unique) {
            queryBuilder.addExact(
                "non_unique",
                false);
        }

        // TODO jvs 22-Oct-2005:  do something with parameter "approximate"
        // as part of implementing stats
        queryBuilder.addOrderBy(
            "non_unique, type, index_name, ordinal_position");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public boolean supportsResultSetType(int type)
        throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    // implement DatabaseMetaData
    public boolean supportsResultSetConcurrency(
        int type,
        int concurrency)
        throws SQLException
    {
        return (type == ResultSet.TYPE_FORWARD_ONLY)
            && (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    // implement DatabaseMetaData
    public boolean ownUpdatesAreVisible(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean ownDeletesAreVisible(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean ownInsertsAreVisible(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean othersUpdatesAreVisible(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean othersDeletesAreVisible(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean othersInsertsAreVisible(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean updatesAreDetected(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean deletesAreDetected(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean insertsAreDetected(int type)
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsBatchUpdates()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public ResultSet getUDTs(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        int [] types)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.udts_view");
        queryBuilder.addExact("type_cat", catalog);
        queryBuilder.addPattern("type_schem", schemaPattern);
        queryBuilder.addPattern("type_name", typeNamePattern);
        queryBuilder.addOrderBy("data_type,type_schem,type_name,type_cat");

        // TODO:  re-enable once IN is working
        /*
        queryBuilder.addInList("data_type",types);
         */
        if ((types != null) && (types.length == 1)) {
            queryBuilder.addExact(
                "data_type",
                types[0]);
        }

        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public Connection getConnection()
        throws SQLException
    {
        return connection;
    }

    // implement DatabaseMetaData
    public boolean supportsSavepoints()
        throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsNamedParameters()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMultipleOpenResults()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsGetGeneratedKeys()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public ResultSet getSuperTypes(
        String catalog,
        String schemaPattern,
        String typeNamePattern)
        throws SQLException
    {
        // For now, ignore all parameters because we always return
        // empty set.
        return executeDaemonQuery(
            "select * from sys_boot.jdbc_metadata.super_types_view");
    }

    // implement DatabaseMetaData
    public ResultSet getSuperTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern)
        throws SQLException
    {
        // For now, ignore all parameters because we always return
        // empty set.
        return executeDaemonQuery(
            "select * from sys_boot.jdbc_metadata.super_tables_view");
    }

    // implement DatabaseMetaData
    public ResultSet getAttributes(
        String catalog,
        String schemaPattern,
        String typeNamePattern,
        String attributeNamePattern)
        throws SQLException
    {
        QueryBuilder queryBuilder =
            createQueryBuilder(
                "select * from sys_boot.jdbc_metadata.attributes_view");
        queryBuilder.addExact("type_cat", catalog);
        queryBuilder.addPattern("type_schem", schemaPattern);
        queryBuilder.addPattern("type_name", typeNamePattern);
        queryBuilder.addPattern("attr_name", attributeNamePattern);
        queryBuilder.addOrderBy(
            "type_schem,type_name,ordinal_position,type_cat");
        return queryBuilder.execute();
    }

    // implement DatabaseMetaData
    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException
    {
        return holdability == getResultSetHoldability();
    }

    // implement DatabaseMetaData
    public int getResultSetHoldability()
        throws SQLException
    {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // implement DatabaseMetaData
    public int getSQLStateType()
        throws SQLException
    {
        // TODO
        return sqlStateXOpen;
    }

    // implement DatabaseMetaData
    public boolean locatorsUpdateCopy()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsStatementPooling()
        throws SQLException
    {
        return false;
    }

    //
    // begin JDBC 4 methods
    //

    // implement DatabaseMetaData
    public ResultSet getFunctions(
        String catalog,
        String schemaPattern,
        String functionNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException("getFunctions");
    }

    // implement DatabaseMetaData
    public ResultSet getFunctionColumns(
        String catalog,
        String schemaPattern,
        String functionNamePattern,
        String columnNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException("getFunctionColumns");
    }

    // implement DatabaseMetaData
    public ResultSet getClientInfoProperties()
        throws SQLException
    {
        throw new UnsupportedOperationException("getClientInfoProperties");
    }

    // implement DatabaseMetaData
    public boolean autoCommitFailureClosesAllResultSets()
        throws SQLException
    {
        throw new UnsupportedOperationException(
            "autoCommitFailureClosesAllResultSets");
    }

    // implement DatabaseMetaData
    public boolean supportsStoredFunctionsUsingCallSyntax()
        throws SQLException
    {
        throw new UnsupportedOperationException(
            "supportsStoredFunctionsUsingCallSyntax");
    }

    // implement DatabaseMetaData
    public ResultSet getSchemas(String catalog, String schemaPattern)
        throws SQLException
    {
        throw new UnsupportedOperationException("getSchemas");
    }

    // implement DatabaseMetaData
    public RowIdLifetime getRowIdLifetime()
        throws SQLException
    {
        throw new UnsupportedOperationException("getRowIdLifetime");
    }

    //~ Inner Classes ----------------------------------------------------------

    //
    // end JDBC 4 methods
    //

    /**
     * Helper class for building up queries used by metadata calls.
     */
    protected class QueryBuilder
    {
        protected StringBuilder sql;
        protected List<Serializable> values;
        private boolean whereAdded;

        protected QueryBuilder(String base)
        {
            sql = new StringBuilder(base);
            values = new ArrayList<Serializable>();
        }

        void addConjunction()
        {
            if (!whereAdded) {
                sql.append(" WHERE ");
            } else {
                sql.append(" AND ");
            }
            whereAdded = true;
        }

        void addPattern(
            String colName,
            String value)
        {
            if (value == null) {
                return;
            }
            if (value.equals("%")) {
                addConjunction();
                sql.append(colName);
                sql.append(" IS NOT NULL");
                return;
            }
            if (!repos.isFennelEnabled()) {
                // Without Fennel, we don't yet support LIKE, so just
                // try our best by ignoring the pattern and treating it
                // as an exact match.
                addExact(colName, value);
                return;
            }
            if ((value.indexOf('%') == -1) && (value.indexOf('_') == -1)) {
                // They didn't supply a pattern anyway.
                addExact(colName, value);
                return;
            }
            addConjunction();
            sql.append(colName);
            sql.append(" like ? escape '\\'");
            values.add(value);
        }

        void addExact(
            String colName,
            Serializable value)
        {
            if (value == null) {
                return;
            }
            addConjunction();
            sql.append(colName);
            sql.append(" = ?");
            values.add(value);
        }

        void addInList(
            String colName,
            String [] valueList)
        {
            if (valueList == null) {
                return;
            }
            addConjunction();
            sql.append(colName);
            sql.append(" in (");
            for (int i = 0; i < valueList.length; ++i) {
                if (i > 0) {
                    sql.append(",");
                }

                // REVIEW:  automatically uppercase?
                sql.append("'");
                sql.append(valueList[i]);
                sql.append("'");
            }
            sql.append(")");
        }

        void addOrderBy(String colList)
        {
            sql.append(" ORDER BY ");
            sql.append(colList);
        }

        protected ResultSet execute()
            throws SQLException
        {
            PreparedStatement stmt =
                connection.prepareStatement(sql.toString());
            daemonize(stmt);
            for (int i = 0; i < values.size(); ++i) {
                stmt.setObject(
                    i + 1,
                    values.get(i));
            }
            return stmt.executeQuery();
        }
    }
}

// End FarragoJdbcEngineDatabaseMetaData.java
