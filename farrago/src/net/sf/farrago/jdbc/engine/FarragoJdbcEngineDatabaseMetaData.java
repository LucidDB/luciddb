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

package net.sf.farrago.jdbc.engine;

import net.sf.farrago.catalog.*;
import net.sf.farrago.parser.NonReservedKeyword;

import java.sql.*;

import java.util.*;


/**
 * FarragoJdbcEngineDatabaseMetaData implements the {@link
 * java.sql.DatabaseMetaData} interface with Farrago specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineDatabaseMetaData implements DatabaseMetaData
{
    private FarragoJdbcEngineConnection connection;
    private FarragoCatalog catalog;
    
    FarragoJdbcEngineDatabaseMetaData(FarragoJdbcEngineConnection connection)
    {
        this.connection = connection;
        catalog = connection.getSession().getCatalog();
    }
    
    // implement DatabaseMetaData
    public boolean allProceduresAreCallable() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean allTablesAreSelectable() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public String getURL() throws SQLException
    {
        return connection.getSession().getUrl();
    }

    // implement DatabaseMetaData
    public String getUserName() throws SQLException
    {
        // TODO
        return "jackalope";
    }

    // implement DatabaseMetaData
    public boolean isReadOnly() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedHigh() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedLow() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedAtStart() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean nullsAreSortedAtEnd() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public String getDatabaseProductName() throws SQLException
    {
        return "Farrago";
    }

    // implement DatabaseMetaData
    public String getDatabaseProductVersion() throws SQLException
    {
        return "0.1";
    }

    // implement DatabaseMetaData
    public String getDriverName() throws SQLException
    {
        return "FarragoJdbcDriver";
    }

    // implement DatabaseMetaData
    public String getDriverVersion() throws SQLException
    {
        return "0.1";
    }

    // implement DatabaseMetaData
    public int getDriverMajorVersion()
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getDriverMinorVersion()
    {
        return 1;
    }

    // implement DatabaseMetaData
    public boolean usesLocalFiles() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean usesLocalFilePerTable() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public String getIdentifierQuoteString() throws SQLException
    {
        return "\"";
    }

    // implement DatabaseMetaData
    public String getSQLKeywords() throws SQLException
    {
       return NonReservedKeyword.nonRservedKeywords;
    }

    // implement DatabaseMetaData
    public String getNumericFunctions() throws SQLException
    {
        // TODO:  get these from catalog
        return "";
    }

    // implement DatabaseMetaData
    public String getStringFunctions() throws SQLException
    {
        // TODO:  get these from catalog
        return "";
    }

    // implement DatabaseMetaData
    public String getSystemFunctions() throws SQLException
    {
        // TODO:  get these from catalog
        return "";
    }

    // implement DatabaseMetaData
    public String getTimeDateFunctions() throws SQLException
    {
        // TODO:  get these from catalog
        return "";
    }

    // implement DatabaseMetaData
    public String getSearchStringEscape() throws SQLException
    {
        // REVIEW
        return null;
    }

    // implement DatabaseMetaData
    public String getExtraNameCharacters() throws SQLException
    {
        return "";
    }

    // implement DatabaseMetaData
    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsColumnAliasing() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean nullPlusNonNullIsNull() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsConvert() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsConvert(
        int fromType,int toType) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsTableCorrelationNames() throws SQLException
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
    public boolean supportsExpressionsInOrderBy() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOrderByUnrelated() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsGroupBy() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsGroupByUnrelated() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsGroupByBeyondSelect() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsLikeEscapeClause() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMultipleResultSets() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMultipleTransactions() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsNonNullableColumns() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsANSI92FullSQL() throws SQLException
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
    public boolean supportsOuterJoins() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsFullOuterJoins() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public String getSchemaTerm() throws SQLException
    {
        return "schema";
    }

    // implement DatabaseMetaData
    public String getProcedureTerm() throws SQLException
    {
        return "";
    }

    // implement DatabaseMetaData
    public String getCatalogTerm() throws SQLException
    {
        return "catalog";
    }

    // implement DatabaseMetaData
    public boolean isCatalogAtStart() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public String getCatalogSeparator() throws SQLException
    {
        return ".";
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSchemasInIndexDefinitions() throws SQLException
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
    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsCatalogsInPrivilegeDefinitions()
        throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsPositionedDelete() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsPositionedUpdate() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsSelectForUpdate() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsStoredProcedures() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInComparisons() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInExists() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInIns() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsCorrelatedSubqueries() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsUnion() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsUnionAll() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
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
    public int getMaxBinaryLiteralLength() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxCharLiteralLength() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnNameLength() throws SQLException
    {
        return catalog.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInGroupBy() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInIndex() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInOrderBy() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInSelect() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxColumnsInTable() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxConnections() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxCursorNameLength() throws SQLException
    {
        return catalog.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxIndexLength() throws SQLException
    {
        // TODO
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxSchemaNameLength() throws SQLException
    {
        return catalog.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxProcedureNameLength() throws SQLException
    {
        return catalog.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxCatalogNameLength() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxRowSize() throws SQLException
    {
        // TODO
        return 0;
    }

    // implement DatabaseMetaData
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public int getMaxStatementLength() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxStatements() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxTableNameLength() throws SQLException
    {
        return catalog.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getMaxTablesInSelect() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getMaxUserNameLength() throws SQLException
    {
        return catalog.getIdentifierPrecision();
    }

    // implement DatabaseMetaData
    public int getDefaultTransactionIsolation() throws SQLException
    {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    // implement DatabaseMetaData
    public boolean supportsTransactions() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsTransactionIsolationLevel(int level)
        throws SQLException
    {
        return level == Connection.TRANSACTION_READ_UNCOMMITTED;
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
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public ResultSet getProcedures(
        String catalog, String schemaPattern,
        String procedureNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getProcedureColumns(
        String catalog,
        String schemaPattern,
        String procedureNamePattern,
        String columnNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getTables(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String types[]) throws SQLException
    {
        QueryBuilder queryBuilder = new QueryBuilder(
            "select * from sys_boot.jdbc_metadata.tables_view");
        queryBuilder.addExact("table_cat",catalog);
        queryBuilder.addPattern("table_schem",schemaPattern);
        queryBuilder.addPattern("table_name",tableNamePattern);
        // TODO:  re-enable once IN is working
        /*
        queryBuilder.addInList("table_type",types);
        */
        queryBuilder.addOrderBy("table_type,table_schem,table_name,table_cat");
        return queryBuilder.execute();
    }

    private Statement newDaemonStatement() throws SQLException
    {
        Statement stmt = connection.createStatement();
        daemonize(stmt);
        return stmt;
    }

    private void daemonize(Statement stmt)
    {
        FarragoJdbcEngineStatement farragoStmt =
            (FarragoJdbcEngineStatement) stmt;
        farragoStmt.stmtContext.daemonize();
    }

    // implement DatabaseMetaData
    public ResultSet getSchemas() throws SQLException
    {
        return newDaemonStatement().executeQuery(
            "select * from sys_boot.jdbc_metadata.schemas_view "
            + "order by table_schem,table_catalog");
    }

    // implement DatabaseMetaData
    public ResultSet getCatalogs() throws SQLException
    {
        return newDaemonStatement().executeQuery(
            "select * from sys_boot.jdbc_metadata.catalogs_view "
            + "order by table_cat");
    }

    // implement DatabaseMetaData
    public ResultSet getTableTypes() throws SQLException
    {
        return newDaemonStatement().executeQuery(
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
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getColumnPrivileges(
        String catalog,
        String schema,
        String table,
        String columnNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getTablePrivileges(
        String catalog,
        String schemaPattern,
        String tableNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getBestRowIdentifier(
        String catalog,
        String schema,
        String table, int scope,
        boolean nullable)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getVersionColumns(
        String catalog, String schema,
        String table)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getPrimaryKeys(
        String catalog, String schema,
        String table)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getImportedKeys(
        String catalog, String schema,
        String table)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getExportedKeys(
        String catalog, String schema,
        String table)
        throws SQLException
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getTypeInfo() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getIndexInfo(
        String catalog, String schema,
        String table, boolean unique,
        boolean approximate) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public boolean supportsResultSetType(int type) throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    // implement DatabaseMetaData
    public boolean supportsResultSetConcurrency(
        int type,
        int concurrency) throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY
            && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    // implement DatabaseMetaData
    public boolean ownUpdatesAreVisible(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean ownDeletesAreVisible(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean ownInsertsAreVisible(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean othersUpdatesAreVisible(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean othersDeletesAreVisible(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean othersInsertsAreVisible(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean updatesAreDetected(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean deletesAreDetected(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean insertsAreDetected(int type) throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsBatchUpdates() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public ResultSet getUDTs(
        String catalog, String schemaPattern,
        String typeNamePattern,
        int[] types) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public Connection getConnection() throws SQLException
    {
        return connection;
    }

    // implement DatabaseMetaData
    public boolean supportsSavepoints() throws SQLException
    {
        return true;
    }

    // implement DatabaseMetaData
    public boolean supportsNamedParameters() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsMultipleOpenResults() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsGetGeneratedKeys() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public ResultSet getSuperTypes(
        String catalog, String schemaPattern,
        String typeNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getSuperTables(
        String catalog, String schemaPattern,
        String tableNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public ResultSet getAttributes(
        String catalog, String schemaPattern,
        String typeNamePattern,
        String attributeNamePattern)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement DatabaseMetaData
    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException
    {
        return holdability == getResultSetHoldability();
    }

    // implement DatabaseMetaData
    public int getResultSetHoldability() throws SQLException
    {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    // implement DatabaseMetaData
    public int getDatabaseMajorVersion() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getDatabaseMinorVersion() throws SQLException
    {
        return 1;
    }

    // implement DatabaseMetaData
    public int getJDBCMajorVersion() throws SQLException
    {
        return 3;
    }

    // implement DatabaseMetaData
    public int getJDBCMinorVersion() throws SQLException
    {
        return 0;
    }

    // implement DatabaseMetaData
    public int getSQLStateType() throws SQLException
    {
        // TODO
        return sqlStateXOpen;
    }

    // implement DatabaseMetaData
    public boolean locatorsUpdateCopy() throws SQLException
    {
        return false;
    }

    // implement DatabaseMetaData
    public boolean supportsStatementPooling() throws SQLException
    {
        return false;
    }

    /**
     * Helper class for building up queries used by metadata calls.
     */
    private class QueryBuilder 
    {
        StringBuffer sql;
        List values;
        
        QueryBuilder(String base)
        {
            sql = new StringBuffer(base);
            values = new ArrayList();
        }

        void addConjunction()
        {
            if (values.isEmpty()) {
                sql.append(" WHERE ");
            } else {
                sql.append(" AND ");
            }
        }

        void addPattern(String colName,String value)
        {
            if (value == null || value.equals("%")) {
                return;
            }
            if (value.equals("%")) {
                // FIXME:  this is incorrect since it's supposed to
                // be equivalent to IS NOT NULL.  Just here to be able
                // to use metadata from sqlline; remove once LIKE is working.
                return;
            }
            addConjunction();
            sql.append(colName);
            sql.append(" like ?");
            values.add(value);
        }

        void addExact(String colName,String value)
        {
            if (value == null) {
                return;
            }
            addConjunction();
            sql.append(colName);
            sql.append(" = ?");
            values.add(value);
        }

        void addInList(String colName,String [] valueList)
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

        ResultSet execute() throws SQLException
        {
            PreparedStatement stmt = connection.prepareStatement(
                sql.toString());
            daemonize(stmt);
            for (int i = 0; i < values.size(); ++i) {
                stmt.setObject(i+1,values.get(i));
            }
            return stmt.executeQuery();
        }
    }
}

// End FarragoJdbcEngineDatabaseMetaData.java
