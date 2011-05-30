/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
package net.sf.farrago.syslib;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * FarragoMedUDR is a set of user-defined routines providing access to SQL/MED
 * information.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoMedUDR
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Tests that a connection can be established to a particular SQL/MED local
     * or foreign data server. If no exception is thrown, the test was
     * successful.
     *
     * @param serverName name of data server to test
     */
    public static void testServer(
        String serverName)
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            FemDataServer femServer =
                stmtValidator.findDataServer(
                    new SqlIdentifier(serverName, SqlParserPos.ZERO));
            stmtValidator.getDataWrapperCache().loadServerFromCatalog(
                femServer);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    /**
     * Tests that a connection can be established for all SQL/MED servers
     * instantiated from a particular data wrapper. If no exception is thrown,
     * the test was successful.
     *
     * @param wrapperName name of data wrapper to test
     */
    public static void testAllServersForWrapper(
        String wrapperName)
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            FemDataWrapper femWrapper =
                stmtValidator.findDataWrapper(
                    new SqlIdentifier(wrapperName, SqlParserPos.ZERO), true);
            for (FemDataServer femServer : femWrapper.getServer()) {
                try {
                    stmtValidator.getDataWrapperCache().loadServerFromCatalog(
                        femServer);
                } catch (Throwable ex) {
                    throw FarragoResource.instance().ServerTestConnFailed.ex(
                        femServer.getName(),
                        ex);
                }
            }
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    /**
     * Queries SQL/MED connection information for a foreign data server.
     *
     * @param wrapperName name of foreign data wrapper to use
     * @param serverOptions table of option NAME/VALUE pairs to use; this can be
     * empty to query for all options
     * @param resultInserter writes result data set
     */
    public static void browseConnectServer(
        String wrapperName,
        ResultSet serverOptions,
        PreparedStatement resultInserter)
        throws SQLException
    {
        // Convert serverOptions into a Properties object
        Properties serverProps = toProperties(serverOptions);

        // TODO: pass in locale name as a parameter
        final Locale locale = Locale.getDefault();

        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            browseConnectServerImpl(
                stmtValidator,
                locale,
                wrapperName,
                serverProps,
                resultInserter);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    private static void browseConnectServerImpl(
        FarragoSessionStmtValidator stmtValidator,
        Locale locale,
        String wrapperName,
        Properties serverProps,
        PreparedStatement resultInserter)
        throws SQLException
    {
        FemDataWrapper femWrapper =
            stmtValidator.findDataWrapper(
                new SqlIdentifier(wrapperName, SqlParserPos.ZERO),
                true);
        FarragoDataWrapperCache wrapperCache =
            stmtValidator.getDataWrapperCache();
        Properties wrapperProps =
            wrapperCache.getStorageOptionsAsProperties(
                femWrapper);
        FarragoMedDataWrapper medWrapper =
            wrapperCache.loadWrapperFromCatalog(femWrapper);
        DriverPropertyInfo [] infoArray =
            medWrapper.getServerPropertyInfo(
                locale,
                wrapperProps,
                serverProps);
        populateResult(resultInserter, infoArray);
    }

    /**
     * Queries SQL/MED connection information for a foreign schema.
     *
     * @param serverName name of foreign server to use
     * @param resultInserter writes result data set
     */
    public static void browseForeignSchemas(
        String serverName,
        PreparedStatement resultInserter)
        throws SQLException
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            browseForeignSchemasImpl(
                stmtValidator,
                serverName,
                resultInserter);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    private static void browseForeignSchemasImpl(
        FarragoSessionStmtValidator stmtValidator,
        String serverName,
        PreparedStatement resultInserter)
        throws SQLException
    {
        FemDataServer femServer =
            stmtValidator.findDataServer(
                new SqlIdentifier(serverName, SqlParserPos.ZERO));

        FarragoMedDataServer medServer =
            stmtValidator.getDataWrapperCache().loadServerFromCatalog(
                femServer);

        FarragoMedNameDirectory dir = medServer.getNameDirectory();
        if (dir == null) {
            return;
        }
        FarragoMedMetadataQuery query = new MedMetadataQueryImpl();
        query.getResultObjectTypes().add(FarragoMedMetadataQuery.OTN_SCHEMA);
        FarragoMedMetadataSink sink =
            new BrowseSchemaSink(
                query,
                serverName,
                resultInserter);
        dir.queryMetadata(query, sink);
    }

    /**
     * Queries SQL/MED information for a wrapper.
     *
     * @param wrapperName name of foreign data wrapper to use
     * @param wrapperOptions table of option NAME/VALUE pairs to use; this can
     *     be empty to query for all options
     * @param localeName Name of locale (e.g. 'en-US') or NULL to use server's
     *     default locale
     * @param resultInserter writes result data set
     */
    public static void browseWrapper(
        String wrapperName,
        ResultSet wrapperOptions,
        String localeName,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties wrapperProps = toProperties(wrapperOptions);
        final Locale locale = toLocale(localeName);

        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            browseWrapperImpl(
                stmtValidator,
                locale,
                wrapperName,
                wrapperProps,
                resultInserter);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    private static void browseWrapperImpl(
        FarragoSessionStmtValidator stmtValidator,
        Locale locale,
        String wrapperName,
        Properties wrapperProps,
        PreparedStatement resultInserter)
        throws SQLException
    {
        FemDataWrapper femWrapper =
            stmtValidator.findDataWrapper(
                new SqlIdentifier(wrapperName, SqlParserPos.ZERO),
                true);
        FarragoDataWrapperCache wrapperCache =
            stmtValidator.getDataWrapperCache();

        // NOTE: By design, existing wrapper options are ignored.
        Util.discard(
            wrapperCache.getStorageOptionsAsProperties(
                femWrapper));

        FarragoMedDataWrapper medWrapper =
            wrapperCache.loadWrapperFromCatalog(femWrapper);
        DriverPropertyInfo [] infoArray =
            medWrapper.getPluginPropertyInfo(
                locale,
                wrapperProps);
        populateResult(resultInserter, infoArray);
    }

    /**
     * Queries SQL/MED information for a table.
     *
     * <p>You might think that this method would take the name of the schema and
     * table, but the table may not yet exist. The foreign
     * server must, so the foreign server's name is passed in. Instead of the
     * actual table, its options are passed in, and these provide
     * sufficient information for the SQL/MED wrapper to describe what options
     * are valid for the table.
     *
     * @param serverName Name of the foreign server that contains the table
     * @param tableOptions Options for the table,
     *     represented as a table of option NAME/VALUE pairs;
     *     this can be empty to query for all options
     * @param localeName Name of locale (e.g. 'en-US') or NULL to use server's
     *     default locale
     * @param resultInserter writes result data set
     */
    public static void browseTable(
        String serverName,
        ResultSet tableOptions,
        String localeName,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties tableProps = toProperties(tableOptions);
        final Locale locale = toLocale(localeName);

        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            browseTableImpl(
                stmtValidator,
                locale,
                serverName,
                tableProps,
                resultInserter);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    private static void browseTableImpl(
        FarragoSessionStmtValidator stmtValidator,
        Locale locale,
        String serverName,
        Properties tableProps,
        PreparedStatement resultInserter)
        throws SQLException
    {
        final FarragoDataWrapperCache wrapperCache =
            stmtValidator.getDataWrapperCache();

        final FemDataServer femDataServer =
            stmtValidator.findDataServer(
                new SqlIdentifier(serverName, SqlParserPos.ZERO));
        final Properties serverProps =
            wrapperCache.getStorageOptionsAsProperties(
                femDataServer);

        final FemDataWrapper femWrapper = femDataServer.getWrapper();
        final Properties wrapperProps =
            wrapperCache.getStorageOptionsAsProperties(
                femWrapper);

        FarragoMedDataWrapper medWrapper =
            wrapperCache.loadWrapperFromCatalog(femWrapper);
        DriverPropertyInfo [] infoArray =
            medWrapper.getColumnSetPropertyInfo(
                locale,
                wrapperProps,
                serverProps,
                tableProps);
        populateResult(resultInserter, infoArray);
    }

    /**
     * Queries SQL/MED to find allowable properties for a column.
     *
     * <p>You might think that this method would take the name of the schema,
     * table and column, but the table and column may not yet exist. The foreign
     * server must, so the foreign server's name is passed in. Instead of the
     * actual table and column, their options are passed in, and these provide
     * sufficient information for the SQL/MED wrapper to describe what options
     * are valid for the column.
     *
     * @param serverName Name of the foreign server that contains the table that
     *     contains this column
     * @param tableOptions Options for the table that contains this column,
     *     represented as a table of option NAME/VALUE pairs;
     *     this can be empty to query for all options
     * @param columnOptions Options for the column,
     *     represented as a table of of option NAME/VALUE pairs;
     *     this can be empty to query for all options
     * @param localeName Name of locale (e.g. 'en-US') or NULL to use server's
     *     default locale
     * @param resultInserter writes result data set
     */
    public static void browseColumn(
        String serverName,
        ResultSet tableOptions,
        ResultSet columnOptions,
        String localeName,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties columnProps = toProperties(columnOptions);
        Properties tableProps = toProperties(tableOptions);
        final Locale locale = toLocale(localeName);

        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn =
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            browseColumnImpl(
                stmtValidator, locale, serverName, tableProps, columnProps,
                resultInserter);
        } finally {
            txn.commit();
            stmtValidator.closeAllocation();
        }
    }

    private static void browseColumnImpl(
        FarragoSessionStmtValidator stmtValidator,
        Locale locale,
        String serverName,
        Properties tableProps,
        Properties columnProps,
        PreparedStatement resultInserter)
        throws SQLException
    {
        FarragoDataWrapperCache wrapperCache =
            stmtValidator.getDataWrapperCache();

        final FemDataServer femDataServer =
            stmtValidator.findDataServer(
                new SqlIdentifier(serverName, SqlParserPos.ZERO));
        final Properties serverProps =
            wrapperCache.getStorageOptionsAsProperties(
                femDataServer);

        final FemDataWrapper femWrapper = femDataServer.getWrapper();
        final Properties wrapperProps =
            wrapperCache.getStorageOptionsAsProperties(
                femWrapper);

        FarragoMedDataWrapper medWrapper =
            wrapperCache.loadWrapperFromCatalog(femWrapper);
        DriverPropertyInfo [] infoArray =
            medWrapper.getColumnPropertyInfo(
                locale,
                wrapperProps,
                serverProps,
                tableProps,
                columnProps);
        populateResult(resultInserter, infoArray);
    }

    /**
     * Converts a result set, containing name/value pairs, into a properties
     * object.
     *
     * @param options Result set of name/value pairs
     * @return Properties object
     * @throws SQLException
     */
    protected static Properties toProperties(
        ResultSet options)
        throws SQLException
    {
        Properties properties = new Properties();
        while (options.next()) {
            String name = options.getString(1).trim();
            String value = options.getString(2);
            if (value != null) {
                value = value.trim();
            }
            properties.setProperty(name, value);
        }
        return properties;
    }

    /**
     * Populates a result set with a collection of driver properties,
     * denormalizing if any property has more than one choice.
     *
     * @param resultInserter Result set
     * @param driverProperties Driver properties
     * @throws SQLException on error
     */
    protected static void populateResult(
        PreparedStatement resultInserter,
        DriverPropertyInfo[] driverProperties)
        throws SQLException
    {
        for (int iOption = 0; iOption < driverProperties.length; ++iOption) {
            DriverPropertyInfo info = driverProperties[iOption];
            populateChoice(iOption, info, -1, resultInserter);
            if (info.choices == null) {
                continue;
            }
            for (int iChoice = 0; iChoice < info.choices.length; ++iChoice) {
                populateChoice(
                    iOption,
                    info,
                    iChoice,
                    resultInserter);
            }
        }
    }

    private static void populateChoice(
        int optionOrdinal,
        DriverPropertyInfo info,
        int choiceOrdinal,
        PreparedStatement resultInserter)
        throws SQLException
    {
        resultInserter.setInt(1, optionOrdinal);
        resultInserter.setString(2, info.name);
        resultInserter.setString(3, info.description);
        resultInserter.setBoolean(4, info.required);
        resultInserter.setInt(5, choiceOrdinal);
        String value;
        if (choiceOrdinal == -1) {
            value = info.value;
        } else {
            value = info.choices[choiceOrdinal];
        }
        resultInserter.setString(6, value);
        resultInserter.executeUpdate();
    }

    private static Locale toLocale(String localeName)
    {
        return localeName == null
               ? Locale.getDefault()
               : Util.parseLocale(localeName);
    }

    /**
     * Returns the plugin property info for a given mofId and library.
     *
     * @param mofId The mofId to get property info for
     * @param libraryName The library to get property info for
     * @param wrapperOptions Wrapper options
     * @param localeArg What locale to get properties for
     * @param resultInserter Where the result rows are placed
     * @throws java.sql.SQLException on repo access failure
     */
    public static void getPluginPropertyInfo(
        String mofId,
        String libraryName,
        ResultSet wrapperOptions,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Locale locale = Util.parseLocale(localeArg);
        Properties wrapperProperties = toProperties(wrapperOptions);
        final State state = new State();
        try {
            FarragoMedDataWrapper dataWrapper =
                state.getWrapper(
                    mofId,
                    libraryName,
                    wrapperProperties);
            DriverPropertyInfo[] driverPropertyInfo =
                dataWrapper.getPluginPropertyInfo(
                    locale,
                    wrapperProperties);
            populateResult(resultInserter, driverPropertyInfo);
        } finally {
            state.close();
        }
    }

    /**
     * Returns the server property info for a given mofId and library.
     *
     * @param mofId The mofId to get property info for
     * @param libraryName The library to get property info for
     * @param wrapperOptions The wrapper props to request
     * @param serverOptions The server props to request
     * @param localeArg What locale to get properties for
     * @param resultInserter Where the result rows are placed
     * @throws java.sql.SQLException on repo access failure
     */
    public static void getServerPropertyInfo(
        String mofId,
        String libraryName,
        ResultSet wrapperOptions,
        ResultSet serverOptions,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Locale locale = Util.parseLocale(localeArg);
        Properties wrapperProperties = toProperties(wrapperOptions);
        Properties serverProperties = toProperties(serverOptions);
        final State state = new State();
        try {
            FarragoMedDataWrapperInfo dataWrapper =
                state.getWrapper(
                    mofId, libraryName, wrapperProperties);
            DriverPropertyInfo[] driverPropertyInfo =
                dataWrapper.getServerPropertyInfo(
                    locale,
                    wrapperProperties,
                    serverProperties);
            populateResult(resultInserter, driverPropertyInfo);
        } finally {
            state.close();
        }
    }

    /**
     * Returns whether a library is foreign.
     *
     * @param mofId The id to check
     * @param libraryName The library to check
     * @param options The options to pass to the data wrapper
     * @param localeString What locale to check for
     * @param resultInserter Where the result rows are placed
     * @throws java.sql.SQLException on repo access failure
     */
    public static void isForeign(
        String mofId,
        String libraryName,
        ResultSet options,
        String localeString, // TODO Should this be removed?
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties properties = toProperties(options);
        final State state = new State();
        try {
            FarragoMedDataWrapper dataWrapper =
                state.getWrapper(
                    mofId, libraryName, properties);
            boolean isForeign = dataWrapper.isForeign();
            resultInserter.setBoolean(1, isForeign);
            resultInserter.executeUpdate();
        } finally {
            state.close();
        }
    }

    public static void browseEmptyOptions(
        PreparedStatement resultInserter)
        throws SQLException
    {
        // insert no rows, return immediately
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class BrowseSchemaSink
        extends MedAbstractMetadataSink
    {
        private final String serverName;
        private final PreparedStatement resultInserter;

        BrowseSchemaSink(
            FarragoMedMetadataQuery query,
            String serverName,
            PreparedStatement resultInserter)
        {
            super(query, null);
            this.serverName = serverName;
            this.resultInserter = resultInserter;
        }

        // implement FarragoMedMetadataSink
        public boolean writeObjectDescriptor(
            String name,
            String typeName,
            String remarks,
            Properties properties)
        {
            if (!shouldInclude(name, typeName, false)) {
                return false;
            }

            try {
                resultInserter.setString(1, name);
                resultInserter.setString(2, remarks);
                resultInserter.executeUpdate();
            } catch (SQLException ex) {
                throw FarragoResource.instance().ValidatorImportFailed.ex(
                    name,
                    serverName,
                    ex);
            }

            return true;
        }

        // implement FarragoMedMetadataSink
        public boolean writeColumnDescriptor(
            String tableName,
            String columnName,
            int ordinal,
            RelDataType type,
            String remarks,
            String defaultValue,
            Properties properties)
        {
            return false;
        }
    }

    /**
     * Holds all state for a service call, and cleans it up with a
     * {@link #close()} method.
     */
    private static class State {
        private FarragoDataWrapperCache dataWrapperCache;

        /**
         * Returns a FarragoMedDataWrapper based on the data passed in.
         *
         * @param mofId Meta object ID of the wrapper
         * @param libraryName Library name of the wrapper
         * @param options Options to pass to the wrapper
         * @return FarragoMedDataWrapper that matches the args
         */
        FarragoMedDataWrapper getWrapper(
            String mofId,
            String libraryName,
            Properties options)
        {
            if (dataWrapperCache == null) {
                FarragoDbSession session =
                    (FarragoDbSession) FarragoUdrRuntime.getSession();
                FarragoDatabase db = session.getDatabase();
                FarragoObjectCache sharedCache = db.getDataWrapperCache();
                dataWrapperCache =
                    session.newFarragoDataWrapperCache(
                        session,
                        sharedCache,
                        session.getRepos(),
                        db.getFennelDbHandle(),
                        null);
            }
            return dataWrapperCache.loadWrapper(
                mofId,
                libraryName,
                options);
        }

        /**
         * Releases all resources.
         */
        void close()
        {
            if (dataWrapperCache != null) {
                dataWrapperCache.closeAllocation();
                dataWrapperCache = null;
            }
        }
    }
}

// End FarragoMedUDR.java
