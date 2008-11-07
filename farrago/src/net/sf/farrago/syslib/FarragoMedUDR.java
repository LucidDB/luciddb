/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


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
     * Tests that a connection can be established to a particular
     * SQL/MED local or foreign data server.  If no exception
     * is thrown, the test was successful.
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
     * instantiated from a particular data wrapper.  If no exception is thrown,
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
                    new SqlIdentifier(wrapperName, SqlParserPos.ZERO),
                    true);
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
     * @param resultInserter writes result table
     */
    public static void browseConnectServer(
        String wrapperName,
        ResultSet serverOptions,
        PreparedStatement resultInserter)
        throws SQLException
    {
        // Convert serverOptions into a Properties object
        Properties serverProps = new Properties();
        while (serverOptions.next()) {
            String name = serverOptions.getString(1).trim();
            String value = serverOptions.getString(2);
            if (value != null) {
                value = value.trim();
            }
            serverProps.setProperty(name, value);
        }

        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoReposTxnContext txn = 
            new FarragoReposTxnContext(session.getRepos(), true);
        txn.beginReadTxn();
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        try {
            browseConnectServerImpl(
                stmtValidator,
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
                Locale.getDefault(),
                wrapperProps,
                serverProps);
        for (int iOption = 0; iOption < infoArray.length; ++iOption) {
            DriverPropertyInfo info = infoArray[iOption];
            browseConnectServerChoice(iOption, info, -1, resultInserter);
            if (info.choices == null) {
                continue;
            }
            for (int iChoice = 0; iChoice < info.choices.length; ++iChoice) {
                browseConnectServerChoice(
                    iOption,
                    info,
                    iChoice,
                    resultInserter);
            }
        }
    }

    private static void browseConnectServerChoice(
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
}

// End FarragoMedUDR.java
