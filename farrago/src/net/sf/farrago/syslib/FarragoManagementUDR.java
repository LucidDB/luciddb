/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import net.sf.farrago.catalog.FarragoModelLoader;
import net.sf.farrago.db.FarragoDatabase;
import net.sf.farrago.db.FarragoDbSession;
import net.sf.farrago.runtime.FarragoUdrRuntime;
import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.session.FarragoSessionExecutingStmtInfo;
import net.sf.farrago.session.FarragoSessionInfo;
import net.sf.farrago.session.FarragoSessionVariables;

import org.eigenbase.util.*;


/**
 * FarragoManagementUDR is a set of user-defined routines providing
 * access to information about the running state of Farrago,
 * intended for management purposes - such as a list of currently
 * executing statements.  The UDRs are used to create views in
 * initsql/createMgmtViews.sql.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public abstract class FarragoManagementUDR
{
    //~ Static fields/initializers --------------------------------------------

    static final String STORAGEFACTORY_PROP_NAME =
        "org.netbeans.mdr.storagemodel.StorageFactoryClassName";
    static final String [] STORAGE_PROP_NAMES =
        new String [] {
            "MDRStorageProperty.org.netbeans.mdr.persistence.jdbcimpl.driverClassName",
            "MDRStorageProperty.org.netbeans.mdr.persistence.jdbcimpl.url",
            "MDRStorageProperty.org.netbeans.mdr.persistence.jdbcimpl.userName",
            "MDRStorageProperty.org.netbeans.mdr.persistence.jdbcimpl.password",
            "MDRStorageProperty.org.netbeans.mdr.persistence.jdbcimpl.schemaName"
        };

    //~ Methods ---------------------------------------------------------------

    /**
     * Populates a table of information on currently executing statements.
     */
    public static void statements(PreparedStatement resultInserter)
        throws SQLException
    {
        FarragoSession callerSession = FarragoUdrRuntime.getSession();
        FarragoDatabase db = ((FarragoDbSession) callerSession).getDatabase();
        List<FarragoSession> sessions = db.getSessions();
        for (FarragoSession s : sessions) {
            FarragoSessionInfo info = s.getSessionInfo();
            List<Long> ids = info.getExecutingStmtIds();
            for (long id : ids) {
                FarragoSessionExecutingStmtInfo stmtInfo =
                    info.getExecutingStmtInfo(id);
                if (stmtInfo != null) {
                    int i = 0;
                    resultInserter.setLong(++i, id);
                    resultInserter.setLong(
                        ++i,
                        info.getId());
                    resultInserter.setString(
                        ++i,
                        stmtInfo.getSql());
                    resultInserter.setTimestamp(
                        ++i,
                        new Timestamp(stmtInfo.getStartTime()));
                    resultInserter.setString(
                        ++i,
                        Arrays.asList(stmtInfo.getParameters()).toString());
                    resultInserter.executeUpdate();
                }
            }
        }
    }

    /**
     * Populates a table of catalog objects in use by active statements.
     *
     * @param resultInserter
     * @throws SQLException
     */
    public static void objectsInUse(PreparedStatement resultInserter)
        throws SQLException
    {
        FarragoSession callerSession = FarragoUdrRuntime.getSession();
        FarragoDatabase db = ((FarragoDbSession) callerSession).getDatabase();
        List<FarragoSession> sessions = db.getSessions();
        for (FarragoSession s : sessions) {
            FarragoSessionInfo info = s.getSessionInfo();
            List<Long> ids = info.getExecutingStmtIds();
            for (long id : ids) {
                FarragoSessionExecutingStmtInfo stmtInfo =
                    info.getExecutingStmtInfo(id);
                if (stmtInfo != null) {
                    List<String> mofIds = stmtInfo.getObjectsInUse();
                    for (String mofId : mofIds) {
                        int i = 0;
                        resultInserter.setLong(
                            ++i,
                            info.getId());
                        resultInserter.setLong(++i, id);
                        resultInserter.setString(++i, mofId);
                        resultInserter.executeUpdate();
                    }
                }
            }
        }
    }

    /**
     * Populates a table of currently active sessions.
     *
     * @param resultInserter
     * @throws SQLException
     */
    public static void sessions(PreparedStatement resultInserter)
        throws SQLException
    {
        FarragoSession callerSession = FarragoUdrRuntime.getSession();
        FarragoDatabase db = ((FarragoDbSession) callerSession).getDatabase();
        List<FarragoSession> sessions = db.getSessions();
        for (FarragoSession s : sessions) {
            int i = 0;
            FarragoSessionVariables v = s.getSessionVariables();
            FarragoSessionInfo info = s.getSessionInfo();
            resultInserter.setLong(
                ++i,
                info.getId());
            resultInserter.setString(
                ++i,
                s.getUrl());
            resultInserter.setString(++i, v.currentUserName);
            resultInserter.setString(++i, v.currentRoleName);
            resultInserter.setString(++i, v.sessionUserName);
            resultInserter.setString(++i, v.systemUserName);
            resultInserter.setString(++i, v.systemUserFullName);
            resultInserter.setString(++i, v.sessionName);
            resultInserter.setString(++i, v.programName);
            resultInserter.setLong(++i, v.processId);
            resultInserter.setString(++i, v.catalogName);
            resultInserter.setString(++i, v.schemaName);
            resultInserter.setBoolean(
                ++i,
                s.isClosed());
            resultInserter.setBoolean(
                ++i,
                s.isAutoCommit());
            resultInserter.setBoolean(
                ++i,
                s.isTxnInProgress());
            resultInserter.executeUpdate();
        }
    }

    /**
     * Sleeps for a given number of milliseconds (checking
     * for query cancellation every second).
     *
     * @param millis number of milliseconds to sleep
     *
     * @return 0 (instead of void, so that this can be used as a UDF)
     */
    public static int sleep(long millis)
    {
        try {
            while (millis != 0) {
                long delta = Math.min(1000, millis);
                Thread.sleep(delta);
                millis -= delta;
                FarragoUdrRuntime.checkCancel();
            }
        } catch (InterruptedException ex) {
            // should not happen
            throw Util.newInternal(ex);
        }
        return 0;
    }

    /**
     * Populates a table of properties of the current repository connection.
     *
     * @param resultInserter
     * @throws SQLException
     */
    public static void repositoryProperties(PreparedStatement resultInserter)
        throws SQLException
    {
        FarragoSession callerSession = FarragoUdrRuntime.getSession();
        FarragoDatabase db = ((FarragoDbSession) callerSession).getDatabase();
        FarragoModelLoader loader = db.getSystemRepos().getModelLoader();
        if (loader != null) {
            Properties props = loader.getStorageProperties();
            int i = 0;
            resultInserter.setString(++i, STORAGEFACTORY_PROP_NAME);
            resultInserter.setString(
                ++i,
                loader.getStorageFactoryClassName());
            resultInserter.executeUpdate();

            for (String propName : STORAGE_PROP_NAMES) {
                i = 0;
                resultInserter.setString(++i, propName);
                resultInserter.setString(
                    ++i,
                    props.getProperty(propName));
                resultInserter.executeUpdate();
            }
        }
    }
}
