/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
import java.util.List;

import net.sf.farrago.db.FarragoDbSingleton;
import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.session.FarragoSessionVariables;
import net.sf.farrago.util.FarragoSessionExecutingStmtInfo;
import net.sf.farrago.util.FarragoSessionInfo;


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
    //~ Methods ---------------------------------------------------------------

    /**
     * Populates a table of information on currently executing statements.
     *
     * returns table(id int, sqlStmt varchar(1024), createTime timestamp, parameters varchar(1024)
     */
    public static void statements(PreparedStatement resultInserter)
        throws SQLException
    {
        List<FarragoSession> sessions = FarragoDbSingleton
                .getSessions();
        for (FarragoSession s : sessions) {
            FarragoSessionInfo info = s.getSessionInfo();
            Integer [] ids = info.getExecutingStmtIds();
            for (int x = 0; x < ids.length; x++) {
                FarragoSessionExecutingStmtInfo stmtInfo =
                    info.getExecutingStmtInfo(ids[x]);

                if (stmtInfo != null) {
                    int i = 0;
                    resultInserter.setInt(++i, ids[x]);
                    resultInserter.setString(
                        ++i,
                        stmtInfo.getSql());
                    resultInserter.setTimestamp(
                        ++i,
                        new Timestamp(stmtInfo.getStartTime()));
                    resultInserter.setString(
                        ++i,
                        arrayToString(stmtInfo.getParameters()));
                    resultInserter.executeUpdate();
                }
            }
        }
    }

    /**
     * Populates a table of catalog objects in use by active statements.
     * returns table(stmtId int, mofId varchar(32))
     *
     * @param resultInserter
     * @throws SQLException
     */
    public static void objectsInUse(PreparedStatement resultInserter)
        throws SQLException
    {
        List<FarragoSession> sessions = FarragoDbSingleton.getSessions();
        for (FarragoSession s : sessions) {
            FarragoSessionInfo info = s.getSessionInfo();
            Integer [] ids = info.getExecutingStmtIds();
            for (int x = 0; x < ids.length; x++) {
                FarragoSessionExecutingStmtInfo stmtInfo =
                    info.getExecutingStmtInfo(ids[x]);
                if (stmtInfo != null) {
                    String [] mofIds = stmtInfo.getObjectsInUse();
                    for (int y = 0; y < mofIds.length; y++) {
                        int i = 0;
                        resultInserter.setInt(++i, ids[x]);
                        resultInserter.setString(++i, mofIds[y]);
                        resultInserter.executeUpdate();
                    }
                }
            }
        }
    }

    /**
     * Populates a table of currently active sessions.
     * returns table(id int, url varchar(256), currentUserName varchar(256), currentRoleName varchar(256), sessionUserName varchar(256), systemUserName varchar(256), catalogName varchar(256), schemaName varchar(256), isClosed boolean, isAutoCommit boolean, isTxnInProgress boolean)
     *
     * @param resultInserter
     * @throws SQLException
     */
    public static void sessions(PreparedStatement resultInserter)
        throws SQLException
    {
        List<FarragoSession> sessions = FarragoDbSingleton.getSessions();
        for (FarragoSession s : sessions) {
            int i = 0;
            FarragoSessionVariables v = s.getSessionVariables();
            resultInserter.setInt(
                ++i,
                s.hashCode());
            resultInserter.setString(
                ++i,
                s.getUrl());
            resultInserter.setString(++i, v.currentUserName);
            resultInserter.setString(++i, v.currentRoleName);
            resultInserter.setString(++i, v.sessionUserName);
            resultInserter.setString(++i, v.systemUserName);
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

    private static String arrayToString(Object [] array)
    {
        StringBuilder sb = new StringBuilder();
        if (array != null) {
            for (int i = 0; i < array.length; i++) {
                if (array[i] != null) {
                    sb.append(array[i].toString());
                } else {
                    sb.append("[NULL]");
                }
                if (i < (array.length - 1)) {
                    sb.append(", ");
                }
            }
        }
        return sb.toString();
    }
}
