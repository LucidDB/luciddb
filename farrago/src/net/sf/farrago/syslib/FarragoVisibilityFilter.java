/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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

import javax.jmi.reflect.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.*;

/**
 * FarragoVisibilityFilter implements a visibility-filtering UDX for creating
 * metadata views which return only those objects to which the current user has
 * access.  It optimizes the processing by (a) batching up visibility checks
 * and (b) skipping redundant visibility checks (e.g. all columns for
 * the same table).
 *
 * @author John Sichi
 * @version $Id$
 */
class FarragoVisibilityFilter
{
    private final ResultSet inputSet;
    private final PreparedStatement resultInserter;
    private final Set<String> pendingMofIds;
    private final List<List<Object>> buffer;
    private final int nColumns;
    private final int iMofIdCol;
    private final int iClassNameCol;
    private final FarragoSessionPrivilegeChecker privilegeChecker;
    private final RefClass refClass;
    private final FarragoRepos repos;

    private static final int BATCH_SIZE = 500;

    FarragoVisibilityFilter(
        ResultSet inputSet,
        String className,
        List<String> idColumnNames,
        PreparedStatement resultInserter)
        throws SQLException
    {
        this.inputSet = inputSet;
        this.resultInserter = resultInserter;
        pendingMofIds = new HashSet<String>();
        buffer = new ArrayList<List<Object>>();
        repos = FarragoUdrRuntime.getRepos();

        nColumns = inputSet.getMetaData().getColumnCount();
        int nOutput = resultInserter.getParameterMetaData().getParameterCount();
        assert (nOutput == nColumns);
        if (className == null) {
            assert (idColumnNames.size() == 2);
            refClass = null;
            String classNameCol = idColumnNames.get(1);
            iClassNameCol = inputSet.findColumn(classNameCol) - 1;
        } else {
            assert (idColumnNames.size() == 1);
            JmiClassVertex classVertex =
                repos.getModelGraph().getVertexForClassName(className);
            assert (classVertex != null);
            refClass = classVertex.getRefClass();
            iClassNameCol = -1;
        }
        String idCol = idColumnNames.get(0);
        iMofIdCol = inputSet.findColumn(idCol) - 1;
        privilegeChecker =
            FarragoUdrRuntime.getSession().newPrivilegeChecker();
    }

    public void execute() throws SQLException
    {
        EnkiMDRepository enkiRepos = repos.getEnkiMdrRepos();
        String currentUserName =
            FarragoUdrRuntime.getSession().getSessionVariables()
            .currentUserName;
        FemUser user = (FemUser)
            FarragoCatalogUtil.getAuthIdByName(
                repos,
                currentUserName);
        assert (user != null);

        while (inputSet.next()) {
            List<Object> tuple = new ArrayList<Object>();
            for (int i = 0; i < nColumns; i++) {
                tuple.add(inputSet.getObject(i + 1));
            }
            String mofId = tuple.get(iMofIdCol).toString();
            if (!pendingMofIds.contains(mofId)) {
                pendingMofIds.add(mofId);
                RefBaseObject refObj;
                if (refClass == null) {
                    String className = tuple.get(iClassNameCol).toString();
                    JmiClassVertex classVertex =
                        repos.getModelGraph().getVertexForClassName(
                            className);
                    assert (classVertex != null);
                    RefClass refClassCurrent = classVertex.getRefClass();
                    refObj = enkiRepos.getByMofId(mofId, refClassCurrent);
                } else {
                    refObj = enkiRepos.getByMofId(mofId, refClass);
                }
                assert (refObj != null);
                assert (refObj instanceof CwmModelElement)
                    : refObj.getClass().getName();
                CwmModelElement modelElement = (CwmModelElement) refObj;
                privilegeChecker.requestAccess(
                    modelElement,
                    user,
                    null,
                    null,
                    false);
            }
            buffer.add(tuple);
            if (buffer.size() > BATCH_SIZE) {
                flushBuffer();
            }
        }
        flushBuffer();
    }

    private void flushBuffer()
        throws SQLException
    {
        Set<String> visible = privilegeChecker.checkVisibility();

        for (List<Object> tuple : buffer) {
            if (!visible.contains(tuple.get(iMofIdCol))) {
                continue;
            }
            for (int i = 0; i < nColumns; i++) {
                Object value = tuple.get(i);
                resultInserter.setObject(i + 1, value);
            }
            resultInserter.executeUpdate();
        }
        buffer.clear();
        pendingMofIds.clear();
    }
}

// End FarragoVisibilityFilter.java
