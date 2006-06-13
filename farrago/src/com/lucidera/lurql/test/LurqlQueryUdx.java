/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.lurql.test;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.mdr.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.session.*;
import net.sf.farrago.db.*;
import net.sf.farrago.util.*;

import net.sf.farrago.fem.med.*;

import com.lucidera.lurql.*;

import org.eigenbase.jmi.*;

import javax.jmi.reflect.*;

import java.sql.*;
import java.io.*;
import java.util.*;

/**
 * LurqlQueryUdx executes a LURQL query and returns the resulting collection as
 * a relational result set.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LurqlQueryUdx
{
    /**
     * Executes a LURQL query, producing result columns as follows:
     *
     *<ol>
     *
     *<li>CLASS_NAME:  class name of matching object
     *
     *<li>OBJ_NAME:  name of matching object, or null if it lacks
     * a "name" attribute
     *
     *<li>MOF_ID:  MOFID of matching object
     *
     *<li>OBJ_ATTRS:  concatenation of attribute/value pairs defining
     * matching object
     *
     *</ol>
     *
     * @param foreignServerName name of predefined foreign server to use as
     * source; must be defined using the MDR foreign data wrapper
     *
     * @param lurql LURQL query to execute (may not contain parameters)
     *
     * @param resultInserter used to write results
     */
    public static void queryMedMdr(
        String foreignServerName,
        String lurql,
        PreparedStatement resultInserter)
        throws Exception
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoDatabase db = ((FarragoDbSession) session).getDatabase();
        FarragoObjectCache objCache = db.getDataWrapperCache();
        FarragoAllocationOwner owner = new FarragoCompoundAllocation();
        try {
            FarragoDataWrapperCache wrapperCache =
                new FarragoDataWrapperCache(
                    owner,
                    objCache,
                    db.getPluginClassLoader(),
                    session.getRepos(),
                    db.getFennelDbHandle(),
                    null);
            FemDataServer femServer = (FemDataServer)
                FarragoCatalogUtil.getModelElementByName(
                    session.getRepos().getMedPackage().getFemDataServer().
                    refAllOfType(),
                    foreignServerName);
            if (femServer == null) {
                throw new SQLException(
                    "Unknown foreign server " + foreignServerName);
            }
            FarragoMedDataServer dataServer =
                wrapperCache.loadServerFromCatalog(femServer);
            assert(dataServer != null);
            if (!(dataServer instanceof MedMdrDataServer)) {
                throw new SQLException(
                    "Foreign server " + foreignServerName
                    + " is not an MDR server");
            }
            MedMdrDataServer mdrServer = (MedMdrDataServer) dataServer;
            RefPackage refPackage = mdrServer.getRootPackage();
            // NOTE jvs 12-June-2006:  pass strict=false in
            // case extent references other packages we can't see
            JmiModelGraph modelGraph = new JmiModelGraph(refPackage, false);
            JmiModelView modelView = new JmiModelView(modelGraph);
            JmiQueryProcessor queryProcessor =
                new LurqlQueryProcessor(
                    mdrServer.getMdrRepository());
            JmiPreparedQuery query = queryProcessor.prepare(
                modelView, lurql);
            // TODO jvs 11-June-2006:  Configure loopback connection
            Collection results = query.execute(null, null);
            for (Object obj : results) {
                RefObject refObj = (RefObject) obj;
                SortedMap map = JmiObjUtil.getAttributeValues(refObj);
                resultInserter.setString(
                    1,
                    JmiObjUtil.getMetaObjectName(refObj));
                resultInserter.setString(
                    2,
                    (String) map.get("name"));
                resultInserter.setString(
                    3,
                    refObj.refMofId());
                resultInserter.setString(
                    4,
                    map.toString());
                resultInserter.executeUpdate();
            }
        } finally {
            owner.closeAllocation();
        }
    }
}

// End LurqlQueryUdx.java
