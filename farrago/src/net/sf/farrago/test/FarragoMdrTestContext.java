/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.test;

import java.io.*;

import java.sql.*;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.mdr.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.netbeans.api.mdr.*;
import org.netbeans.mdr.handlers.*;


/**
 * FarragoMdrTestContext holds information needed by a test UDR which accesses
 * catalog metadata via an MDR foreign server.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoMdrTestContext
    extends FarragoCompoundAllocation
{
    //~ Instance fields --------------------------------------------------------

    private FarragoDbSession session;
    private FarragoDatabase db;
    private FarragoObjectCache objCache;
    private FarragoDataWrapperCache wrapperCache;
    private FemDataServer femServer;
    private MedMdrDataServer mdrServer;
    private RefPackage refPackage;
    private JmiModelGraph modelGraph;
    private JmiModelView modelView;
    private MDRepository mdrRepos;

    //~ Methods ----------------------------------------------------------------

    public void init(String foreignServerName)
        throws Exception
    {
        session = (FarragoDbSession) FarragoUdrRuntime.getSession();
        db = session.getDatabase();
        if (foreignServerName == null) {
            mdrRepos = db.getSystemRepos().getMdrRepos();
            modelGraph = db.getSystemRepos().getModelGraph();
            modelView = db.getSystemRepos().getModelView();
            return;
        }
        objCache = db.getDataWrapperCache();
        wrapperCache =
            session.newFarragoDataWrapperCache(
                this,
                objCache,
                session.getRepos(),
                db.getFennelDbHandle(),
                null);
        femServer =
            FarragoCatalogUtil.getModelElementByName(
                session.getRepos().allOfType(FemDataServer.class),
                foreignServerName);
        if (femServer == null) {
            throw new SQLException(
                "Unknown foreign server " + foreignServerName);
        }
        FarragoMedDataServer dataServer =
            wrapperCache.loadServerFromCatalog(femServer);
        assert (dataServer != null);
        if (!(dataServer instanceof MedMdrDataServer)) {
            throw new SQLException(
                "Foreign server " + foreignServerName
                + " is not an MDR server");
        }
        mdrServer = (MedMdrDataServer) dataServer;
        mdrRepos = mdrServer.getMdrRepository();
        refPackage = mdrServer.getRootPackage();

        // NOTE jvs 12-June-2006:  pass strict=false in
        // case extent references other packages we can't see
        modelGraph =
            new JmiModelGraph(
                refPackage,
                BaseObjectHandler.getDefaultClassLoader(),
                false);
        modelView = new JmiModelView(modelGraph);
    }

    public FarragoSession getSession()
    {
        return session;
    }

    public FarragoDatabase getDatabase()
    {
        return db;
    }

    public FemDataServer getFemDataServer()
    {
        return femServer;
    }

    public MedMdrDataServer getMedDataServer()
    {
        return mdrServer;
    }

    public RefPackage getRefPackage()
    {
        return refPackage;
    }

    public JmiModelGraph getModelGraph()
    {
        return modelGraph;
    }

    public JmiModelView getModelView()
    {
        return modelView;
    }

    public MDRepository getMdrRepos()
    {
        return mdrRepos;
    }
}

// End FarragoMdrTestContext.java
