/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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

    private FarragoSession session;
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
        session = FarragoUdrRuntime.getSession();
        db = ((FarragoDbSession) session).getDatabase();
        if (foreignServerName == null) {
            mdrRepos = db.getSystemRepos().getMdrRepos();
            modelGraph = db.getSystemRepos().getModelGraph();
            modelView = db.getSystemRepos().getModelView();
            return;
        }
        objCache = db.getDataWrapperCache();
        wrapperCache =
            new FarragoDataWrapperCache(
                this,
                objCache,
                db.getPluginClassLoader(),
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
