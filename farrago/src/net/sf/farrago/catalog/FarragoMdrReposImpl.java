/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
package net.sf.farrago.catalog;

import java.io.*;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;

import javax.jmi.reflect.RefPackage;

import net.sf.farrago.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.JmiModelGraph;
import org.eigenbase.jmi.mem.JmiModeledMemFactory;
import org.netbeans.api.mdr.*;
import org.netbeans.mdr.NBMDRepositoryImpl;


/**
 * Implementation of {@link FarragoRepos} using a MDR repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoMdrReposImpl
    extends FarragoReposImpl
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getReposTracer();

    //~ Instance fields --------------------------------------------------------

    /**
     * Root package in transient repository.
     */
    private final FarragoPackage transientFarragoPackage;

    /**
     * Fennel package in repository.
     */
    private final FennelPackage fennelPackage;

    /**
     * The loader for the underlying MDR repository.
     */
    private FarragoModelLoader modelLoader;

    /**
     * The underlying MDR repository.
     */
    private final MDRepository mdrRepository;

    /**
     * MofId for current instance of FemFarragoConfig.
     */
    private final String currentConfigMofId;

    protected FarragoMemFactory memFactory;

    /* OLD MEMFACTORY: */
    private String memStorageId;
    private static final boolean USE_OLD_MEMFACTORY = false;
    /* end OLD MEMFACTORY */

    
    //~ Constructors -----------------------------------------------------------

    /**
     * Opens a Farrago repository.
     */
    public FarragoMdrReposImpl(
        FarragoAllocationOwner owner,
        FarragoModelLoader modelLoader,
        boolean userRepos)
    {
        super(owner);
        this.modelLoader = modelLoader;

        tracer.fine("Loading catalog");
        if (FarragoProperties.instance().homeDir.get() == null) {
            throw FarragoResource.instance().MissingHomeProperty.ex(
                FarragoProperties.instance().homeDir.getPath());
        }

        MdrUtil.integrateTracing(FarragoTrace.getMdrTracer());

        if (!userRepos) {
            File lockFile =
                new File(
                    modelLoader.getSystemReposFile().toString() + ".lck");
            try {
                new FarragoFileLockAllocation(allocations, lockFile, true);
            } catch (IOException ex) {
                throw FarragoResource.instance().CatalogFileLockFailed.ex(
                    lockFile.toString());
            }
        }

        if (FarragoReposUtil.isReloadNeeded(modelLoader)) {
            try {
                FarragoReposUtil.reloadRepository(modelLoader);
            } catch (Exception ex) {
                throw FarragoResource.instance().CatalogReloadFailed.ex(ex);
            }
        }

        FarragoPackage farragoPackage =
            modelLoader.loadModel("FarragoCatalog", userRepos);
        if (farragoPackage == null) {
            throw FarragoResource.instance().CatalogUninitialized.ex();
        }

        super.setRootPackage(farragoPackage);

        mdrRepository = modelLoader.getMdrRepos();

        if (USE_OLD_MEMFACTORY) {
            // Create special in-memory storage for transient objects
            try {
                NBMDRepositoryImpl nbRepos = (NBMDRepositoryImpl) mdrRepository;
                Map props = new HashMap();
                memStorageId =
                    nbRepos.mountStorage(
                        FarragoTransientStorageFactory.class.getName(),
                        props);
                beginReposTxn(true);
                boolean rollback = true;
                try {
                    RefPackage memExtent =
                        nbRepos.createExtent(
                            "TransientCatalog",
                            getFarragoPackage().refMetaObject(),
                            null,
                            memStorageId);
                    transientFarragoPackage = (FarragoPackage) memExtent;
                    rollback = false;
                } finally {
                    endReposTxn(rollback);
                }
                FarragoTransientStorage.ignoreCommit = true;
                fennelPackage = transientFarragoPackage.getFem().getFennel();
            } catch (Throwable ex) {
                throw FarragoResource.instance().CatalogInitTransientFailed.ex(ex);
            }
    
            // Load configuration
            currentConfigMofId = getDefaultConfig().refMofId();
            initGraph();
        } else {
            // Load configuration
            currentConfigMofId = getDefaultConfig().refMofId();
            initGraph();
    
            // Create special in-memory storage for transient objects
            try {
                memFactory = new FarragoMemFactory(getModelGraph());
    
                transientFarragoPackage = memFactory.getFarragoPackage();
                fennelPackage = transientFarragoPackage.getFem().getFennel();
            } catch (Throwable ex) {
                throw FarragoResource.instance().CatalogInitTransientFailed.ex(ex);
            }
        }
        tracer.info("Catalog successfully loaded");
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoRepos
    public MDRepository getMdrRepos()
    {
        return mdrRepository;
    }

    // implement FarragoRepos
    public FarragoPackage getTransientFarragoPackage()
    {
        return transientFarragoPackage;
    }

    // override FarragoMetadataFactory
    public FennelPackage getFennelPackage()
    {
        // NOTE jvs 5-May-2004:  return the package corresponding to
        // in-memory storage
        return fennelPackage;
    }

    // implement FarragoRepos
    public FemFarragoConfig getCurrentConfig()
    {
        // TODO:  prevent updates
        return (FemFarragoConfig) mdrRepository.getByMofId(currentConfigMofId);
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        allocations.closeAllocation();
        if (modelLoader == null) {
            return;
        }
        tracer.fine("Closing catalog");
        if (USE_OLD_MEMFACTORY) {
            if (memStorageId != null) {
                mdrRepository.beginTrans(true);
                FarragoTransientStorage.ignoreCommit = false;
                if (transientFarragoPackage != null) {
                    transientFarragoPackage.refDelete();
                }
                mdrRepository.endTrans();
                memStorageId = null;
            }
        } else {
            if (transientFarragoPackage != null) {
                transientFarragoPackage.refDelete();
            }
            memFactory = null;
        }
        modelLoader.close();
        modelLoader = null;
        tracer.info("Catalog successfully closed");
    }

    // implement FarragoTransientTxnContext
    public void beginTransientTxn()
    {
        tracer.fine("Begin transient repository transaction");
        if (USE_OLD_MEMFACTORY) {
            mdrRepository.beginTrans(true);
        }
    }

    // implement FarragoTransientTxnContext
    public void endTransientTxn()
    {
        tracer.fine("End transient repository transaction");
        if (USE_OLD_MEMFACTORY) {
            mdrRepository.endTrans(false);
        }
    }

    // implement FarragoRepos
    public void beginReposTxn(boolean writable)
    {
        if (writable) {
            tracer.fine("Begin read/write repository transaction");
        } else {
            tracer.fine("Begin read-only repository transaction");
        }
        mdrRepository.beginTrans(writable);
    }

    // implement FarragoRepos
    public void endReposTxn(boolean rollback)
    {
        if (rollback) {
            tracer.fine("Rollback repository transaction");
        } else {
            tracer.fine("Commit repository transaction");
        }
        mdrRepository.endTrans(rollback);
    }

    // implement FarragoRepos
    public FarragoModelLoader getModelLoader()
    {
        return modelLoader;
    }
    
    //~ Inner classes ---------------------------------------------------------
    
    protected class FarragoMemFactory extends FarragoMetadataFactoryImpl
    {
        private final FactoryImpl factoryImpl;
    
        public FarragoMemFactory(JmiModelGraph modelGraph)
        {
            factoryImpl = new FactoryImpl(modelGraph);
            this.setRootPackage((FarragoPackage) factoryImpl.getRootPackage());
        }
    
        public FactoryImpl getImpl()
        {
            return factoryImpl;
        }
        
        public RefPackage newRefPackage(Class ifacePackage)
        {
            return factoryImpl.newRefPackage(ifacePackage);
        }
    }

    private class FactoryImpl extends JmiModeledMemFactory
    {
        FactoryImpl(JmiModelGraph modelGraph)
        {
            super(modelGraph);
        }
    
        protected RefPackageImpl newRootPackage()
        {
            return new RefPackageImpl(FarragoPackage.class);
        }
    }
    
}

// End FarragoReposImpl.java
