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
package net.sf.farrago.catalog;

import java.io.*;

import java.util.*;
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.enki.trans.*;
import org.eigenbase.jmi.*;
import org.eigenbase.jmi.mem.*;

import org.netbeans.api.mdr.*;


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
    private FarragoPackage transientFarragoPackage;

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
    private final EnkiMDRepository mdrRepository;

    /**
     * MofId for current instance of FemFarragoConfig.
     */
    private final String currentConfigMofId;

    protected FarragoMemFactory memFactory;

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

        mdrRepository = (EnkiMDRepository) modelLoader.getMdrRepos();

        checkModelTimestamp("FarragoCatalog");

        // Load configuration
        currentConfigMofId = getDefaultConfig().refMofId();
        initGraph();

        // Create special in-memory storage for transient objects
        try {
            Properties storeProps = modelLoader.getStorageProperties();
            String storeType =
                storeProps.getProperty(MDRepositoryFactory.ENKI_IMPL_TYPE);
            if (MdrProvider.NETBEANS_MDR.toString().equals(storeType)) {
                // Netbeans branch
                memFactory = new FarragoMemFactory(getModelGraph());
                transientFarragoPackage = memFactory.getFarragoPackage();
            } else {
                // Enki/Hibernate branch
                transientFarragoPackage = createTransientPackage();
            }
            fennelPackage = transientFarragoPackage.getFem().getFennel();
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw FarragoResource.instance().CatalogInitTransientFailed.ex(ex);
        } finally {
            // End session started in modelLoader.loadModel
            modelLoader.closeMdrSession();
        }

        tracer.info("Catalog successfully loaded");
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoPackage createTransientPackage() throws Exception
    {
        // NOTE: this uses the JMI Memory only implementation
        // which is slow, but has caused issues with performance
        // Pending resolution of FRG-417 this should revert to implementation
        // in Eigenchange 14049 
        return new FarragoMemFactory(getModelGraph()).getFarragoPackage();
    }

    private void checkModelTimestamp(String extentName)
    {
        String prefix = "TIMESTAMP = ";

        mdrRepository.beginTrans(true);
        boolean rollback = true;
        try {
            String storedTimestamp = mdrRepository.getAnnotation(extentName);
            String compiledTimestamp = prefix + getCompiledModelTimestamp();
            if ((storedTimestamp == null)
                || !storedTimestamp.startsWith(prefix))
            {
                // first time:  add timestamp
                mdrRepository.setAnnotation(extentName, compiledTimestamp);
                rollback = false;
            } else {
                // on reload:  verify timestamps
                if (!storedTimestamp.equals(compiledTimestamp)) {
                    throw FarragoResource.instance()
                    .CatalogModelTimestampCheckFailed.ex(
                        storedTimestamp,
                        compiledTimestamp);
                }
            }
        } finally {
            mdrRepository.endTrans(rollback);
        }
    }

    // implement FarragoRepos
    public MDRepository getMdrRepos()
    {
        return mdrRepository;
    }

    // implement FarragoRepos
    public EnkiMDRepository getEnkiMdrRepos()
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
        return (FemFarragoConfig) getEnkiMdrRepos().getByMofId(
            currentConfigMofId,
            getConfigPackage().getFemFarragoConfig());
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        allocations.closeAllocation();
        if (modelLoader == null) {
            return;
        }
        tracer.fine("Closing catalog");
        if (transientFarragoPackage != null) {
            transientFarragoPackage.refDelete();
            transientFarragoPackage = null;
        }
        memFactory = null;

        modelLoader.close();
        modelLoader = null;
        tracer.info("Catalog successfully closed");
    }

    // implement FarragoRepos
    public void beginReposSession()
    {
        tracer.fine("Begin repository session");
        super.beginReposSession();
        mdrRepository.beginSession();
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
    public void endReposSession()
    {
        tracer.fine("End repository session");
        super.endReposSession();
        mdrRepository.endSession();
    }

    // implement FarragoRepos
    public FarragoModelLoader getModelLoader()
    {
        return modelLoader;
    }

    //~ Inner Classes ----------------------------------------------------------

    protected class FarragoMemFactory
        extends FarragoMetadataFactoryImpl
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

        public <T extends RefPackage> T newRefPackage(Class<T> ifacePackage)
        {
            return factoryImpl.newRefPackage(ifacePackage);
        }
    }

    private class FactoryImpl
        extends JmiModeledMemFactory
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

// End FarragoMdrReposImpl.java
