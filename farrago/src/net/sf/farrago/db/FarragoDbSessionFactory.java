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
package net.sf.farrago.db;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;


/**
 * FarragoDbSessionFactory is a basic implementation for the {@link
 * net.sf.farrago.session.FarragoSessionFactory} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbSessionFactory
    implements FarragoSessionFactory
{
    private FarragoPluginClassLoader pluginClassLoader;

    //~ Methods ----------------------------------------------------------------

    public FarragoDbSessionFactory()
    {
        this.pluginClassLoader = new FarragoPluginClassLoader();
    }

    // implement FarragoSessionFactory
    public FarragoSession newSession(
        String url,
        Properties info)
    {
        return new FarragoDbSession(url, info, this);
    }

    // implement FarragoSessionFactory
    public FarragoSession newReentrantSession(FarragoSession session)
    {
        // REVIEW jvs 9-Jan-2007:  Seems like this is redundant,
        // since cloneSession is going to clone the variables again.
        FarragoSessionVariables sessionVars =
            session.getSessionVariables().cloneVariables();

        FarragoSession reentrantSession = session.cloneSession(sessionVars);

        return reentrantSession;
    }

    // implement FarragoSessionFactory
    public void releaseReentrantSession(FarragoSession session)
    {
        session.closeAllocation();
    }

    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        if (defaultPersonality == null) {
            throw new UnsupportedOperationException(
                "no default session personality defined");
        } else {
            return defaultPersonality;
        }
    }

    // implement FarragoSessionFactory
    public void setPluginClassLoader(
        FarragoPluginClassLoader pluginClassLoader)
    {
        this.pluginClassLoader = pluginClassLoader;
    }

    // implement FarragoSessionFactory
    public FarragoPluginClassLoader getPluginClassLoader()
    {
        return pluginClassLoader;
    }

    // implement FarragoSessionFactory
    public FarragoSessionModelExtension newModelExtension(
        FarragoPluginClassLoader pcl,
        FemJar femJar)
    {
        String url = FarragoCatalogUtil.getJarUrl(femJar);
        try {
            String attr =
                FarragoPluginClassLoader.PLUGIN_FACTORY_CLASS_ATTRIBUTE;
            Class factoryClass =
                pcl.loadClassFromJarUrlManifest(url, attr);
            FarragoSessionModelExtensionFactory factory =
                (FarragoSessionModelExtensionFactory) pcl
                .newPluginInstance(factoryClass);

            // TODO:  trace info about extension
            FarragoSessionModelExtension modelExtension =
                factory.newModelExtension();
            return modelExtension;
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginInitFailed.ex(url, ex);
        }
    }

    // implement FarragoSessionFactory
    public FennelCmdExecutor newFennelCmdExecutor()
    {
        return new FennelCmdExecutorImpl();
    }

    // implement FarragoSessionFactory
    public FarragoRepos newRepos(
        FarragoAllocationOwner owner,
        boolean userRepos)
    {
        return new FarragoMdrReposImpl(
            owner,
            new FarragoModelLoader(),
            userRepos);
    }

    // implement FarragoSessionFactory
    public FennelTxnContext newFennelTxnContext(
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle)
    {
        return new FennelTxnContext(repos, fennelDbHandle);
    }

    // implement FarragoSessionFactory
    public FarragoSessionTxnMgr newTxnMgr()
    {
        return new FarragoDbNullTxnMgr();
    }

    // implement FarragoSessionFactory
    public void applyFennelExtensionParameters(Properties map)
    {
    }

    // implement FarragoSessionFactory
    public void specializedInitialization(FarragoAllocationOwner owner)
    {
    }

    // implement FarragoSessionFactory
    public void specializedShutdown()
    {
    }

    // implement FarragoSessionFactory
    public void cleanupSessions()
    {
        FarragoDbSingleton.shutdownConditional(0);
    }

    // implement FarragoSessionFactory
    public void defineResourceBundles(List<ResourceBundle> bundleList)
    {
        bundleList.add(FarragoResource.instance());
    }
}

// End FarragoDbSessionFactory.java
