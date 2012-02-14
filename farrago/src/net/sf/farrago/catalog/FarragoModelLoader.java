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

import net.sf.farrago.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import org.eigenbase.enki.mdr.*;

import org.netbeans.api.mdr.*;
import org.netbeans.mdr.persistence.jdbcimpl.*;


// NOTE:  This class gets compiled independently of everything else since
// it is used by build-time utilities such as ProxyGen.  That means it must
// have minimal dependencies on other non-model-generated Farrago code.
// It probably needs to be moved somewhere else.

/**
 * FarragoModelLoader is a utility class for loading the catalog model.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoModelLoader
{
    //~ Static fields/initializers ---------------------------------------------

    // NOTE jvs 15-Dec-2005:  Do it this way to avoid dependency on
    // FarragoTrace.
    private static final Logger tracer =
        Logger.getLogger("net.sf.farrago.catalog.FarragoRepos");

    //~ Instance fields --------------------------------------------------------

    protected EnkiMDRepository mdrRepos;
    private boolean openMdrReposSession;
    private String storageFactoryClassName;
    private final Properties storageProps;
    private final FarragoProperties farragoProperties;

    //~ Constructors -----------------------------------------------------------

    public FarragoModelLoader()
    {
        this(FarragoProperties.instance());
    }

    public FarragoModelLoader(FarragoProperties farragoProperties)
    {
        this.farragoProperties = farragoProperties;
        storageProps = new Properties();
    }

    //~ Methods ----------------------------------------------------------------

    public void close()
    {
        if (mdrRepos != null) {
            closeMdrSession();
            mdrRepos.shutdown();
            mdrRepos = null;
        }
    }

    public void closeMdrSession()
    {
        if (openMdrReposSession) {
            openMdrReposSession = false;
            mdrRepos.endSession();
        }
    }

    public MDRepository getMdrRepos()
    {
        return mdrRepos;
    }

    public FarragoPackage loadModel(
        String extentName,
        boolean userRepos)
    {
        initStorage(userRepos);
        return (FarragoPackage) mdrRepos.getExtent(extentName);
    }

    public void initStorage(boolean userRepos)
    {
        if (userRepos) {
            setUserReposProperties();
        } else {
            try {
                setSystemReposProperties();
            } catch (IOException ex) {
                throw FarragoResource.instance().CatalogPropsAccessFailed.ex(
                    getSystemReposFile().getAbsolutePath(),
                    ex);
            }
        }
        mdrRepos =
            MdrUtil.loadRepository(storageFactoryClassName, storageProps);

        mdrRepos.beginSession();
        openMdrReposSession = true;
    }

    public File getSystemReposFile()
    {
        File catalogDir = farragoProperties.getCatalogDir();
        return new File(catalogDir, "ReposStorage.properties");
    }

    private void setSystemReposProperties()
        throws IOException
    {
        File reposFile = getSystemReposFile();
        InputStream propsStream = new FileInputStream(reposFile);
        Properties props = new Properties();
        try {
            props.load(propsStream);
        } finally {
            propsStream.close();
        }

        //noinspection unchecked
        final Map<String, String> map = (Map) props;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            setStorageProperty(
                entry.getKey(),
                farragoProperties.expandProperties(
                    entry.getValue()));
        }
    }

    private void setUserReposProperties()
    {
        // REVIEW: SWZ: 2008-02-12: This will fail badly if used with
        // Enki+Hibernate.
        storageFactoryClassName = JdbcStorageFactory.class.getName();
        setStorageProperty(JdbcStorageFactory.STORAGE_URL, "jdbc:farrago:");
        setStorageProperty(JdbcStorageFactory.STORAGE_USER_NAME, "MDR");
        setStorageProperty(JdbcStorageFactory.STORAGE_SCHEMA_NAME, "MDR");
        setStorageProperty(
            JdbcStorageFactory.STORAGE_DRIVER_CLASS_NAME,
            "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver");
        setStorageProperty(
            JdbcStorageFactory.STORAGE_DATATYPE_STREAMABLE,
            "VARBINARY(10000)");
    }

    private void setStorageProperty(
        String name,
        String value)
    {
        tracer.fine(
            "Setting repository storage property '" + name + "' = [ "
            + value + " ]");
        storageProps.put(name, value);
    }

    public Properties getStorageProperties()
    {
        return storageProps;
    }

    public String getStorageFactoryClassName()
    {
        return JdbcStorageFactory.class.getName();
    }

    public FarragoProperties getFarragoProperties()
    {
        return farragoProperties;
    }
}

// End FarragoModelLoader.java
