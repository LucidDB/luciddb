/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
import java.util.*;

import net.sf.farrago.FarragoPackage;
import net.sf.farrago.util.FarragoProperties;
import net.sf.farrago.util.MdrUtil;
import net.sf.farrago.resource.FarragoResource;

import org.netbeans.api.mdr.*;
import org.netbeans.mdr.*;
import org.netbeans.mdr.persistence.btreeimpl.btreestorage.*;
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
    //~ Instance fields -------------------------------------------------------

    protected MDRepository mdrRepos;
    private String storageFactoryClassName;
    private Properties storageProps = new Properties();

    //~ Methods ---------------------------------------------------------------

    public void close()
    {
        if (mdrRepos != null) {
            org.netbeans.jmiimpl.mof.model.NamespaceImpl.clearContains();
            mdrRepos.shutdown();
            mdrRepos = null;
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
                throw FarragoResource.instance().newCatalogPropsAccessFailed(
                    getSystemReposFile().getAbsolutePath(), ex);
            }
        }
        mdrRepos =
            MdrUtil.loadRepository(storageFactoryClassName, storageProps);
    }

    public File getSystemReposFile()
    {
        File catalogDir = FarragoProperties.instance().getCatalogDir();
        return new File(catalogDir, "ReposStorage.properties");
    }

    private void setSystemReposProperties() throws IOException
    {
        File reposFile = getSystemReposFile();
        InputStream propsStream = new FileInputStream(reposFile);
        Properties props = new Properties();
        try {
            props.load(propsStream);
        } finally {
            propsStream.close();
        }

        FarragoProperties farragoProps = FarragoProperties.instance();
        
        Iterator iter = props.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            setStorageProperty(
                entry.getKey().toString(),
                farragoProps.expandProperties(
                    entry.getValue().toString()));
        }
    }

    private void setUserReposProperties()
    {
        storageFactoryClassName = JdbcStorageFactory.class.getName();
        setStorageProperty(JdbcStorageFactory.STORAGE_URL, "jdbc:farrago:");
        setStorageProperty(JdbcStorageFactory.STORAGE_USER_NAME, "MDR");
        setStorageProperty(JdbcStorageFactory.STORAGE_SCHEMA_NAME, "MDR");
        setStorageProperty(JdbcStorageFactory.STORAGE_DRIVER_CLASS_NAME,
            "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver");
        setStorageProperty(JdbcStorageFactory.STORAGE_DATATYPE_STREAMABLE,
            "VARBINARY(10000)");
    }

    private void setStorageProperty(
        String name,
        String value)
    {
        storageProps.put(name, value);
    }
}


// End FarragoModelLoader.java
