/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.catalog;

import java.io.*;
import java.util.*;

import net.sf.farrago.FarragoPackage;
import net.sf.farrago.util.FarragoProperties;
import net.sf.farrago.util.MdrUtil;

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

    private MDRepository mdrRepos;
    private String storageFactoryClassName;
    private Properties storageProps = new Properties();

    //~ Methods ---------------------------------------------------------------

    public void close()
    {
        if (mdrRepos != null) {
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
        if (userRepos) {
            setUserReposProperties();
        } else {
            setSystemReposProperties();
        }
        mdrRepos =
            MdrUtil.loadRepository(storageFactoryClassName, storageProps);
        return (FarragoPackage) mdrRepos.getExtent(extentName);
    }

    private File getSystemReposFileSansExt()
    {
        File catalogDir = FarragoProperties.instance().getCatalogDir();
        return new File(catalogDir, "FarragoCatalog");
    }

    public File getSystemReposFile()
    {
        return new File(getSystemReposFileSansExt().toString() + ".btd");
    }

    private void setSystemReposProperties()
    {
        File reposFile = getSystemReposFileSansExt();

        storageFactoryClassName = BtreeFactory.class.getName();

        setStorageProperty(
            "org.netbeans.mdr.persistence.Dir",
            reposFile.toString());
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
