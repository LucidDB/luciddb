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
import org.netbeans.mdr.*;
import org.netbeans.api.mdr.*;
import org.netbeans.mdr.persistence.btreeimpl.btreestorage.*;
import org.netbeans.mdr.persistence.jdbcimpl.*;
import net.sf.farrago.FarragoPackage;
import net.sf.farrago.util.MdrUtil;

// NOTE:  This class gets compiled independently of everything else since
// it is used by build-time utilities such as ProxyGen.  That means it must
// have no dependencies on other non-model-generated Farrago code.
// It probably needs to be moved somewhere else.

/**
 * FarragoModelLoader is a utility class for loading the catalog model.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoModelLoader
{
    public static final String HOME_PROPERTY = "net.sf.farrago.home";

    private MDRepository repository;

    private String storageFactoryClassName;
    
    private Properties storageProps = new Properties();

    public void close()
    {
        if (repository != null) {
            repository.shutdown();
            repository = null;
        }
    }

    public MDRepository getRepository()
    {
        return repository;
    }
        
    public FarragoPackage loadModel(String extentName,boolean userCatalog)
    {
        if (userCatalog) {
            setUserCatalogProperties();
        } else {
            setSystemCatalogProperties();
        }
        repository = MdrUtil.loadRepository(
            storageFactoryClassName,
            storageProps);
        return (FarragoPackage) repository.getExtent(extentName);
    }

    private File getSystemCatalogFileSansExt()
    {
        String homeDir = System.getProperties().getProperty(HOME_PROPERTY);
        assert(homeDir != null);
        File catalogDir = new File(homeDir,"catalog");
        return new File(catalogDir,"FarragoCatalog");
    }

    public File getSystemCatalogFile()
    {
        return new File(getSystemCatalogFileSansExt().toString() + ".btd");
    }

    private void setSystemCatalogProperties()
    {
        File catalogFile = getSystemCatalogFileSansExt();

        storageFactoryClassName = BtreeFactory.class.getName();
            
        setStorageProperty(
            "org.netbeans.mdr.persistence.Dir",
            catalogFile.toString());
    }

    private void setUserCatalogProperties()
    {
        storageFactoryClassName = JdbcStorageFactory.class.getName();
        setStorageProperty(
            JdbcStorageFactory.STORAGE_URL,
            "jdbc:farrago:");
        setStorageProperty(
            JdbcStorageFactory.STORAGE_USER_NAME,
            "MDR");
        setStorageProperty(
            JdbcStorageFactory.STORAGE_SCHEMA_NAME,
            "MDR");
        setStorageProperty(
            JdbcStorageFactory.STORAGE_DRIVER_CLASS_NAME,
            "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver");
        setStorageProperty(
            JdbcStorageFactory.STORAGE_DATATYPE_STREAMABLE,
            "VARBINARY(10000)");
    }

    private void setStorageProperty(String name,String value)
    {
        storageProps.put(name,value);
    }
}

// End FarragoModelLoader.java
