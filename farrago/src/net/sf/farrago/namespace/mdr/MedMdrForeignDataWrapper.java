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

package net.sf.farrago.namespace.mdr;

import net.sf.farrago.namespace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;

import org.netbeans.api.mdr.*;

import java.sql.*;
import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

// TODO:  change most asserts into proper exceptions

// TODO:  throw exception on unknown option?

/**
 * MedMdrForeignDataWrapper implements the FarragoForeignDataWrapper
 * interface by representing classes in an MDR repository as tables
 * (mapping the class extent to a corresponding set of rows).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMdrForeignDataWrapper
    implements FarragoForeignDataWrapper
{
    public static final String PROP_STORAGE_FACTORY_CLASS =
    "STORAGE_FACTORY_CLASS";
    
    public static final String PROP_EXTENT_NAME = "EXTENT_NAME";
    
    public static final String PROP_CLASS_NAME = "CLASS_NAME";
    
    public static final String PROP_ROOT_PACKAGE_NAME = "ROOT_PACKAGE_NAME";
    
    public static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";
    
    private MDRepository repository;

    private RefPackage rootPackage;

    private FarragoCatalog catalog;

    boolean foreignRepository;

    String serverMofId;

    String schemaName;
    
    /**
     * Default constructor for access to external repositories.
     */
    public MedMdrForeignDataWrapper()
    {
    }

    // implement FarragoForeignDataWrapper
    public String getSuggestedName()
    {
        return "MDR";
    }
    
    // implement FarragoForeignDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for MDR repository extents";
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getWrapperPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getColumnSetPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public DriverPropertyInfo [] getColumnPropertyInfo(Locale locale)
    {
        // TODO
        return new DriverPropertyInfo[0];
    }
    
    // implement FarragoForeignDataWrapper
    public void initialize(
        FarragoCatalog catalog,
        Properties props) throws SQLException
    {
        this.catalog = catalog;
        
        // nothing to do
        assert(props.isEmpty());
    }
    
    // implement FarragoForeignDataWrapper
    public FarragoForeignDataWrapper newServer(
        String serverMofId,
        Properties props) throws SQLException
    {
        Properties storageProps = new Properties();
        storageProps.putAll(props);
        
        String storageFactoryClassName = getNonStorageProperty(
            props,PROP_STORAGE_FACTORY_CLASS);
        String extentName = getNonStorageProperty(
            props,PROP_EXTENT_NAME);
        String rootPackageName = getNonStorageProperty(
            props,PROP_ROOT_PACKAGE_NAME);

        MedMdrForeignDataWrapper server =
            new MedMdrForeignDataWrapper();
        boolean success = false;
        try {
            server.serverMofId = serverMofId;
            server.catalog = catalog;

            server.schemaName = getNonStorageProperty(
                props,PROP_SCHEMA_NAME);
            
            if (extentName != null) {
                server.initAsForeignServer(
                    storageFactoryClassName,
                    extentName,
                    storageProps);
            } else {
                server.initAsCatalogServer();
            }

            if (rootPackageName != null) {
                server.findRootPackage(rootPackageName);
            }
            
            success = true;
            return server;
        } finally {
            if (!success) {
                server.closeAllocation();
            }
        }
    }

    private void initAsForeignServer(
        String storageFactoryClassName,
        String extentName,
        Properties storageProps)
    {
        foreignRepository = true;
        repository = MdrUtil.loadRepository(
            storageFactoryClassName,
            storageProps);
        rootPackage = repository.getExtent(extentName);
    }

    /**
     * @return the root package, or null if this is not a server
     */
    public RefPackage getRootPackage()
    {
        return rootPackage;
    }

    private void findRootPackage(String rootPackageName)
    {
        if (rootPackageName.equals("..")) {
            rootPackage = rootPackage.refImmediatePackage();
            return;
        }
        MedMdrNameDirectory directory = getMdrNameDirectory();
        String [] names = rootPackageName.split("\\.");
        rootPackage = directory.lookupRefPackage(names,names.length);
        assert(rootPackage != null);
    }

    private void initAsCatalogServer()
    {
        foreignRepository = false;
        repository = catalog.getRepository();
        rootPackage = catalog.farragoPackage;
    }

    private String getNonStorageProperty(Properties props,String propName)
    {
        String value = props.getProperty(propName);
        props.remove(propName);
        return value;
    }

    private MedMdrNameDirectory getMdrNameDirectory()
    {
        assert(repository != null);
        return new MedMdrNameDirectory(this,rootPackage);
    }
    
    // implement FarragoForeignDataWrapper
    public FarragoNameDirectory getNameDirectory()
        throws SQLException
    {
        return getMdrNameDirectory();
    }
    
    // implement FarragoForeignDataWrapper
    public FarragoNamedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        SaffronType rowType,
        Map columnPropMap)
        throws SQLException
    {
        assert(repository != null);
        String className = tableProps.getProperty(PROP_CLASS_NAME);
        assert(className != null);

        MedMdrNameDirectory directory = getMdrNameDirectory();
        return directory.lookupColumnSetAndImposeType(
            typeFactory,
            className.split("\\."),
            localName,
            rowType);
    }

    // implement FarragoForeignDataWrapper
    public Object getRuntimeSupport(Object param) throws SQLException
    {
        String [] qualifiedName = (String []) param;
        return getMdrNameDirectory().lookupRefClass(qualifiedName);
    }
    
    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (repository != null) {
            if (foreignRepository) {
                repository.shutdown();
            }
            repository = null;
        }
    }
}

// End MedMdrForeignDataWrapper.java
