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
import net.sf.farrago.namespace.impl.*;

import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.query.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;
import net.sf.saffron.oj.stmt.*;

import org.netbeans.api.mdr.*;

import openjava.mop.*;
import openjava.ptree.*;

import java.sql.*;
import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

// TODO:  change most asserts into proper exceptions

// TODO:  throw exception on unknown option?

/**
 * MedMdrDataServer implements the {@link FarragoMedDataServer} interface
 * for MDR data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrDataServer extends MedAbstractDataServer
{
    public static final String PROP_STORAGE_FACTORY_CLASS =
    "STORAGE_FACTORY_CLASS";
    
    public static final String PROP_EXTENT_NAME = "EXTENT_NAME";
    
    public static final String PROP_CLASS_NAME = "CLASS_NAME";
    
    public static final String PROP_ROOT_PACKAGE_NAME = "ROOT_PACKAGE_NAME";
    
    public static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";

    FarragoCatalog catalog;
    
    MDRepository repository;

    RefPackage rootPackage;

    boolean foreignRepository;

    String schemaName;
    
    MedMdrDataServer(
        String serverMofId,
        Properties props,
        FarragoCatalog catalog)
    {
        super(serverMofId,props);
        this.catalog = catalog;
    }

    /**
     * @return the root package, or null if this is not a server
     */
    public RefPackage getRootPackage()
    {
        return rootPackage;
    }

    void initialize()
    {
        Properties props = getProperties();
        
        Properties storageProps = new Properties();
        storageProps.putAll(props);
        
        String storageFactoryClassName = getNonStorageProperty(
            props,PROP_STORAGE_FACTORY_CLASS);
        String extentName = getNonStorageProperty(
            props,PROP_EXTENT_NAME);
        String rootPackageName = getNonStorageProperty(
            props,PROP_ROOT_PACKAGE_NAME);

        schemaName = getNonStorageProperty(
            props,PROP_SCHEMA_NAME);
            
        if (extentName != null) {
            initAsForeignServer(
                storageFactoryClassName,
                extentName,
                storageProps);
        } else {
            initAsCatalogServer();
        }

        if (rootPackageName != null) {
            setRootPackage(rootPackageName);
        }
    }

    private void setRootPackage(String rootPackageName)
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
    
    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return getMdrNameDirectory();
    }
    
    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
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

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param) throws SQLException
    {
        if (param == null) {
            return repository;
        }
        String [] qualifiedName = (String []) param;
        return getMdrNameDirectory().lookupRefBaseObject(qualifiedName);
    }
    
    // implement FarragoMedDataServer
    public void registerRules(SaffronPlanner planner)
    {
        planner.addRule(new MedMdrJoinRule());
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

    Expression generateRuntimeSupportCall(Expression arg)
    {
        Variable connectionVariable =
            new Variable(OJStatement.connectionVariable);
        return new MethodCall(
            connectionVariable,
            "getDataServerRuntimeSupport",
            new ExpressionList(
                Literal.makeLiteral(getServerMofId()),
                arg));
    }
}

// End MedMdrDataServer.java
