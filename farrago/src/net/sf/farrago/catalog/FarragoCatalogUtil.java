/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;


import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.*;

import javax.jmi.reflect.*;
import java.util.*;
import java.lang.reflect.*;

import java.sql.Timestamp;

/**
 * Static utilities for accessing the Farrago catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoCatalogUtil
{
    /**
     * Sets default attributes for a new catalog instance.
     *
     * @param repos repository in which catalog is stored
     *
     * @param catalog catalog to initialize
     */
    public static void initializeCatalog(
        FarragoRepos repos,
        CwmCatalog catalog)
    {
        catalog.setDefaultCharacterSetName(repos.getDefaultCharsetName());
        catalog.setDefaultCollationName(repos.getDefaultCollationName());
    }
    
    /**
     * Calculates the number of parameters expected by a routine.
     * For functions, this is different from the number of parameters
     * defined in the repository, because CWM represents the return
     * type by appending an extra parameter.
     */
    public static int getRoutineParamCount(FemRoutine routine)
    {
        int nParams = routine.getParameter().size();
        if (routine.getType() == ProcedureTypeEnum.FUNCTION) {
            --nParams;
        }
        return nParams;
    }

    /**
     * Determines whether a routine is a constructor method.
     *
     * @param routine routine in question
     *
     * @return true if routine is a constructor method
     */
    public static boolean isRoutineConstructor(FemRoutine routine)
    {
        // TODO:  once we support non-constructor methods
        return isRoutineMethod(routine);
    }

    /**
     * Determines whether a routine is a method.
     *
     * @param routine routine in question
     *
     * @return true if routine is a method
     */
    public static boolean isRoutineMethod(FemRoutine routine)
    {
        return routine.getSpecification().getOwner() != routine;
    }

    /**
     * Sets the specification for a routine.
     *
     * @param repos repository storing the routine definition
     *
     * @param routine new routine
     *
     * @param typeDef owning type if a method, else null
     */
    public static void setRoutineSpecification(
        FarragoRepos repos,
        FemRoutine routine,
        FemUserDefinedType typeDef)
    {
        CwmOperation operation = repos.newCwmOperation();
        if (typeDef == null) {
            operation.setOwner(routine);
        } else {
            operation.setOwner(typeDef);
        }
        operation.setName(routine.getName());
        operation.setAbstract(false);
        routine.setSpecification(operation);
    }

    /**
     * Determines whether an index is temporary.
     *
     * @param index the index in question
     *
     * @return true if temporary
     */
    public static boolean isIndexTemporary(CwmSqlindex index)
    {
        return getIndexTable(index).isTemporary();
    }

    /**
     * Gets the table on which an index is defined.
     *
     * @param index the index in question
     *
     * @return containing table
     */
    public static CwmTable getIndexTable(CwmSqlindex index)
    {
        return (CwmTable) index.getSpannedClass();
    }
    
    /**
     * Determines whether an index implements its table's primary key.
     *
     * @param index the index in question
     *
     * @return true if is the primary key index
     */
    public static boolean isIndexPrimaryKey(FemLocalIndex index)
    {
        return index.getName().startsWith("SYS$PRIMARY_KEY");
    }
    
    /**
     * Finds the primary key for a table.
     *
     * @param table the table of interest
     *
     * @return the PrimaryKey constraint, or null if none is defined
     */
    public static FemPrimaryKeyConstraint getPrimaryKey(CwmClassifier table)
    {
        Iterator iter = table.getOwnedElement().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (obj instanceof FemPrimaryKeyConstraint) {
                return (FemPrimaryKeyConstraint) obj;
            }
        }
        return null;
    }
    
    /**
     * Finds the clustered index storing a table's data.
     *
     * @param repos repository storing the table definition
     *
     * @param table the table to access
     *
     * @return clustered index or null if none
     */
    public static FemLocalIndex getClusteredIndex(
        FarragoRepos repos, CwmClass table)
    {
        Iterator iter = getTableIndexes(repos, table).iterator();
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();
            if (index.isClustered()) {
                return index;
            }
        }
        return null;
    }

    /**
     * Gets the collection of indexes spanning a table.
     *
     * @param repos repository storing the table definition
     *
     * @param table the table of interest
     *
     * @return index collection
     */
    public static Collection getTableIndexes(
        FarragoRepos repos, CwmClass table)
    {
        return
            repos.getKeysIndexesPackage().getIndexSpansClass().getIndex(table);
    }

    /**
     * Sets the generated name for an index used to implement a constraint.
     *
     * @param repos repos storing index
     * @param constraint the constraint being implemented
     * @param index the index implementing the constraint
     */
    public static void generateConstraintIndexName(
        FarragoRepos repos, 
        FemAbstractUniqueConstraint constraint,
        CwmSqlindex index)
    {
        String name =
            "SYS$CONSTRAINT_INDEX$" + constraint.getNamespace().getName()
            + "$" + constraint.getName();
        index.setName(uniquifyGeneratedName(repos, constraint, name));
    }

    /**
     * Sets the generated name for an anonymous constraint.
     *
     * @param repos repos storing constraint
     * @param constraint the anonymous constraint
     */
    public static void generateConstraintName(
        FarragoRepos repos, 
        FemAbstractUniqueConstraint constraint)
    {
        if (constraint instanceof FemPrimaryKeyConstraint) {
            constraint.setName("SYS$PRIMARY_KEY");
        } else {
            String name =
                "SYS$UNIQUE_KEY$"
                + generateUniqueConstraintColumnList(constraint);
            constraint.setName(uniquifyGeneratedName(repos, constraint, name));
        }
    }
    
    /**
     * Generated names are normally unique by construction.  However, if they
     * exceed the name length limit, truncation could cause collisions.  In
     * that case, we use repository object ID's to distinguish them.
     *
     * @param repos repos storing object
     * @param refObj object for which to construct name
     * @param name generated name
     *
     * @return uniquified name
     */
    private static String uniquifyGeneratedName(
        FarragoRepos repos, 
        RefObject refObj,
        String name)
    {
        if (name.length() <= repos.getIdentifierPrecision()) {
            return name;
        }
        String mofId = refObj.refMofId();
        return name.substring(
            0, repos.getIdentifierPrecision() - (mofId.length() + 1))
            + "_"
            + mofId;
    }

    private static String generateUniqueConstraintColumnList(
        FemAbstractUniqueConstraint constraint)
    {
        StringBuffer sb = new StringBuffer();
        Iterator iter = constraint.getFeature().iterator();
        while (iter.hasNext()) {
            CwmColumn column = (CwmColumn) iter.next();

            // TODO:  deal with funny chars
            sb.append(column.getName());
            if (iter.hasNext()) {
                sb.append("_");
            }
        }
        return sb.toString();
    }
    
    /**
     * Searches a collection for a CwmModelElement by name.
     *
     * @param collection the collection to search
     * @param name name of element to find
     *
     * @return CwmModelElement found, or null if not found
     */
    public static CwmModelElement getModelElementByName(
        Collection collection,
        String name)
    {
        Iterator iter = collection.iterator();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            if (element.getName().equals(name)) {
                return element;
            }
        }
        return null;
    }
    
    /**
     * Searches a collection for a CwmModelElement by name and type.
     *
     * @param collection the collection to search
     * @param name name of element to find
     * @param type class which sought object must instantiate
     *
     * @return CwmModelElement found, or null if not found
     */
    public static CwmModelElement getModelElementByNameAndType(
        Collection collection,
        String name,
        RefClass type)
    {
        Iterator iter = collection.iterator();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            if (!element.getName().equals(name)) {
                continue;
            }
            if (element.refIsInstanceOf(type.refMetaObject(), true)) {
                return element;
            }
        }
        return null;
    }
    
    /**
     * Filters a collection for all CwmModelElements of a given type.
     *
     * @param inCollection the collection to search
     * @param outCollection receives matching objects
     * @param type class which sought objects must instantiate
     */
    public static void filterTypedModelElements(
        Collection inCollection,
        Collection outCollection,
        Class type)
    {
        Iterator iter = inCollection.iterator();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            if (type.isInstance(element)) {
                outCollection.add(element);
            }
        }
    }

    /**
     * Indexes a collection of model elements by name.
     *
     * @param inCollection elements to be indexed
     *
     * @param outMap receives indexed elements; key is name, value is element
     */
    public static void indexModelElementsByName(
        Collection inCollection,
        Map outMap)
    {
        Iterator iter = inCollection.iterator();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            outMap.put(element.getName(), element);
        }
    }
    
    /**
     * Looks up a schema by name in a catalog.
     *
     * @param repos repos storing catalog
     *
     * @param catalog CwmCatalog to search
     *
     * @param schemaName name of schema to find
     *
     * @return schema definition, or null if not found
     */
    public static FemLocalSchema getSchemaByName(
        FarragoRepos repos,
        CwmCatalog catalog,
        String schemaName)
    {
        return (FemLocalSchema) getModelElementByNameAndType(
            catalog.getOwnedElement(),
            schemaName,
            repos.getSql2003Package().getFemLocalSchema());
    }

    /**
     * Determines whether a column may contain null values.  This must be used
     * rather than directly calling CwmColumn.getIsNullable, because a column
     * which is part of a primary key or clustered index may not contain
     * nulls even when its definition says it can.
     *
     * @param repos repos storing column definition
     *
     * @param column the column of interest
     *
     * @return whether nulls are allowed
     */
    public static boolean isColumnNullable(
        FarragoRepos repos,
        CwmColumn column)
    {
        if (column.getIsNullable().equals(NullableTypeEnum.COLUMN_NO_NULLS)) {
            return false;
        }

        CwmClassifier owner = column.getOwner();
        if (!(owner instanceof CwmTable)) {
            return true;
        }

        FemPrimaryKeyConstraint primaryKey =
            FarragoCatalogUtil.getPrimaryKey(owner);
        if (primaryKey != null) {
            if (primaryKey.getFeature().contains(column)) {
                return false;
            }
        }
        FemLocalIndex clusteredIndex = FarragoCatalogUtil.getClusteredIndex(
            repos, (CwmTable) owner);
        if (clusteredIndex != null) {
            Iterator iter = clusteredIndex.getIndexedFeature().iterator();
            while (iter.hasNext()) {
                CwmIndexedFeature indexedFeature =
                    (CwmIndexedFeature) iter.next();
                if (indexedFeature.getFeature().equals(column)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Gets the routine which implements a particular user-defined ordering,
     * or null if the ordering does not invoke a routine.
     *
     * @param ordering user-defined ordering of interest
     *
     * @return invoked routine or null
     */
    public static FemRoutine getRoutineForOrdering(FemUserDefinedOrdering udo)
    {
        Collection deps = udo.getOwnedElement();
        if (deps.isEmpty()) {
            return null;
        }
        assert(deps.size() == 1);
        CwmDependency dep = (CwmDependency) deps.iterator().next();
        assert(dep.getSupplier().size() == 1);
        return (FemRoutine) dep.getSupplier().iterator().next();
    }

    /**
     * Constructs a fully qualified name for an object.
     *
     * @param element model element
     *
     * @return qualified identifier
     */
    public static SqlIdentifier getQualifiedName(CwmModelElement element)
    {
        List names = new ArrayList(3);
        names.add(element.getName());
        for (CwmNamespace ns = element.getNamespace(); ns != null;
             ns = ns.getNamespace())
        {
            names.add(ns.getName());
        }
        Collections.reverse(names);
        String [] nameArray = (String []) names.toArray(Util.emptyStringArray);
        return new SqlIdentifier(nameArray, SqlParserPos.ZERO);
    }

    /**
     * Casts a {@link FemAbstractTypedElement} to the {@link
     * FemSqltypedElement} interface via a proxy.
     *
     * @param element element to cast
     *
     * @return cast result (a proxy)
     */
    public static FemSqltypedElement toFemSqltypedElement(
        final FemAbstractTypedElement element)
    {
        Class [] interfaces = element.getClass().getInterfaces();
        Class [] newInterfaces = new Class[interfaces.length + 1];
        System.arraycopy(interfaces, 0, newInterfaces, 1, interfaces.length);
        newInterfaces[0] = FemSqltypedElement.class;

        return (FemSqltypedElement) Proxy.newProxyInstance(
            FarragoCatalogUtil.class.getClassLoader(),
            newInterfaces,
            new InvocationHandler()
            {
                // implement InvocationHandler
                public Object invoke(
                    Object proxy,
                    Method method,
                    Object [] args)
                    throws Throwable
                {
                    if (method.getName().equals("getModelElement")) {
                        return element;
                    }
                    
                    // delegate by name
                    Method delegateMethod =
                        element.getClass().getMethod(
                            method.getName(),
                            method.getParameterTypes());
                    return delegateMethod.invoke(element, args);
                }
            });
    }

    /**
     * Returns a collection of just the structural features of a classifier,
     * hiding other features such as operations.
     *
     * @param classifier to access
     *
     * @return list of structural features
     */
    public static List getStructuralFeatures(CwmClassifier classifier)
    {
        List structuralFeatures = new ArrayList();
        Iterator iter = classifier.getFeature().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (obj instanceof CwmStructuralFeature) {
                structuralFeatures.add(obj);
            }
        }
        return structuralFeatures;
    }

    /**
     * Returns the URL for a jar with all properties expanded.
     *
     * @param femJar jar to access
     *
     * @return expanded URL as a string
     */
    public static String getJarUrl(FemJar femJar)
    {
        return FarragoProperties.instance().expandProperties(femJar.getUrl());
    }

    /**
     * Finds the FemAuthId for a specified Authorization name.
     *
     * @param repos repository storing the Authorization Id
     *
     * @param authName the input name used for this lookup
     *
     * @return repository element represents the authorization
     * identifier
     */
    public static FemAuthId getAuthIdByName(
        FarragoRepos repos, String authName)
    {
        Collection authIdCollection =
            repos.getSecurityPackage().getFemAuthId().
            refAllOfType();
        FemAuthId femAuthId = (FemAuthId)
            FarragoCatalogUtil.getModelElementByName(
                authIdCollection, authName);

        return femAuthId;
    }

    /**
     * Creates a new grant on a ROLE with specified role name and associate it
     * to the grantor and grantee auth ids respectively. By default, the admin
     * option is set to false. The caller will have to set it on the grant
     * object returned.
     *
     * @param repos repository containing the objects
     *
     * @param grantorName the creator of this grant
     *
     * @param granteeName the receipient of this grant
     *
     * @param roleName the role name of the authorization id to be granted by
     * this new grant
     *
     * @return new grant object
     */
    public static FemGrant newRoleGrant(
        FarragoRepos repos, String grantorName, String granteeName,
        String roleName)
    {
        FemAuthId grantorAuthId;
        FemAuthId granteeAuthId;
        FemAuthId grantedRole;
        
        // create a creation grant and set its properties
        FemGrant grant;

        // Find the authId by name for grantor and grantee
        grantorAuthId = FarragoCatalogUtil.getAuthIdByName(repos, grantorName);
        granteeAuthId = FarragoCatalogUtil.getAuthIdByName(repos, granteeName);

        // Find the Fem role by name
        grantedRole = FarragoCatalogUtil.getAuthIdByName(repos, roleName); 
        if (grantedRole == null)
        {
            // TODO: throw res.instance().newRoleNameInvalid(roleName);
        }
        
        grant = newElementGrant(
            repos, grantorAuthId,  granteeAuthId, grantedRole);
        
        // set properties specific for a grant of a role
        grant.setAction(PrivilegedActionEnum.INHERIT_ROLE.toString());
        grant.setWithGrantOption(false);

        return grant;
    }

    /**
     * Creates a grant on a specified repos element, with AuthId's
     * specified as strings.
     *
     * @param repos repository storing the objects
     *
     * @param grantorName the creator of this grant
     *
     * @param granteeName the recipient of this grant
     *
     * @param grantedObject element being granted 
     *
     * @return grant a grant object
     */
    public static FemGrant newElementGrant(
        FarragoRepos repos, String grantorName, String granteeName,
        CwmModelElement grantedObject)
    {
        FemAuthId grantorAuthId;
        FemAuthId granteeAuthId;
        
        // Find the authId by name for grantor and grantee
        grantorAuthId = FarragoCatalogUtil.getAuthIdByName(repos, grantorName);
        granteeAuthId = FarragoCatalogUtil.getAuthIdByName(repos, granteeName);

        return newElementGrant(
            repos, grantorAuthId,  granteeAuthId, grantedObject);
    }
    
    /**
     * Create a new grant for an element, with AuthId's specified
     * as repository objects.
     *
     * @param repos repository storing the objects
     *
     * @param grantorAuthId the creator of this grant
     *
     * @param granteeAuthId the receipient of this grant
     *
     * @param grantedObject element being granted
     *
     * @return new grant object
     */
    public static FemGrant newElementGrant(
        FarragoRepos repos, FemAuthId grantorAuthId, FemAuthId granteeAuthId,
        CwmModelElement grantedObject)
    {
        FemAuthId grantedRole;
        
        // create a privilege object and set its properties
        FemGrant grant = repos.newFemGrant();        
        
        // TODO: to grant.setHierarchyOption(hierarchyOption);
        
        // associate the grant with the grantor and grantee
        grant.setGrantor(grantorAuthId);
        grant.setGrantee(granteeAuthId);
        grant.setElement(grantedObject);

        return grant;
    }

    /**
     * Creates a new creation grant.
     *
     * @param repos repository storing the objects
     *
     * @param grantorName the name of the creator of the grant
     *
     * @param granteeName the name of the grantee of the grant
     *
     * @param grantedObject element being created
     *
     * @return new grant object
     */
    public static FemCreationGrant newCreationGrant(
        FarragoRepos repos, String grantorName, String granteeName,
        CwmModelElement grantedObject)
    {
        FemAuthId grantorAuthId;
        FemAuthId granteeAuthId;
        
        // create a creation grant and set its properties
        FemCreationGrant grant = repos.newFemCreationGrant();

        // Find the authId by name for grantor and grantee
        grantorAuthId = FarragoCatalogUtil.getAuthIdByName(repos, grantorName);
        granteeAuthId = FarragoCatalogUtil.getAuthIdByName(repos, granteeName);
        
        // set the privilege name (i.e. action) and properties.
        grant.setAction(PrivilegedActionEnum.CREATION.toString());
        grant.setWithGrantOption(false);

        // TODO: set creation grant attributes
        Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
        grant.setCreationTimestamp(ts.toString());
        grant.setModificationTimestamp(grant.getCreationTimestamp());
        
        // associate the grant with the grantor and grantee respectively
        grant.setGrantor(grantorAuthId);
        grant.setGrantee(granteeAuthId);
        grant.setElement(grantedObject);
        return grant;
    }    
}

// End FarragoCatalogUtil.java
