/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import javax.jmi.reflect.*;
import java.util.*;
import java.lang.reflect.*;

/**
 * Static utilities for accessing the Farrago catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoCatalogUtil
{
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
    public static CwmPrimaryKey getPrimaryKey(CwmClassifier table)
    {
        Iterator iter = table.getOwnedElement().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (obj instanceof CwmPrimaryKey) {
                return (CwmPrimaryKey) obj;
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
        CwmUniqueConstraint constraint,
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
        CwmUniqueConstraint constraint)
    {
        if (constraint instanceof CwmPrimaryKey) {
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
        CwmUniqueConstraint constraint)
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
        Class type)
    {
        Iterator iter = collection.iterator();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            if (!element.getName().equals(name)) {
                continue;
            }
            if (type.isInstance(element)) {
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
     * Looks up a schema by name in a catalog.
     *
     * @param catalog CwmCatalog to search
     *
     * @param schemaName name of schema to find
     *
     * @return schema definition, or null if not found
     */
    public static FemLocalSchema getSchemaByName(
        CwmCatalog catalog,
        String schemaName)
    {
        return (FemLocalSchema) getModelElementByNameAndType(
            catalog.getOwnedElement(),
            schemaName,
            FemLocalSchema.class);
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

        CwmPrimaryKey primaryKey = FarragoCatalogUtil.getPrimaryKey(owner);
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
        return new SqlIdentifier(nameArray, null);
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
                    if (method.getName().equals("getImpl")) {
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
}

// End FarragoCatalogUtil.java
