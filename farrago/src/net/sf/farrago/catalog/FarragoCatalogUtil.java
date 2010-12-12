/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

import java.lang.reflect.*;

import java.sql.Timestamp;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.util.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * Static utilities for accessing the Farrago catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoCatalogUtil
{
    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration of the different type of row count statistics
     */
    private enum RowCountStatType
    {
        ROW_COUNT, DELETED_ROW_COUNT, ANALYZE_ROW_COUNT
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets default attributes for a new catalog instance.
     *
     * @param repos repository in which catalog is stored
     * @param catalog catalog to initialize
     */
    public static void initializeCatalog(
        FarragoRepos repos,
        CwmCatalog catalog)
    {
        catalog.setDefaultCharacterSetName(
            SaffronProperties.instance().defaultCharset.get());
        catalog.setDefaultCollationName(
            SaffronProperties.instance().defaultCollation.get());
    }

    /**
     * Calculates the number of parameters expected by a routine. For functions,
     * this is different from the number of parameters defined in the
     * repository, because CWM represents the return type by appending an extra
     * parameter.
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
        return !routine.getSpecification().getOwner().equals(routine);
    }

    /**
     * Sets the specification for a routine.
     *
     * @param repos repository storing the routine definition
     * @param routine new routine
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
     * Gets the {@link FemLocalTable} on which a {@link FemLocalIndex} is
     * defined.
     *
     * @param index the index in question
     *
     * @return containing table
     */
    public static FemLocalTable getIndexTable(FemLocalIndex index)
    {
        return (FemLocalTable) index.getSpannedClass();
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
        return index.getName().startsWith(
            "SYS$CONSTRAINT_INDEX$SYS$PRIMARY_KEY");
    }

    /**
     * Determines whether an index implements either a primary key or a unique
     * constraint
     *
     * @param index the index in question
     *
     * @return true if is either the primary key index or a unique constraint
     * index
     */
    public static boolean isIndexUnique(FemLocalIndex index)
    {
        return ((CwmIndex) index).isUnique();
    }

    /**
     * Determines whether an index implements an internal deletion index.
     *
     * @param index the index in question
     *
     * @return true if index is the deletion index
     */
    public static boolean isDeletionIndex(FemLocalIndex index)
    {
        return index.getName().startsWith("SYS$DEL");
    }

    /**
     * Finds the unique key constraints for a table.
     *
     * @param table the table of interest
     *
     * @return a list of unique key constraints, or an empty list if none is
     * defined
     */
    public static List<FemUniqueKeyConstraint> getUniqueKeyConstraints(
        CwmClassifier table)
    {
        List<FemUniqueKeyConstraint> listOfConstraints =
            new ArrayList<FemUniqueKeyConstraint>();

        for (Object obj : table.getOwnedElement()) {
            if (obj instanceof FemUniqueKeyConstraint) {
                listOfConstraints.add((FemUniqueKeyConstraint) obj);
            }
        }
        return listOfConstraints;
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
        for (CwmModelElement obj : table.getOwnedElement()) {
            if (obj instanceof FemPrimaryKeyConstraint) {
                return (FemPrimaryKeyConstraint) obj;
            }
        }
        return null;
    }

    /**
     * Returns a bitmap with bits set for any column from a table that is part
     * of either a primary key or a unique constraint
     *
     * @param table the table of interest
     *
     * @return bitmap with set bits
     */
    public static BitSet getUniqueKeyCols(CwmClassifier table)
    {
        BitSet uniqueCols = new BitSet();

        // first retrieve the columns from the primary key
        FemPrimaryKeyConstraint primKey =
            FarragoCatalogUtil.getPrimaryKey(table);
        if (primKey != null) {
            addKeyCols((List) primKey.getFeature(), uniqueCols);
        }

        // then, loop through each unique constraint
        List<FemUniqueKeyConstraint> uniqueConstraints =
            FarragoCatalogUtil.getUniqueKeyConstraints(table);
        for (FemUniqueKeyConstraint uniqueConstraint : uniqueConstraints) {
            addKeyCols((List) uniqueConstraint.getFeature(), uniqueCols);
        }

        return uniqueCols;
    }

    private static void addKeyCols(List<FemAbstractColumn> keyCols,
        BitSet keys)
    {
        for (FemAbstractColumn keyCol : keyCols) {
            keys.set(keyCol.getOrdinal());
        }
    }

    /**
     * Determines whether a table contains a unique key
     *
     * @param table the table of interest
     *
     * @return true if the table has a unique key
     */
    public static boolean hasUniqueKey(CwmClassifier table)
    {
        for (CwmModelElement obj : table.getOwnedElement()) {
            if ((obj instanceof FemPrimaryKeyConstraint)
                || (obj instanceof FemUniqueKeyConstraint))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds the clustered index storing a table's data.
     *
     * @param repos repository storing the table definition
     * @param table the table to access
     *
     * @return clustered index or null if none
     */
    public static FemLocalIndex getClusteredIndex(
        FarragoRepos repos,
        CwmClass table)
    {
        for (FemLocalIndex index : getTableIndexes(repos, table)) {
            if (index.isClustered()) {
                return index;
            }
        }
        return null;
    }

    /**
     * Finds all clustered indexes storing a table's data.
     *
     * @param repos repository storing the table definition
     * @param table the table to access
     *
     * @return list of clustered indexes or an empty list if none
     */
    public static List<FemLocalIndex> getClusteredIndexes(
        FarragoRepos repos,
        CwmClass table)
    {
        ArrayList<FemLocalIndex> indexList = new ArrayList<FemLocalIndex>();

        for (FemLocalIndex index : getTableIndexes(repos, table)) {
            if (index.isClustered()) {
                indexList.add(index);
            }
        }
        return indexList;
    }

    /**
     * Finds all unclustered indexes storing a table's data.
     *
     * @param repos repository storing the table definition
     * @param table the table to access
     *
     * @return list of clustered indexes or an empty list if none
     */
    public static List<FemLocalIndex> getUnclusteredIndexes(
        FarragoRepos repos,
        CwmClass table)
    {
        ArrayList<FemLocalIndex> indexList = new ArrayList<FemLocalIndex>();

        for (FemLocalIndex index : getTableIndexes(repos, table)) {
            if (!index.isClustered() && !isDeletionIndex(index)) {
                indexList.add(index);
            }
        }
        return indexList;
    }

    /**
     * Returns the index corresponding to the internal deletion index
     *
     * @param repos repository storing the table definition
     * @param table the table to access
     *
     * @return the deletion index if it exists, otherwise NULL
     */
    public static FemLocalIndex getDeletionIndex(
        FarragoRepos repos,
        CwmClass table)
    {
        for (FemLocalIndex index : getTableIndexes(repos, table)) {
            if (isDeletionIndex(index)) {
                return index;
            }
        }
        return null;
    }

    /**
     * Gets the collection of indexes spanning a table.
     *
     * @param repos repository storing the table definition
     * @param table the table of interest
     *
     * @return index collection
     */
    public static Collection<FemLocalIndex> getTableIndexes(
        final FarragoRepos repos,
        final CwmClass table)
    {
        // REVIEW: SWZ: 2008-02-11:  The association IndexSpansClass is
        // between CwmClass and CwmIndex.  However, Farrago never creates
        // any CwmIndex (or CqmSqlindex) instances, only FemLocalIndex
        // instances.  Netbeans MDR doesn't generate generic types for return
        // values, but Enki does.  Consider modifying calls to this method to
        // do the conversion as necessary.  Or perhaps introduce a
        // getModelElementByType method.

        // REVIEW: SWZ: 2008-04-23: Force deterministic order onto the indexes.
        // Many unit tests end up depending on this for reliable ordering
        // of output.
        List<FemLocalIndex> result = new ArrayList<FemLocalIndex>();
        for (
            CwmIndex index
            : repos.getKeysIndexesPackage().getIndexSpansClass().getIndex(
                table))
        {
            result.add((FemLocalIndex) index);
        }
        Collections.sort(result, JmiMofIdComparator.instance);

        return result;
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
        String name = "SYS$CONSTRAINT_INDEX$" + constraint.getName();
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
        String name;
        if (constraint instanceof FemPrimaryKeyConstraint) {
            name = "SYS$PRIMARY_KEY$"
                + constraint.getNamespace().getName();
        } else {
            name =
                "SYS$UNIQUE_KEY$"
                + constraint.getNamespace().getName()
                + "$"
                + generateUniqueConstraintColumnList(constraint);
        }
        constraint.setName(uniquifyGeneratedName(repos, constraint, name));
    }

    /**
     * Generated names are normally unique by construction. However, if they
     * exceed the name length limit, truncation could cause collisions. In that
     * case, we use repository object ID's to distinguish them.
     *
     * @param repos repos storing object
     * @param refObj object for which to construct name
     * @param name generated name
     *
     * @return uniquified name
     */
    public static String uniquifyGeneratedName(
        FarragoRepos repos,
        RefObject refObj,
        String name)
    {
        if (name.length() <= repos.getIdentifierPrecision()) {
            return name;
        }
        String mofId = refObj.refMofId();
        return name.substring(
            0,
            repos.getIdentifierPrecision()
            - (mofId.length() + 1))
            + "_"
            + mofId;
    }

    private static String generateUniqueConstraintColumnList(
        FemAbstractUniqueConstraint constraint)
    {
        StringBuilder sb = new StringBuilder();
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
     * Searches a collection of {@link CwmModelElement}s (or a subtype) by name.
     *
     * @param collection the collection to search
     * @param name name of element to find
     *
     * @return CwmModelElement found, or null if not found
     */
    public static <T extends CwmModelElement> T getModelElementByName(
        Collection<T> collection,
        String name)
    {
        for (T element : collection) {
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
     * @param clazz class which sought object must instantiate
     *
     * @return CwmModelElement found, or null if not found
     */
    public static <T extends CwmModelElement> T getModelElementByNameAndType(
        Collection<? extends CwmModelElement> collection,
        String name,
        Class<T> clazz)
    {
        for (CwmModelElement element : collection) {
            if (!element.getName().equals(name)) {
                continue;
            }
            if (clazz.isInstance(element)) {
                return clazz.cast(element);
            }
        }
        return null;
    }

    /**
     * @deprecated use typesafe version instead
     */
    public static CwmModelElement getModelElementByNameAndType(
        Collection collection,
        String name,
        RefClass type)
    {
        return getModelElementByNameAndType(
            (Collection<CwmModelElement>) collection,
            name,
            (Class<CwmModelElement>) JmiObjUtil.getClassForRefClass(type));
    }

    /**
     * Filters a collection for all {@link CwmModelElement}s of a given type.
     *
     * @param inCollection the collection to search
     * @param outCollection receives matching objects
     * @param type class which sought objects must instantiate
     *
     * @see Util#filter(List, java.lang.Class)
     */
    public static <OutT extends CwmModelElement, AskT extends OutT> void
    filterTypedModelElements(
        Collection<? extends CwmModelElement> inCollection,
        Collection<OutT> outCollection,
        Class<AskT> type)
    {
        for (CwmModelElement element : inCollection) {
            if (type.isInstance(element)) {
                outCollection.add(type.cast(element));
            }
        }
    }

    /**
     * Indexes a collection of model elements by name.
     *
     * @param inCollection elements to be indexed
     * @param outMap receives indexed elements; key is name, value is element
     */
    public static <T extends CwmModelElement> void indexModelElementsByName(
        Collection<T> inCollection,
        Map<String, T> outMap)
    {
        for (T element : inCollection) {
            outMap.put(
                element.getName(),
                element);
        }
    }

    /**
     * Looks up a schema by name in a catalog.
     *
     * @param catalog CwmCatalog to search
     * @param schemaName name of schema to find
     *
     * @return schema definition, or null if not found
     */
    public static FemLocalSchema getSchemaByName(
        CwmCatalog catalog,
        String schemaName)
    {
        return getModelElementByNameAndType(
            catalog.getOwnedElement(),
            schemaName,
            FemLocalSchema.class);
    }

    /**
     * Determines whether a column may contain null values. This must be used
     * rather than directly calling CwmColumn.getIsNullable, because a column
     * which is part of a primary key or clustered index may not contain nulls
     * even when its definition says it can.
     *
     * <p>REVIEW jvs 7-July-2006: The statement above is no longer true; we now
     * store the derived nullability in isNullable, and remember the original
     * declared nullability in isDeclaredNullable. Maybe we should deprecate
     * this method now.
     *
     * @param repos repos storing column definition
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
        } else {
            return true;
        }
    }

    /**
     * Gets the routine which implements a particular user-defined ordering, or
     * null if the ordering does not invoke a routine.
     *
     * @param udo user-defined ordering of interest
     *
     * @return invoked routine or null
     */
    public static FemRoutine getRoutineForOrdering(FemUserDefinedOrdering udo)
    {
        Collection<CwmModelElement> deps = udo.getOwnedElement();
        if (deps.isEmpty()) {
            return null;
        }
        assert (deps.size() == 1);
        CwmDependency dep = (CwmDependency) deps.iterator().next();
        assert (dep.getSupplier().size() == 1);
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
        List<String> names = new ArrayList<String>(3);
        names.add(element.getName());
        for (
            CwmNamespace ns = element.getNamespace();
            ns != null;
            ns = ns.getNamespace())
        {
            names.add(ns.getName());
        }
        Collections.reverse(names);
        String [] nameArray = names.toArray(new String[names.size()]);
        return new SqlIdentifier(nameArray, SqlParserPos.ZERO);
    }

    /**
     * Casts a {@link FemAbstractTypedElement} to the {@link FemSqltypedElement}
     * interface via a proxy.
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
            new InvocationHandler() {
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
    public static List<CwmStructuralFeature> getStructuralFeatures(
        CwmClassifier classifier)
    {
        return Util.filter(
            classifier.getFeature(),
            CwmStructuralFeature.class);
    }

    /**
     * Determines whether a UDF has a RETURNS TABLE clause.
     *
     * @param routine UDF
     *
     * @return true if RETURNS TABLE
     */
    public static boolean isTableFunction(FemRoutine routine)
    {
        return !getStructuralFeatures(routine).isEmpty();
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
     * @param authName the input name used for this lookup
     *
     * @return repository element represents the authorization identifier
     */
    public static FemAuthId getAuthIdByName(
        FarragoRepos repos,
        String authName)
    {
        return FarragoCatalogUtil.getModelElementByName(
            repos.allOfType(FemAuthId.class),
            authName);
    }

    /**
     * Looks up a user by name in a catalog.
     *
     * @param repos repos storing catalog
     * @param userName name of user to find
     *
     * @return user definition, or null if not found
     */
    public static FemUser getUserByName(
        FarragoRepos repos,
        String userName)
    {
        return FarragoCatalogUtil.getModelElementByName(
            repos.allOfType(FemUser.class),
            userName);
    }

    /**
     * Looks up a role by name in a catalog.
     *
     * @param repos repos storing catalog
     * @param roleName name of role to find
     *
     * @return role definition, or null if not found
     */
    public static FemRole getRoleByName(
        FarragoRepos repos,
        String roleName)
    {
        return FarragoCatalogUtil.getModelElementByName(
            repos.allOfType(FemRole.class),
            roleName);
    }

    /**
     * Creates a new grant of a role and associates it
     * with the grantor and grantee auth ids respectively. By default, the admin
     * option is set to false. The caller will have to set it on the grant
     * object returned.
     *
     * @param repos repository containing the objects
     * @param grantorName the creator of this grant
     * @param granteeId the receipient of this grant
     * @param roleName the role to be granted
     *
     * @return new grant object
     */
    public static FemGrant newRoleGrant(
        FarragoRepos repos,
        FemAuthId grantorAuthId,
        FemAuthId granteeAuthId,
        FemAuthId grantedRole)
    {
        // create a creation grant and set its properties
        FemGrant grant =
            newElementGrant(
                repos,
                grantorAuthId,
                granteeAuthId,
                grantedRole);

        // set properties specific for a grant of a role
        grant.setAction(PrivilegedActionEnum.INHERIT_ROLE.toString());
        grant.setWithGrantOption(false);

        return grant;
    }

    /**
     * Creates a grant on a specified repos element, with AuthId's specified as
     * strings.
     *
     * @param repos repository storing the objects
     * @param grantorName the creator of this grant
     * @param granteeName the recipient of this grant
     * @param grantedObject element being granted
     *
     * @return grant a grant object
     */
    public static FemGrant newElementGrant(
        FarragoRepos repos,
        String grantorName,
        String granteeName,
        CwmModelElement grantedObject)
    {
        FemAuthId grantorAuthId;
        FemAuthId granteeAuthId;

        // Find the authId by name for grantor and grantee
        grantorAuthId = FarragoCatalogUtil.getAuthIdByName(repos, grantorName);
        granteeAuthId = FarragoCatalogUtil.getAuthIdByName(repos, granteeName);

        return newElementGrant(
            repos,
            grantorAuthId,
            granteeAuthId,
            grantedObject);
    }

    /**
     * Create a new grant for an element, with AuthId's specified as repository
     * objects.
     *
     * @param repos repository storing the objects
     * @param grantorAuthId the creator of this grant
     * @param granteeAuthId the receipient of this grant
     * @param grantedObject element being granted
     *
     * @return new grant object
     */
    public static FemGrant newElementGrant(
        FarragoRepos repos,
        FemAuthId grantorAuthId,
        FemAuthId granteeAuthId,
        CwmModelElement grantedObject)
    {
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
     * Creates a new grant representing ownership of an object by its creator.
     *
     * @param repos repository storing the objects
     * @param grantorName the name of the creator of the grant
     * @param granteeName the name of the grantee of the grant
     * @param grantedObject element being created
     *
     * @return new grant object
     */
    public static FemGrant newCreationGrant(
        FarragoRepos repos,
        String grantorName,
        String granteeName,
        CwmModelElement grantedObject)
    {
        // Find the authId by name for grantor and grantee
        FemAuthId grantorAuthId =
            FarragoCatalogUtil.getAuthIdByName(repos, grantorName);
        FemAuthId granteeAuthId =
            FarragoCatalogUtil.getAuthIdByName(repos, granteeName);

        assert (grantorAuthId != null);
        assert (granteeAuthId != null);

        return newCreationGrant(
            repos,
            grantorAuthId,
            granteeAuthId,
            grantedObject);
    }

    /**
     * Creates a new grant representing ownership of an object by its creator.
     *
     * @param repos repository storing the objects
     * @param grantorAuthId a FemAuthId representing the creator of the grant
     * @param granteeAuthId a FemAuthId representing the grantee of the grant
     * @param grantedObject element being created
     *
     * @return new grant object
     */
    public static FemGrant newCreationGrant(
        FarragoRepos repos,
        FemAuthId grantorAuthId,
        FemAuthId granteeAuthId,
        CwmModelElement grantedObject)
    {
        // create a creation grant and set its properties
        FemGrant grant = repos.newFemGrant();

        // set the privilege name (i.e. action) and properties.
        grant.setAction(PrivilegedActionEnum.CREATION.toString());
        grant.setWithGrantOption(false);

        // associate the grant with the grantor and grantee respectively
        grant.setGrantor(grantorAuthId);
        grant.setGrantee(granteeAuthId);
        grant.setElement(grantedObject);
        return grant;
    }

    /**
     * Determines the allowed access for a table
     *
     * @param table Repository table
     *
     * @return Access type of the table
     */
    public static SqlAccessType getTableAllowedAccess(CwmNamedColumnSet table)
    {
        SqlAccessType allowedAccess;
        if (table instanceof FemBaseColumnSet) {
            FemBaseColumnSet cs = (FemBaseColumnSet) table;
            String accessNames = cs.getAllowedAccess();
            if (accessNames != null) {
                allowedAccess = SqlAccessType.create(accessNames);
            } else {
                allowedAccess = SqlAccessType.ALL;
            }
        } else if (table instanceof CwmView) {
            CwmView view = (CwmView) table;
            allowedAccess =
                view.isReadOnly() ? SqlAccessType.READ_ONLY : SqlAccessType.ALL;
        } else {
            allowedAccess = SqlAccessType.ALL;
        }
        return allowedAccess;
    }

    /**
     * Retrieves the current and deleted row counts for an abstract column set,
     * optionally based on a label setting.
     *
     * @param table the abstract column set
     * @param labelTimestamp creation timestamp of the label setting that
     * determines which row counts to retrieve; null if there is no label
     * setting
     * @param rowCounts the row counts to be returned; the first element in the
     * array is the current row count and the second is the deleted row count
     */
    public static void getRowCounts(
        FemAbstractColumnSet table,
        Timestamp labelTimestamp,
        Long [] rowCounts)
    {
        // If there's no label setting, retrieve the latest stats.
        if (labelTimestamp == null) {
            rowCounts[0] = table.getRowCount();
            rowCounts[1] = table.getDeletedRowCount();
            return;
        }

        // Older catalog versions and newly created tables only store
        // the row counts directly in the columnSet record so there won't
        // be separate row count stat records yet.
        List<FemRowCountStatistics> rowCountStatsList =
            table.getRowCountStats();
        if (rowCountStatsList.isEmpty()) {
            rowCounts[0] = table.getRowCount();
            rowCounts[1] = table.getDeletedRowCount();
            return;
        } else {
            // Work backwards through the list until we find the first set
            // of row counts that are older than the label timestamp passed
            // in.
            for (int i = rowCountStatsList.size() - 1; i >= 0; i--) {
                FemRowCountStatistics stats = rowCountStatsList.get(i);
                Timestamp statTime = getMaxTimestamp(table, stats);
                if ((statTime == null)
                    || (statTime.compareTo(labelTimestamp) < 0))
                {
                    rowCounts[0] = stats.getRowCount();
                    rowCounts[1] = stats.getDeletedRowCount();
                    return;
                }
            }
        }

        // If we reach this point, then the current set of row counts were all
        // generated after the label setting, so no row count stats are
        // available.
        rowCounts[0] = null;
        rowCounts[1] = null;
    }

    /**
     * Returns the max of the dml and analyze timestamps stored in a row count
     * statistics record. If both timestamps are null, returns null.
     *
     * @param table the abstract column set corresponding to the row count stats
     * record
     * @param stats the row count stats record
     *
     * @return maximum of the dml and analyze timestamps or null if both
     * timestamps are null
     */
    private static Timestamp getMaxTimestamp(
        FemAbstractColumnSet table,
        FemRowCountStatistics stats)
    {
        String dmlTime = stats.getDmlTimestamp();
        String analyzeTime = stats.getAnalyzeTimestamp();
        if (analyzeTime == null) {
            if (dmlTime == null) {
                return null;
            } else {
                return Timestamp.valueOf(dmlTime);
            }
        }
        if (dmlTime == null) {
            return Timestamp.valueOf(analyzeTime);
        }

        Timestamp dmlTimestamp = Timestamp.valueOf(dmlTime);
        Timestamp analyzeTimestamp = Timestamp.valueOf(analyzeTime);
        if (dmlTimestamp.compareTo(analyzeTimestamp) > 0) {
            return dmlTimestamp;
        } else {
            return analyzeTimestamp;
        }
    }

    /**
     * Updates the current and deleted row counts for an abstract column set,
     * creating new row count stat records as needed.
     *
     * @param table the abstract column set
     * @param rowCount the current row count
     * @param deletedRowCount the deleted row count
     * @param repos repository
     */
    public static void updateRowCounts(
        FemAbstractColumnSet table,
        long rowCount,
        long deletedRowCount,
        FarragoRepos repos)
    {
        List<RowCountStat> rowCounts = new ArrayList<RowCountStat>();
        RowCountStat rowCountStat =
            new RowCountStat(RowCountStatType.ROW_COUNT, rowCount);
        rowCounts.add(rowCountStat);
        rowCountStat =
            new RowCountStat(
                RowCountStatType.DELETED_ROW_COUNT,
                deletedRowCount);
        rowCounts.add(rowCountStat);
        updateRowCounts(table, rowCounts, repos);
    }

    /**
     * Updates various row counts for an abstract column set, creating new row
     * count stat records as needed.
     *
     * @param table the abstract column set
     * @param rowCounts list of row counts to be updated
     * @param repos repository
     */
    public static void updateRowCounts(
        FemAbstractColumnSet table,
        List<RowCountStat> rowCounts,
        FarragoRepos repos)
    {
        List<FemRowCountStatistics> rowCountStatsList =
            table.getRowCountStats();

        // There will be no rowCountStats record if this table was migrated
        // from an earlier version.  So, first migrate over the existing row
        // count stats stored in the table record.  Note that even in the
        // case where there are no stats yet, we'll create an initial record,
        // which will be overwritten with the new stats further below.
        if (rowCountStatsList.isEmpty()) {
            FemRowCountStatistics rowCountStats =
                repos.newFemRowCountStatistics();
            rowCountStats.setColumnSet(table);
            rowCountStats.setRowCount(table.getRowCount());
            rowCountStats.setDeletedRowCount(table.getDeletedRowCount());
            rowCountStats.setAnalyzeRowCount(table.getLastAnalyzeRowCount());
            rowCountStats.setAnalyzeTimestamp(table.getAnalyzeTime());
            // leave the dml timestamp set to null because we don't really
            // know what it is

            // add the new record to our list
            rowCountStatsList = new ArrayList<FemRowCountStatistics>();
            rowCountStatsList.add(rowCountStats);
        }

        // Determine if we need to create a new row count stats record or can
        // reuse the latest one.  We can reuse the latest if those stats were
        // created after the newest label was created.  When determining the
        // timestamp of the row count stats, take the maximum of the dml and
        // analyze timestamps.
        FemRowCountStatistics rowCountStats =
            rowCountStatsList.get(rowCountStatsList.size() - 1);
        Timestamp newestLabelTimestamp = getNewestLabelCreationTimestamp(repos);
        Timestamp maxTimestamp = getMaxTimestamp(table, rowCountStats);
        if ((newestLabelTimestamp == null)
            || (maxTimestamp == null)
            || (newestLabelTimestamp.compareTo(maxTimestamp) < 0))
        {
            setNewRowCounts(table, rowCountStats, rowCounts);
        } else {
            FemRowCountStatistics newRowCountStats =
                repos.newFemRowCountStatistics();
            newRowCountStats.setColumnSet(table);

            // initialize the record with the values from the previous
            // record
            newRowCountStats.setDmlTimestamp(rowCountStats.getDmlTimestamp());
            newRowCountStats.setRowCount(rowCountStats.getRowCount());
            newRowCountStats.setDeletedRowCount(
                rowCountStats.getDeletedRowCount());
            newRowCountStats.setAnalyzeTimestamp(
                rowCountStats.getAnalyzeTimestamp());
            newRowCountStats.setAnalyzeRowCount(
                rowCountStats.getAnalyzeRowCount());

            // now, set the new, current values
            setNewRowCounts(table, newRowCountStats, rowCounts);
        }
    }

    /**
     * Updates the row count statistic for an abstract column set
     *
     * @param columnSet the column set whose row count will be updated
     * @param rowCount number of rows returned by column set
     * @param updateRowCount if true, the {@link
     * FemAbstractColumnSet#setRowCount(Long)} property is updated.
     * @param updateAnalyzeRowCount if true, the {@link
     * FemAbstractColumnSet#setLastAnalyzeRowCount(Long)} property is updated
     *
     * @deprecated
     */
    public static void updateRowCount(
        FemAbstractColumnSet columnSet,
        Long rowCount,
        boolean updateRowCount,
        boolean updateAnalyzeRowCount)
    {
        if (updateAnalyzeRowCount) {
            columnSet.setAnalyzeTime(createTimestamp());
            columnSet.setLastAnalyzeRowCount(rowCount);
        }

        if (updateRowCount) {
            columnSet.setRowCount(rowCount);
        }
    }

    /**
     * Updates the row count statistic for an abstract column set, creating new
     * row count stat records, as needed.
     *
     * @param columnSet the column set whose row count will be updated
     * @param rowCount number of rows returned by column set
     * @param updateRowCount if true, the current row count is updated
     * @param updateAnalyzeRowCount if true, the analyze row count is updated
     */
    public static void updateRowCount(
        FemAbstractColumnSet columnSet,
        Long rowCount,
        boolean updateRowCount,
        boolean updateAnalyzeRowCount,
        FarragoRepos repos)
    {
        List<RowCountStat> rowCounts = new ArrayList<RowCountStat>();
        RowCountStat rowCountStat;

        if (updateAnalyzeRowCount) {
            rowCountStat =
                new RowCountStat(
                    RowCountStatType.ANALYZE_ROW_COUNT,
                    rowCount);
            rowCounts.add(rowCountStat);
        }

        if (updateRowCount) {
            rowCountStat =
                new RowCountStat(RowCountStatType.ROW_COUNT, rowCount);
            rowCounts.add(rowCountStat);
        }

        updateRowCounts(columnSet, rowCounts, repos);
    }

    /**
     * Retrieves the page count statistic for a local index, taking into
     * consideration the current label setting.
     *
     * @param index the index whose page count will be retrieved
     * @param labelTimestamp creation timestamp of the label setting that
     * determines which page count to retrieve; null if there is no label
     * setting
     *
     * @return the page count for the index
     */
    public static Long getPageCount(
        FemLocalIndex index,
        Timestamp labelTimestamp)
    {
        // If there's no label setting, retrieve the latest stat.
        if (labelTimestamp == null) {
            return index.getPageCount();
        }

        // Older catalog versions only store the page count directly in
        // the index record so there won't be separate page count
        // stat records yet.
        List<FemIndexStatistics> indexStatsList = index.getIndexStats();
        if (indexStatsList.isEmpty()) {
            return index.getPageCount();
        } else {
            // Work backwards through the list until we find the first set
            // of stats that are older than the label timestamp passed
            // in.
            for (int i = indexStatsList.size() - 1; i >= 0; i--) {
                FemIndexStatistics stats = indexStatsList.get(i);
                Timestamp statTime = Timestamp.valueOf(stats.getAnalyzeTime());
                if (statTime.compareTo(labelTimestamp) < 0) {
                    return stats.getPageCount();
                }
            }
        }

        // The current set of stats were all generated after the label setting,
        // so no index page count is available.
        return null;
    }

    /**
     * Updates the page count statistic for a local index in the index record.
     *
     * @param index the index whose page count will be updated
     * @param pageCount number of pages on disk used by index
     *
     * @deprecated
     */
    public static void updatePageCount(
        FemLocalIndex index,
        Long pageCount)
    {
        index.setAnalyzeTime(createTimestamp());
        index.setPageCount(pageCount);
    }

    /**
     * Updates the page count statistic for a local index, creating new index
     * stat records as needed.
     *
     * @param index the index whose page count will be updated
     * @param pageCount number of pages on disk used by index
     * @param repos repository
     */
    public static void updatePageCount(
        FemLocalIndex index,
        Long pageCount,
        FarragoRepos repos)
    {
        String currTimestamp = createTimestamp();
        List<FemIndexStatistics> indexStatsList = index.getIndexStats();

        // There will be no indexStats record if this index was migrated
        // from an earlier version.  So, first migrate over the existing page
        // count stat stored in the index record.  In the case where there's
        // no stat yet, we'll just create the initial record.
        boolean noExistingStat = false;
        if (indexStatsList.isEmpty()) {
            FemIndexStatistics indexStats = repos.newFemIndexStatistics();
            indexStats.setLocalIndex(index);
            if (index.getAnalyzeTime() == null) {
                noExistingStat = true;
            } else {
                indexStats.setPageCount(index.getPageCount());
                indexStats.setAnalyzeTime(index.getAnalyzeTime());
            }

            // Add the new record to the list of stats
            indexStatsList = new ArrayList<FemIndexStatistics>();
            indexStatsList.add(indexStats);
        }

        // Determine whether to update the latest record or create a new one.
        FemIndexStatistics indexStats =
            indexStatsList.get(indexStatsList.size() - 1);
        Timestamp newestLabelTimestamp = getNewestLabelCreationTimestamp(repos);
        if ((newestLabelTimestamp == null)
            || noExistingStat
            || (newestLabelTimestamp.compareTo(
                    Timestamp.valueOf(
                        indexStats.getAnalyzeTime())) < 0))
        {
            indexStats.setPageCount(pageCount);
            indexStats.setAnalyzeTime(currTimestamp);
        } else {
            indexStats = repos.newFemIndexStatistics();
            indexStats.setLocalIndex(index);
            indexStats.setPageCount(pageCount);
            indexStats.setAnalyzeTime(currTimestamp);
        }

        // Set the latest stats in the index record.
        index.setAnalyzeTime(currTimestamp);
        index.setPageCount(pageCount);
    }

    /**
     * Retrieves the histogram for a column based on a label setting.
     *
     * @param column the column
     * @param labelTimestamp the creation timestamp of the label setting; null
     * if there is no label setting
     *
     * @return the corresponding histogram, if it exists
     */
    public static FemColumnHistogram getHistogram(
        FemAbstractColumn column,
        Timestamp labelTimestamp)
    {
        List<FemColumnHistogram> histogramList = column.getHistogram();
        int listSize = histogramList.size();

        // If there's no label setting, just return the latest histogram
        if ((labelTimestamp == null) && (listSize > 0)) {
            return histogramList.get(listSize - 1);
        }

        // Work backwards through the list until we find the first
        // histogram older than the label timestamp passed in.
        for (int i = listSize - 1; i >= 0; i--) {
            FemColumnHistogram histogram = histogramList.get(i);
            Timestamp statTime = Timestamp.valueOf(histogram.getAnalyzeTime());
            if (statTime.compareTo(labelTimestamp) < 0) {
                return histogram;
            }
        }

        // All histograms were created after the label timestamp passed in,
        // which means this column had no histograms at the time of the label
        // setting.
        return null;
    }

    public static void updateHistogram(
        FarragoRepos repos,
        FemAbstractColumn column,
        Long distinctValues,
        boolean distinctValuesEstimated,
        float samplePercent,
        long sampleSize,
        int barCount,
        long rowsPerBar,
        long rowsLastBar,
        List<FemColumnHistogramBar> bars)
    {
        FemColumnHistogram histogram =
            getHistogramForUpdate(repos, column, true);

        histogram.setAnalyzeTime(createTimestamp());
        histogram.setDistinctValueCount(distinctValues);
        histogram.setDistinctValueCountEstimated(distinctValuesEstimated);
        histogram.setPercentageSampled(samplePercent);
        histogram.setSampleSize(sampleSize);
        histogram.setBarCount(barCount);

        // TODO: make row count an attribute of bars
        histogram.setRowsPerBar(rowsPerBar);
        histogram.setRowsLastBar(rowsLastBar);

        // only delete the original bars that are in excess of the
        // new number of bars since we're reusing the original bars
        List<FemColumnHistogramBar> oldBars = histogram.getBar();
        int oldBarsCount = oldBars.size();
        if (oldBarsCount > barCount) {
            Iterator<FemColumnHistogramBar> iter =
                oldBars.listIterator(barCount);
            while (iter.hasNext()) {
                FemColumnHistogramBar bar = iter.next();
                iter.remove();
                bar.refDelete();
            }
        }

        // only update the bars corresponding to new bars; reused bars are
        // already associated with the histogram
        if (barCount > oldBarsCount) {
            int ordinal = oldBarsCount;
            Iterator<FemColumnHistogramBar> iter = bars.listIterator(ordinal);
            while (iter.hasNext()) {
                FemColumnHistogramBar bar = iter.next();
                bar.setHistogram(histogram);
                bar.setOrdinal(ordinal++);
            }
        }
    }

    /**
     * Determines which histogram record should be updated. Either the latest
     * one is reused, or a new one is created, if desired.
     *
     * @param repos repository
     * @param column the column for which the histogram will be created
     * @param createNewHistogram if true and the latest record cannot be
     * updated, then create a new histogram record
     *
     * @return the histogram record to be updated or null if an existing record
     * cannot be updated and a new one was not created
     */
    public static FemColumnHistogram getHistogramForUpdate(
        FarragoRepos repos,
        FemAbstractColumn column,
        boolean createNewHistogram)
    {
        FemColumnHistogram histogram;
        List<FemColumnHistogram> histogramList = column.getHistogram();

        // If there are no histogram records yet, create one.  Otherwise,
        // determine whether the newest histogram was created before or
        // after the newest label.  If it was created before, then create a
        // new record; otherwise, reuse the newest one.
        if (histogramList.isEmpty()) {
            if (createNewHistogram) {
                histogram = repos.newFemColumnHistogram();
                histogram.setColumn(column);
            } else {
                histogram = null;
            }
        } else {
            histogram = histogramList.get(histogramList.size() - 1);
            Timestamp newestLabelTimestamp =
                getNewestLabelCreationTimestamp(repos);
            if ((newestLabelTimestamp != null)
                && (newestLabelTimestamp.compareTo(
                        Timestamp.valueOf(histogram.getAnalyzeTime())) > 0))
            {
                if (createNewHistogram) {
                    histogram = repos.newFemColumnHistogram();
                    histogram.setColumn(column);
                } else {
                    histogram = null;
                }
            }
        }
        return histogram;
    }

    /**
     * Updates system-maintained attributes of an object.
     *
     * @param annotatedElement object to update
     * @param timestamp timestamp to use for creation/modification
     * @param isNew whether object is being created
     */
    public static void updateAnnotatedElement(
        FemAnnotatedElement annotatedElement,
        String timestamp,
        boolean isNew)
    {
        annotatedElement.setModificationTimestamp(timestamp);
        if (isNew) {
            annotatedElement.setCreationTimestamp(timestamp);
            annotatedElement.setLineageId(UUID.randomUUID().toString());
        }
    }

    /**
     * Creates a timestamp reflecting the current time
     *
     * @return the timestamp encoded as a string
     */
    public static String createTimestamp()
    {
        return new Timestamp(System.currentTimeMillis()).toString();
    }

    /**
     * Creates a new recovery reference for a recoverable action on an object.
     *
     * @param repos repository in which object is defined
     * @param recoveryType description of recoverable action
     * @param modelElement object on which recovery would be needed
     */
    public static FemRecoveryReference createRecoveryReference(
        FarragoRepos repos,
        RecoveryType recoveryType,
        CwmModelElement modelElement)
    {
        FemRecoveryReference ref = repos.newFemRecoveryReference();
        ref.setRecoveryType(recoveryType);
        CwmDependency dep = repos.newCwmDependency();
        dep.setName(recoveryType.toString() + " " + modelElement.getName());
        dep.setNamespace(ref);
        dep.setKind("Recovery");
        dep.getClient().add(ref);
        dep.getSupplier().add(modelElement);

        // These are typically created outside of DdlValidator,
        // so fill in standard ModelElement attributes.
        ref.setVisibility(VisibilityKindEnum.VK_PUBLIC);
        dep.setVisibility(VisibilityKindEnum.VK_PUBLIC);
        JmiObjUtil.setMandatoryPrimitiveDefaults(ref);
        JmiObjUtil.setMandatoryPrimitiveDefaults(dep);
        return ref;
    }

    /**
     * Resets the row counts for a table
     *
     * @param table a column set table
     *
     * @deprecated
     */
    public static void resetRowCounts(FemAbstractColumnSet table)
    {
        long zero = 0;
        table.setRowCount(zero);
        table.setDeletedRowCount(zero);
    }

    /**
     * Resets the row counts for a table, creating new row count stat records,
     * as needed.
     *
     * @param table a column set table
     * @param repos repository
     */
    public static void resetRowCounts(
        FemAbstractColumnSet table,
        FarragoRepos repos)
    {
        updateRowCounts(table, 0, 0, repos);
    }

    /**
     * Sets various row counts for an abstract column set. The counts are
     * reflected both in the abstract column set record as well as the row count
     * statistics record.
     *
     * @param table the abstract column set
     * @param rowCountStats the row count statistics
     * @param rowCounts the row counts to be set
     */
    private static void setNewRowCounts(
        FemAbstractColumnSet table,
        FemRowCountStatistics rowCountStats,
        List<RowCountStat> rowCounts)
    {
        String currTimestamp = createTimestamp();
        boolean rowCountSet = false;
        boolean analyzeCountSet = false;

        for (RowCountStat rowCount : rowCounts) {
            if (rowCount.type == RowCountStatType.ROW_COUNT) {
                table.setRowCount(rowCount.count);
                rowCountStats.setRowCount(rowCount.count);
                rowCountSet = true;
            } else if (rowCount.type == RowCountStatType.DELETED_ROW_COUNT) {
                table.setDeletedRowCount(rowCount.count);
                rowCountStats.setDeletedRowCount(rowCount.count);
            } else {
                assert (rowCount.type == RowCountStatType.ANALYZE_ROW_COUNT);
                table.setLastAnalyzeRowCount(rowCount.count);
                table.setAnalyzeTime(currTimestamp);
                rowCountStats.setAnalyzeTimestamp(currTimestamp);
                rowCountStats.setAnalyzeRowCount(rowCount.count);
                analyzeCountSet = true;
            }
        }

        // If only the row count was set and not the analyze count, then
        // this is a dml update, so update the dml timestamp.
        if (rowCountSet && !analyzeCountSet) {
            rowCountStats.setDmlTimestamp(currTimestamp);
        }
    }

    /**
     * Retrieves the creation timestamp of the most recently created label
     * stored in the catalog.
     *
     * @param repos repository
     *
     * @return creation timestamp of the newest label
     */
    public static Timestamp getNewestLabelCreationTimestamp(FarragoRepos repos)
    {
        Collection<FemLabel> labels = repos.allOfType(FemLabel.class);
        Timestamp newestTimestamp = null;
        for (FemLabel label : labels) {
            Timestamp timestamp =
                Timestamp.valueOf(label.getCreationTimestamp());
            if ((newestTimestamp == null)
                || (timestamp.compareTo(newestTimestamp) > 0))
            {
                newestTimestamp = timestamp;
            }
        }
        return newestTimestamp;
    }

    /**
     * Retrieves the csn of the oldest label stored in the catalog.
     *
     * @param repos repository
     *
     * @return csn of the oldest label; null if there are no labels
     */
    public static Long getOldestLabelCsn(FarragoRepos repos)
    {
        Collection<FemLabel> labels = repos.allOfType(FemLabel.class);
        Long oldestCsn = null;
        for (FemLabel label : labels) {
            // Ignore label aliases
            if (label.getParentLabel() != null) {
                continue;
            }
            long csn = label.getCommitSequenceNumber();
            if ((oldestCsn == null) || (csn < oldestCsn)) {
                oldestCsn = csn;
            }
        }
        return oldestCsn;
    }

    /**
     * Retrieves the creation timestamp of the labels that bound a specified
     * label.
     *
     * @param referenceLabel the label that will be used to determine the
     * boundaries
     * @param repos repository
     *
     * @return returns the lower and upper bound timestamps; if the specified
     * label is the oldest, then the lowerBound is set to null; if the specified
     * label is the newest, then the upperBound is set to null; therefore, if
     * the specified label is the only label, then both bounds are set to null
     */
    private static List<Timestamp> getLabelBounds(
        FemLabel referenceLabel,
        FarragoRepos repos)
    {
        Collection<FemLabel> labels = repos.allOfType(FemLabel.class);
        Timestamp lowerBound = null;
        Timestamp upperBound = null;
        Timestamp referenceTimestamp =
            Timestamp.valueOf(referenceLabel.getCreationTimestamp());
        for (FemLabel label : labels) {
            // Ignore label aliases since they are "pointers" to base
            // labels.  So, only the timestamps of base labels are
            // meaningful in determining which label is the oldest.
            if (label.getParentLabel() != null) {
                continue;
            }
            String timestamp = label.getCreationTimestamp();

            // Ignore new labels that haven't been created yet
            if (timestamp == null) {
                continue;
            }
            Timestamp labelTimestamp = Timestamp.valueOf(timestamp);
            int rc = referenceTimestamp.compareTo(labelTimestamp);

            // Find the newest label older than the reference label
            if ((rc > 0)
                && ((lowerBound == null)
                    || (labelTimestamp.compareTo(lowerBound) > 0)))
            {
                lowerBound = labelTimestamp;

                // Find the oldest label newer than the reference label
            } else if (
                (rc < 0)
                && ((upperBound == null)
                    || (labelTimestamp.compareTo(upperBound) < 0)))
            {
                upperBound = labelTimestamp;
            }
        }

        List<Timestamp> ret = new ArrayList<Timestamp>();
        ret.add(lowerBound);
        ret.add(upperBound);
        return ret;
    }

    /*
     * Removes from the various catalog tables containing data statistics the
     * set of stats associated with a label.
     *
     * @param label the label
     * @param repos repository
     * @param usePreviewRefDelete whether to use the repository's preview
     * refDelete feature or simply delete the objects
     */
    public static void removeObsoleteStatistics(
        FemLabel label,
        FarragoRepos repos,
        boolean usePreviewRefDelete)
    {
        // Locate the stats associated with the label by determining the
        // timestamps of the two labels that bound the specified label.
        // Stats that fall within the timestamp range are candidates for
        // removal.
        List<Timestamp> bounds =
            FarragoCatalogUtil.getLabelBounds(label, repos);
        Timestamp lowerBound = bounds.get(0);
        Timestamp upperBound = bounds.get(1);
        boolean onlyLabel = false;

        // If there are no bounds on both ends, then this is the only
        // label.  In that case, the candidates for removal are all stats
        // older the label.  So, set the upper bound to the label's
        // timestamp.
        if ((lowerBound == null) && (upperBound == null)) {
            upperBound = Timestamp.valueOf(label.getCreationTimestamp());
            onlyLabel = true;
        }

        try {
            // Start with RowCountStatistics
            removeObsoleteStatisticsFromTable(
                repos,
                repos.allOfType(FemAbstractColumnSet.class),
                FemAbstractColumnSet.class.getMethod("getRowCountStats"),
                null,
                lowerBound,
                upperBound,
                onlyLabel,
                usePreviewRefDelete);

            // Move on to ColumnHistogram
            removeObsoleteStatisticsFromTable(
                repos,
                repos.allOfType(FemAbstractColumn.class),
                FemAbstractColumn.class.getMethod("getHistogram"),
                FemColumnHistogram.class.getMethod("getAnalyzeTime"),
                lowerBound,
                upperBound,
                onlyLabel,
                usePreviewRefDelete);

            // Finally, IndexStatistics
            removeObsoleteStatisticsFromTable(
                repos,
                repos.allOfType(FemLocalIndex.class),
                FemLocalIndex.class.getMethod("getIndexStats"),
                FemIndexStatistics.class.getMethod("getAnalyzeTime"),
                lowerBound,
                upperBound,
                onlyLabel,
                usePreviewRefDelete);
        } catch (Exception e) {
            throw Util.newInternal(e);
        }
    }

    /**
     * Removes statistics in between 2 timestamp boundaries, provided the stat
     * is not the only remaining statistic record within the timestamp range.
     * The candidate stats are located by walking through a list of parent
     * objects that reference stats.
     *
     * @param <ParentType> the type of the object that references the stat
     * records
     * @param repos repository
     * @param parentList list of parent objects
     * @param statsGetter method that retrieves the list of stat records from
     * each parent object
     * @param timestampGetter method that retrieves the timestamp from each
     * stats record; null if this is a RowCountStatistics object; row count
     * stats are handled as a special case
     * @param lowerBound the lower bound timestamp boundary
     * @param upperBound the upper bound timestamp boundary
     * @param onlyLabel true if this is the special case where the label being
     * dropped is the only remaining one; in this case, the lowerBound should be
     * null
     * @param usePreviewRefDelete whether to use the repository's preview
     * refDelete feature or just delete the objects
     */
    private static <ParentType extends CwmModelElement> void
    removeObsoleteStatisticsFromTable(
        FarragoRepos repos,
        Collection<ParentType> parentList,
        Method statsGetter,
        Method timestampGetter,
        Timestamp lowerBound,
        Timestamp upperBound,
        boolean onlyLabel,
        boolean usePreviewRefDelete)
        throws Exception
    {
        EnkiMDRepository mdrRepos = repos.getEnkiMdrRepos();

        for (ParentType parent : parentList) {
            // We make a copy of the actual list to avoid modifying the
            // repository when this method is used as part of previewRefDelete
            List<RefObject> statsList =
                new ArrayList<RefObject>(
                    (List<RefObject>) statsGetter.invoke(parent));

            // Determine the indices of the stats that are within the bounds
            int lowerIdx = -1;
            int upperIdx = -1;
            int i = 0;
            Iterator<RefObject> iter = statsList.iterator();
            while (iter.hasNext()) {
                RefObject stats = (RefObject) iter.next();
                Timestamp statsTimestamp;
                if (timestampGetter != null) {
                    statsTimestamp =
                        Timestamp.valueOf(
                            (String) timestampGetter.invoke(
                                stats));
                } else {
                    assert (stats instanceof FemRowCountStatistics);
                    statsTimestamp =
                        getMaxTimestamp(
                            (FemAbstractColumnSet) parent,
                            (FemRowCountStatistics) stats);
                }
                if (!timestampInRange(lowerBound, upperBound, statsTimestamp)) {
                    // Stats are ordered so if we're out of range and have
                    // already found an entry matching the lower bound,
                    // then we've exceeded the upper bound.
                    if (lowerIdx >= 0) {
                        break;
                    }
                } else {
                    if (lowerIdx < 0) {
                        lowerIdx = i;
                        upperIdx = i;
                    } else {
                        upperIdx++;
                    }
                }
                i++;
            }

            // We can only remove stats within the range if there 2 of them,
            // since we can't remove the newest one in the range.  Note that
            // because stats are removed as labels are dropped/replaced,
            // and there should never be more than one set of stats associated
            // with each label, the located range should never consist of more
            // than 2 sets of stats.
            if (lowerIdx >= 0) {
                assert ((upperIdx - lowerIdx) <= 1);
                if (onlyLabel) {
                    assert (lowerBound == null);
                    if (statsList.size() >= 2) {
                        // For the special case where we're dropping the only
                        // label, there is a stat associated with that
                        // label that can be dropped, and there is at least
                        // one other stat outside the range, then we should
                        // still be able to drop the one stat that's
                        // in the range.  However, based on the value of
                        // upperIdx, we don't meet the criteria of having 2
                        // stats within the range.  So, bump up upperIdx so we
                        // can force the one stat to be dropped.
                        assert (lowerIdx == upperIdx);
                        upperIdx++;
                    }
                }
                if ((upperIdx - lowerIdx) > 0) {
                    ListIterator<RefObject> listIter =
                        statsList.listIterator(lowerIdx);
                    i = lowerIdx;

                    // Remove all but the newest stat in the range
                    while ((i < upperIdx) && listIter.hasNext()) {
                        RefObject stats = listIter.next();
                        listIter.remove();
                        if (usePreviewRefDelete) {
                            mdrRepos.previewRefDelete(stats);
                        } else {
                            stats.refDelete();
                        }
                        i++;
                    }
                }
            }
        }
    }

    /**
     * Determines if a specified timestamp is within 2 bounds. The bounds are
     * non-inclusive.
     *
     * @param lowerBound the lower bound; if null, then there is no lower bound
     * @param upperBound the upper bound; if null, then there is no upper bound
     * @param timestamp the timestamp; if null, the timestamp represents a very
     * old timestamp and therefore only qualifies if there is no explicit lower
     * bound
     *
     * @return true if the timestamp is within bounds
     */
    private static boolean timestampInRange(
        Timestamp lowerBound,
        Timestamp upperBound,
        Timestamp timestamp)
    {
        if (timestamp == null) {
            if (lowerBound == null) {
                return true;
            } else {
                return false;
            }
        }
        if ((lowerBound == null) || (timestamp.compareTo(lowerBound) > 0)) {
            if ((upperBound == null) || (timestamp.compareTo(upperBound) < 0)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves current backup data stored in the catalog. Information on the
     * full backup, if it exists, is always returned first, followed by
     * information on the last backup.
     *
     * @param repos repository
     *
     * @return list containing current backup data
     */
    public static List<BackupData> getCurrentBackupData(FarragoRepos repos)
    {
        List<BackupData> retList = new ArrayList<BackupData>();
        Collection<FemSystemBackup> backups =
            repos.allOfType(FemSystemBackup.class);
        assert ((backups.size() == 0) || (backups.size() == 2));

        for (FemSystemBackup backup : backups) {
            assert (backup.getStatus() == BackupStatusTypeEnum.COMPLETED);
            BackupData backupData =
                new BackupData(
                    backup.getType(),
                    backup.getCommitSequenceNumber(),
                    backup.getStartTimestamp());
            if (backup.getType() == BackupTypeEnum.FULL) {
                retList.add(0, backupData);
            } else {
                retList.add(backupData);
            }
        }

        return retList;
    }

    /**
     * Adds new records to the system backup catalog corresponding to a pending
     * backup.
     *
     * @param repos repository
     * @param type type of backup
     * @param csn commit sequence number corresponding to the backup
     * @param startTime start time of the backup
     */
    public static void addPendingSystemBackup(
        FarragoRepos repos,
        String type,
        Long csn,
        String startTime)
    {
        for (int i = 0; i < 2; i++) {
            FemSystemBackup backup = repos.newFemSystemBackup();
            if (i == 0) {
                backup.setType(BackupTypeEnum.LAST);
            } else {
                backup.setType(BackupTypeEnum.FULL);
            }
            backup.setCommitSequenceNumber(csn);
            backup.setStartTimestamp(startTime);
            backup.setStatus(BackupStatusTypeEnum.PENDING);
            if (!type.equals("FULL")) {
                break;
            }
        }
    }

    /**
     * Updates the system backup catalog data depending on whether the last
     * pending backup (if any) succeeded or failed.  However, if the data
     * indicates that a partial restore has been done, then nothing is updated.
     *
     * @param repos repository
     * @param backupSucceeded true if the last backup succeeded
     * @param setEndTimestamp if true, record the current timestamp as the
     * ending timestamp when updating pending data to completed
     *
     * @return true if a partial restore was done
     */
    public static boolean updatePendingBackupData(
        FarragoRepos repos,
        boolean backupSucceeded,
        boolean setEndTimestamp)
    {
        Collection<FemSystemBackup> backups =
            repos.allOfType(FemSystemBackup.class);

        // First see which pending records exist and whether a partial restore
        // was done.
        boolean pendingFull = false;
        boolean pendingLast = false;
        for (FemSystemBackup backup : backups) {
            if (backup.getStatus() == BackupStatusTypeEnum.COMPLETED &&
                backup.getStartTimestamp() == null)
            {
                return true;
            }
            if (backup.getStatus() == BackupStatusTypeEnum.PENDING) {
                if (backup.getType() == BackupTypeEnum.LAST) {
                    pendingLast = true;
                } else {
                    assert (backup.getType() == BackupTypeEnum.FULL);
                    pendingFull = true;
                }
            }
        }

        // If no pending records, there's no work to do
        if (!pendingFull && !pendingLast) {
            return false;
        }
        if (pendingFull) {
            assert (pendingLast);
        }

        // If the last backup succeeded, delete the completed records
        // corresponding to the pending records, then update the pending
        // records to completed.  Otherwise, just delete the pending records.
        if (backupSucceeded) {
            for (FemSystemBackup backup : backups) {
                if ((backup.getStatus() == BackupStatusTypeEnum.COMPLETED)
                    && ((pendingFull
                            && (backup.getType() == BackupTypeEnum.FULL))
                        || (pendingLast
                            && (backup.getType() == BackupTypeEnum.LAST))))
                {
                    backup.refDelete();
                }
            }
        }
        backups = repos.allOfType(FemSystemBackup.class);
        for (FemSystemBackup backup : backups) {
            if (backup.getStatus() == BackupStatusTypeEnum.PENDING) {
                if (backupSucceeded) {
                    backup.setStatus(BackupStatusTypeEnum.COMPLETED);
                    if (setEndTimestamp) {
                        backup.setEndTimestamp(createTimestamp().toString());
                    }
                } else {
                    backup.refDelete();
                }
            }
        }

        return false;
    }

    /**
     * Adds or updates records in the system backup catalog, indicating that a
     * partial restore has been completed.
     *
     * @param repos repository
     */
    public static void addPendingRestore(FarragoRepos repos)
    {
        Collection<FemSystemBackup> backups =
            repos.allOfType(FemSystemBackup.class);

        // See if there already are backup records.  If there are, null
        // out the starting timestamp.  That's the indicator that only a
        // partial restore has been completed.
        int updateCount = 0;
        for (FemSystemBackup backup : backups) {
            assert (backup.getStatus() == BackupStatusTypeEnum.COMPLETED);
            backup.setStartTimestamp(null);
            backup.setEndTimestamp(null);
            updateCount++;
        }
        assert (updateCount == 0 || updateCount == 2);
        if (updateCount > 0) {
            return;
        }

        // If not, add new records.
        addNewPendingRestoreRecord(repos, BackupTypeEnum.LAST);
        addNewPendingRestoreRecord(repos, BackupTypeEnum.FULL);
    }

    private static void addNewPendingRestoreRecord(
        FarragoRepos repos,
        BackupTypeEnum type)
    {
        FemSystemBackup backup = repos.newFemSystemBackup();
        backup.setStartTimestamp(null);
        backup.setEndTimestamp(null);
        backup.setType(type);
        backup.setStatus(BackupStatusTypeEnum.COMPLETED);
    }

    /**
     * Extracts the storage options for an element into a Properties. No
     * duplicate property names are allowed. Inline property references (such as
     * <code>${varname}</code> get expanded on read.
     *
     * @param repos repository reference
     *
     * @param element FemElement we want the options from
     *
     * @return Properties object populated with the storage options
     */
    public static Properties getStorageOptionsAsProperties(
        FarragoRepos repos,
        FemElementWithStorageOptions element)
    {
        Properties props = new Properties();

        String optName, optValue;
        for (FemStorageOption option : element.getStorageOptions()) {
            optName = option.getName();
            assert (!props.containsKey(optName));
            optValue = repos.expandProperties(option.getValue());
            props.setProperty(
                optName,
                optValue);
        }
        return props;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Helper class used to represent a specific type of row count statistic
     */
    private static class RowCountStat
    {
        RowCountStatType type;
        long count;

        RowCountStat(RowCountStatType type, long count)
        {
            this.type = type;
            this.count = count;
        }
    }

    /**
     * Helper class used to represent backup information stored in the backup
     * catalog.
     */
    public static class BackupData
    {
        public BackupType type;
        public long csn;
        public String startTimestamp;

        BackupData(BackupType type, long csn, String startTimestamp)
        {
            this.type = type;
            this.csn = csn;
            this.startTimestamp = startTimestamp;
        }
    }
}

// End FarragoCatalogUtil.java
