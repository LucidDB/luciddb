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

import net.sf.farrago.*;
import net.sf.farrago.cwm.*;
import net.sf.farrago.cwm.CwmPackage;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import org.netbeans.api.mdr.*;

import java.io.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

/**
 * FarragoCatalog represents a loaded instance of an MDR repository containing
 * Farrago metadata.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoCatalog
    extends FarragoMetadataFactory implements FarragoAllocation
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoCatalog.class);

    /** TODO:  look this up from repository */
    private static final int maxNameLength = 128;

    /**
     * Reserved name for the system boot catalog.
     */
    public static final String SYSBOOT_CATALOG_NAME = "SYS_BOOT";
    
    /**
     * Reserved name for the local catalog.
     */
    public static final String LOCALDB_CATALOG_NAME = "LOCALDB";
    
    //~ Instance fields -------------------------------------------------------

    /** Farrago config package in repository. */
    public final ConfigPackage configPackage;

    /** CWM Core package in repository. */
    public final CorePackage corePackage;

    /** CWM package in repository. */
    public final CwmPackage cwmPackage;

    /** CWM DataTypes package in repository. */
    public final DataTypesPackage datatypesPackage;

    /** Root package in repository. */
    public final FarragoPackage farragoPackage;

    /** FEM package in repository. */
    public final FemPackage femPackage;

    /** Farrago config package in repository. */
    public final FennelPackage fennelPackage;

    /**
     * FEM SQL/MED package in repository.
     */
    public final MedPackage medPackage;

    /** CWM KeysIndexes package in repository. */
    public final KeysIndexesPackage indexPackage;

    /** CWM Relational package in repository. */
    public final RelationalPackage relationalPackage;

    /** The loader for the underlying MDR repository. */
    private FarragoModelLoader modelLoader;

    /** The underlying MDR repository. */
    private final MDRepository mdrRepository;

    /** Immutable parameters read on startup */
    private FemFarragoConfig immutableConfig;

    /** MofId for current instance of FemFarragoConfig. */
    private final String currentConfigMofId;

    //~ Constructors ----------------------------------------------------------

    /**
     * Open a Farrago catalog.
     */
    public FarragoCatalog(FarragoAllocationOwner owner,boolean userCatalog)
    {
        owner.addAllocation(this);
        tracer.fine("Loading catalog");
        if (System.getProperties().getProperty(
                FarragoModelLoader.HOME_PROPERTY) == null)
        {
            throw FarragoResource.instance().newMissingHomeProperty(
                FarragoModelLoader.HOME_PROPERTY);
        }
        modelLoader = new FarragoModelLoader();

        if (!userCatalog) {
            File catalogFile = modelLoader.getSystemCatalogFile();
            try {
                new FarragoFileLockAllocation(
                    owner,
                    catalogFile,
                    true);
            } catch (IOException ex) {
                throw FarragoResource.instance().newCatalogFileLockFailed(
                    catalogFile.toString(),ex);
            }
        }
        
        farragoPackage = modelLoader.loadModel("FarragoCatalog",userCatalog);
        if (farragoPackage == null) {
            throw FarragoResource.instance().newCatalogUninitialized();
        }
        super.setRootPackage(farragoPackage);
        mdrRepository = modelLoader.getRepository();
        cwmPackage = farragoPackage.getCwm();
        corePackage = cwmPackage.getCore();
        relationalPackage = cwmPackage.getRelational();
        indexPackage = cwmPackage.getKeysIndexes();
        datatypesPackage = cwmPackage.getDataTypes();
        femPackage = farragoPackage.getFem();
        configPackage = femPackage.getConfig();
        fennelPackage = femPackage.getFennel();
        medPackage = femPackage.getMed();

        Collection configs =
            configPackage.getFemFarragoConfig().refAllOfClass();

        // TODO: multiple named configurations.  For now, build should have
        // imported exactly one configuration named Current.
        assert (configs.size() == 1);
        immutableConfig =
            (FemFarragoConfig) configs.iterator().next();
        assert (immutableConfig.getName().equals("Current"));
        currentConfigMofId = immutableConfig.refMofId();

        tracer.info("Catalog successfully loaded");
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return MDRepository storing this catalog
     */
    public MDRepository getRepository()
    {
        return mdrRepository;
    }

    /**
     * .
     *
     * @return CwmCatalog representing this FarragoCatalog
     */
    public CwmCatalog getSelfAsCwmCatalog()
    {
        // TODO:  variable
        return getCwmCatalog(LOCALDB_CATALOG_NAME);
    }

    /**
     * .
     *
     * @return maximum identifier length in characters
     */
    public int getIdentifierPrecision()
    {
        return maxNameLength;
    }

    public FemFarragoConfig getCurrentConfig()
    {
        // TODO:  prevent updates
        return (FemFarragoConfig)
            mdrRepository.getByMofId(currentConfigMofId);
    }
    
    /**
     * Determine whether an index is clustered.
     *
     * @param index the index in question
     *
     * @return true if clustered
     */
    public boolean isClustered(CwmSqlindex index)
    {
        return getTag(index,"clusteredIndex") != null;
    }

    /**
     * Find the clustered index storing a table's data.
     *
     * @param table the table to access
     *
     * @return clustered index or null if none
     */
    public CwmSqlindex getClusteredIndex(CwmClass table)
    {
        Iterator iter = getIndexes(table).iterator();
        while (iter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) iter.next();
            if (isClustered(index)) {
                return index;
            }
        }
        return null;
    }

    /**
     * .
     *
     * @return the name of the default Charset for this catalog
     */
    public String getDefaultCharsetName()
    {
        // REVIEW:  should this be configurable and/or based on default locale?
        return "ISO-8859-1";
    }

    /**
     * .
     *
     * @return the name of the default Collation for this catalog
     */
    public String getDefaultCollationName()
    {
        // REVIEW:  should this be configurable and/or based on default locale?
        return "iso-8859-1$en_US";
    }

    /**
     * .
     *
     * @return true iff Fennel support should be used
     */
    public boolean isFennelEnabled()
    {
        return !immutableConfig.isFennelDisabled();
    }

    /**
     * Get the collection of indexes spanning a table.
     *
     * @param table the table of interest
     *
     * @return index collection
     */
    public Collection getIndexes(CwmClass table)
    {
        return indexPackage.getIndexSpansClass().getIndex(table);
    }

    /**
     * Format the fully-qualified localized name for an existing object.
     *
     * @param modelElement catalog object
     * @param refClass if non-null, use this as the type of the object, e.g.
     *        "table SCHEMA.TABLE"; if null, don't include type (e.g. just
     *        "SCHEMA.TABLE")
     *
     * @return localized name
     */
    public String getLocalizedObjectName(
        CwmModelElement modelElement,
        RefClass refClass)
    {
        String qualifierName = null;
        CwmNamespace namespace = modelElement.getNamespace();
        if (namespace != null) {
            qualifierName = namespace.getName();
        }
        return getLocalizedObjectName(
            qualifierName,
            modelElement.getName(),
            refClass);
    }

    /**
     * Format the fully-qualified localized name for an object that may not
     * exist yet.
     *
     * @param qualifierName name of containing object, or null for unqualified
     *        name
     * @param objectName name of object
     * @param refClass if non-null, the object type to use in the name; if
     *        null, no type is prepended
     *
     * @return localized name
     */
    public String getLocalizedObjectName(
        String qualifierName,
        String objectName,
        RefClass refClass)
    {
        StringBuffer sb = new StringBuffer();

        // TODO:  actual localization, quoting, etc.
        if (refClass != null) {
            sb.append(refClass.refMetaObject().refGetValue("name"));
            sb.append(" ");
        }
        if (qualifierName != null) {
            sb.append(qualifierName);
            sb.append('.');
        }
        sb.append(objectName);
        return sb.toString();
    }

    /**
     * Search a collection for a CwmModelElement by name.
     *
     * @param collection the collection to search
     * @param name name of element to find
     *
     * @return CwmModelElement found, or null if not found
     */
    public CwmModelElement getModelElement(Collection collection,String name)
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
     * Search a collection for a CwmModelElement by name and type.
     *
     * @param collection the collection to search
     * @param name name of element to find
     * @param type class which sought object must instantiate
     *
     * @return CwmModelElement found, or null if not found
     */
    public CwmModelElement getTypedModelElement(
        Collection collection,String name,Class type)
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
     * Determine whether a column may contain null values.  This must be used
     * rather than directly calling CwmColumn.getIsNullable, because a column
     * which is part of a primary key or clustered index may not contain
     * nulls even when its definition says it can.
     *
     * @param column the column of interest
     *
     * @return whether nulls are allowed
     */
    public boolean isNullable(CwmColumn column)
    {
        if (column.getIsNullable().equals(NullableTypeEnum.COLUMN_NO_NULLS)) {
            return false;
        }

        CwmClassifier owner = column.getOwner();
        if (!(owner instanceof CwmTable)) {
            return true;
        }

        CwmPrimaryKey primaryKey = getPrimaryKey(owner);
        if (primaryKey != null) {
            if (primaryKey.getFeature().contains(column)) {
                return false;
            }
        }
        CwmSqlindex clusteredIndex =
            getClusteredIndex((CwmTable) owner);
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
     * Determine whether an index implements its table's primary key.
     *
     * @param index the index in question
     *
     * @return true if is the primary key index
     */
    public boolean isPrimary(CwmSqlindex index)
    {
        return index.getName().startsWith("SYS$PRIMARY_KEY");
    }

    /**
     * Find the primary key for a table.
     *
     * @param table the table of interest
     *
     * @return the PrimaryKey constraint, or null if none is defined
     */
    public CwmPrimaryKey getPrimaryKey(CwmClassifier table)
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
     * Resolve a (possibly qualified) name of a schema object.
     *
     * @param connectionDefaults default qualifier settings
     *
     * @param names array of 1 or more name components, from
     * most general to most specific
     *
     * @return ResolvedSchemaObject, or null if object definitely doesn't
     * exist
     */
    public ResolvedSchemaObject resolveSchemaObjectName(
        FarragoConnectionDefaults connectionDefaults,
        String [] names)
    {
        ResolvedSchemaObject resolved = new ResolvedSchemaObject();
        if (names.length > 3) {
            // Max is catalog.schema.obj
            return null;
        } else if (names.length == 3) {
            resolved.catalogName = names[0];
            resolved.schemaName = names[1];
            resolved.objectName = names[2];
        } else if (names.length == 2) {
            resolved.catalogName = connectionDefaults.catalogName;
            resolved.schemaName = names[0];
            resolved.objectName = names[1];
        } else if (names.length == 1) {
            if (connectionDefaults.schemaName == null) {
                // TODO:  use names for context
                throw FarragoResource.instance().newValidatorNoDefaultSchema();
            }
            resolved.catalogName = connectionDefaults.schemaCatalogName;
            resolved.schemaName = connectionDefaults.schemaName;
            resolved.objectName = names[0];
        } else {
            throw new IllegalArgumentException();
        }

        resolved.catalog = getCwmCatalog(resolved.catalogName);
        if (resolved.catalog == null) {
            // TODO:  throw ValidatorUnknownObject for catalog
            return null;
        }

        if (resolved.catalog instanceof FemDataServer) {
            // we don't have any metadata for direct references to
            // remote objects
            return resolved;
        }

        resolved.schema = getSchema(
            resolved.catalog,resolved.schemaName);
        if (resolved.schema == null) {
            // TODO:  throw ValidatorUnknownObject for schema
            return null;
        }

        resolved.object = getModelElement(
            resolved.schema.getOwnedElement(),resolved.objectName);
        if (resolved.object == null) {
            return null;
        }

        return resolved;
    }

    /**
     * Look up a CwmCatalog by name.
     *
     * @param catalogName name of schema to find
     *
     * @return catalog definition, or null if not found
     */
    public CwmCatalog getCwmCatalog(
        String catalogName)
    {
        Collection catalogs = relationalPackage.getCwmCatalog().refAllOfType();
        return (CwmCatalog) getModelElement(catalogs,catalogName);
    }
    
    /**
     * Look up a schema by name in a catalog.
     *
     * @param cwmCatalog CwmCatalog to search
     *
     * @param schemaName name of schema to find
     *
     * @return schema definition, or null if not found
     */
    public CwmSchema getSchema(
        CwmCatalog cwmCatalog,
        String schemaName)
    {
        return (CwmSchema) getTypedModelElement(
            cwmCatalog.getOwnedElement(),
            schemaName,
            CwmSchema.class);
    }
    
    /**
     * Look up a table by name in a schema.
     *
     * @param schema the schema in which to look
     * @param tableName name of table to find
     *
     * @return table definition, or null if not found
     */
    public CwmTable getTable(CwmNamespace schema,String tableName)
    {
        return (CwmTable) getTypedModelElement(
            schema.getOwnedElement(),tableName,CwmTable.class);
    }

    /**
     * Get an element's tag.
     *
     * @param element the tagged CwmModelElement
     * @param tagName name of tag to find
     *
     * @return tag, or null if not found
     */
    public CwmTaggedValue getTag(CwmModelElement element,String tagName)
    {
        Collection tags =
            corePackage.getTaggedElement().getTaggedValue(element);
        Iterator iter = tags.iterator();
        while (iter.hasNext()) {
            CwmTaggedValue tag = (CwmTaggedValue) iter.next();
            if (tag.getTag().equals(tagName)) {
                return tag;
            }
        }
        return null;
    }

    /**
     * Tag an element.
     *
     * @param element the CwmModelElement to tag
     * @param tagName name of tag to create; if a tag with this name already
     *        exists, it will be updated
     * @param tagValue value to set
     */
    public void setTagValue(
        CwmModelElement element,
        String tagName,
        String tagValue)
    {
        CwmTaggedValue tag = getTag(element,tagName);
        if (tag == null) {
            tag = newCwmTaggedValue();
            tag.setTag(tagName);
            corePackage.getTaggedElement().add(element,tag);
        }
        tag.setValue(tagValue);
    }

    /**
     * Get a value tagged to an element.
     *
     * @param element the tagged CwmModelElement
     * @param tagName name of tag to find
     *
     * @return tag value, or null if not found
     */
    public String getTagValue(CwmModelElement element,String tagName)
    {
        CwmTaggedValue tag = getTag(element,tagName);
        if (tag == null) {
            return null;
        } else {
            return tag.getValue();
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (modelLoader == null) {
            return;
        }
        tracer.fine("Closing catalog");
        modelLoader.close();
        modelLoader = null;
        tracer.info("Catalog successfully closed");
    }

    /**
     * Set the generated name for an index used to implement a constraint.
     *
     * @param constraint the constraint being implemented
     * @param index the index implementing the constraint
     */
    public void generateConstraintIndexName(
        CwmUniqueConstraint constraint,
        CwmSqlindex index)
    {
        String name =
            "SYS$CONSTRAINT_INDEX$" + constraint.getNamespace().getName()
            + "$" + constraint.getName();
        index.setName(uniquifyGeneratedName(constraint,name));
    }

    /**
     * Set the generated name for an anonymous constraint.
     *
     * @param constraint the anonymous constraint
     */
    public void generateConstraintName(CwmUniqueConstraint constraint)
    {
        if (constraint instanceof CwmPrimaryKey) {
            constraint.setName("SYS$PRIMARY_KEY");
        } else {
            String name =
                "SYS$UNIQUE_KEY$"
                + generateUniqueConstraintColumnList(constraint);
            constraint.setName(uniquifyGeneratedName(constraint,name));
        }
    }

    private void defineTypeAlias(String aliasName,CwmSqlsimpleType type)
    {
        CwmTypeAlias typeAlias = newCwmTypeAlias();
        typeAlias.setName(aliasName);
        typeAlias.setType(type);
    }

    private String generateUniqueConstraintColumnList(
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
     * Create objects owned by the system.  This is only done once during
     * database creation.
     */
    public void createSystemObjects()
    {
        tracer.info("Creating system-owned catalog objects");
        boolean rollback = true;
        try {
            mdrRepository.beginTrans(true);
            initCatalog();
            rollback = false;
        } finally {
            mdrRepository.endTrans(rollback);
        }
        tracer.info("Creation of system-owned catalog objects committed");
    }

    private void initCatalog()
    {
        createSystemCatalogs();
        createSystemTypes();
    }

    private void createSystemCatalogs()
    {
        // TODO:  default character set and collation name
        CwmCatalog cwmCatalog;

        cwmCatalog = newCwmCatalog();
        cwmCatalog.setName(SYSBOOT_CATALOG_NAME);
        initializeCwmCatalog(cwmCatalog);

        cwmCatalog = newCwmCatalog();
        cwmCatalog.setName(LOCALDB_CATALOG_NAME);
        initializeCwmCatalog(cwmCatalog);
    }

    public void initializeCwmCatalog(CwmCatalog cwmCatalog)
    {
        cwmCatalog.setDefaultCharacterSetName(getDefaultCharsetName());
        cwmCatalog.setDefaultCollationName(getDefaultCollationName());
    }

    private void createSystemTypes()
    {
        CwmSqlsimpleType type;

        // This is where all the builtin types are defined.  To add a new
        // builtin type, you have to:
        // (1) add a definition here
        // (2) add Java mappings in FarragoTypeFactoryImpl
        // (3) add Fennel mappings in
        // FennelRelUtil.convertSqlTypeNumberToFennelTypeOrdinal
        // (4) review classes in package net.sf.farrago.type
        // (5) since I've already done all the easy cases, you'll probably
        // need lots of extra fancy semantics elsewhere

        // NOTE:  BOOLEAN is not actually working for storage yet;
        // needs special handling in ReflectUtil.getByteBufferRead/WriteMethod
        type = newCwmSqlsimpleType();
        type.setName("BOOLEAN");
        type.setTypeNumber(new Integer(Types.BOOLEAN));
        
        type = newCwmSqlsimpleType();
        type.setName("TINYINT");
        type.setTypeNumber(new Integer(Types.TINYINT));
        type.setNumericPrecision(new Integer(8));
        type.setNumericPrecisionRadix(new Integer(2));
        type.setNumericScale(new Integer(0));

        type = newCwmSqlsimpleType();
        type.setName("SMALLINT");
        type.setTypeNumber(new Integer(Types.SMALLINT));
        type.setNumericPrecision(new Integer(16));
        type.setNumericPrecisionRadix(new Integer(2));
        type.setNumericScale(new Integer(0));

        type = newCwmSqlsimpleType();
        type.setName("INTEGER");
        type.setTypeNumber(new Integer(Types.INTEGER));
        type.setNumericPrecision(new Integer(32));
        type.setNumericPrecisionRadix(new Integer(2));
        type.setNumericScale(new Integer(0));
        defineTypeAlias("INT",type);

        type = newCwmSqlsimpleType();
        type.setName("BIGINT");
        type.setTypeNumber(new Integer(Types.BIGINT));
        type.setNumericPrecision(new Integer(64));
        type.setNumericPrecisionRadix(new Integer(2));
        type.setNumericScale(new Integer(0));

        type = newCwmSqlsimpleType();
        type.setName("REAL");
        type.setTypeNumber(new Integer(Types.REAL));
        type.setNumericPrecision(new Integer(23));
        type.setNumericPrecisionRadix(new Integer(2));

        type = newCwmSqlsimpleType();
        type.setName("DOUBLE");
        type.setTypeNumber(new Integer(Types.DOUBLE));
        type.setNumericPrecision(new Integer(52));
        type.setNumericPrecisionRadix(new Integer(2));
        defineTypeAlias("DOUBLE PRECISION",type);

        type = newCwmSqlsimpleType();
        type.setName("VARCHAR");
        type.setTypeNumber(new Integer(Types.VARCHAR));
        // NOTE: this is an upper bound based on usage of 2-byte length
        // indicators in stored tuples; there are further limits based on page
        // size (imposed during table creation)
        type.setCharacterMaximumLength(new Integer(65535));
        defineTypeAlias("CHARACTER VARYING",type);
        
        type = newCwmSqlsimpleType();
        type.setName("VARBINARY");
        type.setTypeNumber(new Integer(Types.VARBINARY));
        type.setCharacterMaximumLength(new Integer(65535));

        type = newCwmSqlsimpleType();
        type.setName("CHAR");
        type.setTypeNumber(new Integer(Types.CHAR));
        type.setCharacterMaximumLength(new Integer(65535));
        defineTypeAlias("CHARACTER",type);
        
        type = newCwmSqlsimpleType();
        type.setName("BINARY");
        type.setTypeNumber(new Integer(Types.BINARY));
        type.setCharacterMaximumLength(new Integer(65535));
        
        // do we need to set date/time precision=0 explictly here?
        type = newCwmSqlsimpleType();
        type.setName("DATE");
        type.setTypeNumber(new Integer(Types.DATE));
        type.setDateTimePrecision(new Integer(0));

        type = newCwmSqlsimpleType();
        type.setName("TIME");
        type.setTypeNumber(new Integer(Types.TIME));
        type.setDateTimePrecision(new Integer(0));

        type = newCwmSqlsimpleType();
        type.setName("TIMESTAMP");
        type.setTypeNumber(new Integer(Types.TIMESTAMP));
        type.setDateTimePrecision(new Integer(0));

        type = newCwmSqlsimpleType();
        type.setName("BIT");
        type.setTypeNumber(new Integer(Types.BIT));
        type.setCharacterMaximumLength(new Integer(65535));

        type = newCwmSqlsimpleType();
        type.setName("DECIMAL");
        type.setTypeNumber(new Integer(Types.DECIMAL));
        type.setNumericPrecision(new Integer(39));
        defineTypeAlias("DEC",type);
    }


    /**
     * Generated names are normally unique by construction.  However, if they
     * exceed the name length limit, truncation could cause collisions.  In
     * that case, we use repository object ID's to distinguish them.
     *
     * @param refObj object for which to construct name
     * @param name generated name
     *
     * @return uniquified name
     */
    private String uniquifyGeneratedName(RefObject refObj,String name)
    {
        if (name.length() <= maxNameLength) {
            return name;
        }
        String mofId = refObj.refMofId();
        return name.substring(0,maxNameLength - (mofId.length() + 1)) + "_"
        + mofId;
    }

    /**
     * Reconstruct a FemTupleAccessor from an XMI string.
     *
     * @param tupleAccessorXmiString XMI string containing definition of
     *        TupleAccessor
     *
     * @return FemTupleAccessor for accessing tuples conforming to tupleDesc
     */
    public FemTupleAccessor parseTupleAccessor(
        String tupleAccessorXmiString)
    {
        Collection c =
            JmiUtil.importFromXmiString(
                farragoPackage,
                tupleAccessorXmiString);
        assert (c.size() == 1);
        FemTupleAccessor accessor = (FemTupleAccessor) c.iterator().next();
        return accessor;
    }

    /**
     * Data structure for return value of resolveSchemaObjectName().
     */
    public static class ResolvedSchemaObject 
    {
        public CwmCatalog catalog;

        public CwmSchema schema;
        
        public CwmModelElement object;

        public String catalogName;

        public String schemaName;

        public String objectName;

        public String [] getQualifiedName()
        {
            return new String [] 
                {
                    catalogName,schemaName,objectName
                };
        }
    }
}


// End FarragoCatalog.java
