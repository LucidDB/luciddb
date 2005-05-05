/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.*;
import net.sf.farrago.cwm.CwmPackage;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.SaffronProperties;
import org.eigenbase.jmi.*;

import org.netbeans.api.mdr.*;
import org.netbeans.mdr.*;

import java.util.logging.Logger;

/**
 * FarragoRepos represents a loaded instance of an MDR repository containing
 * Farrago metadata.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRepos extends FarragoMetadataFactory
    implements FarragoAllocation,
        FarragoTransientTxnContext
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = FarragoTrace.getReposTracer();

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

    /** Root package in transient repository. */
    private final FarragoPackage transientFarragoPackage;

    /** Fennel package in repository. */
    private final FennelPackage fennelPackage;

    /** The loader for the underlying MDR repository. */
    private FarragoModelLoader modelLoader;

    /** The underlying MDR repository. */
    private final MDRepository mdrRepository;

    /** MofId for current instance of FemFarragoConfig. */
    private final String currentConfigMofId;

    private final boolean isFennelEnabled;

    private final FarragoCompoundAllocation allocations;

    private Map localizedClassNames;

    private List resourceBundles;

    private String memStorageId;

    private JmiModelGraph modelGraph;

    //~ Constructors ----------------------------------------------------------

    /**
     * Opens a Farrago repository.
     */
    public FarragoRepos(
        FarragoAllocationOwner owner,
        FarragoModelLoader modelLoader,
        boolean userRepos)
    {
        owner.addAllocation(this);
        tracer.fine("Loading catalog");
        allocations = new FarragoCompoundAllocation();
        if (FarragoProperties.instance().homeDir.get() == null) {
            throw FarragoResource.instance().newMissingHomeProperty(
                FarragoProperties.instance().homeDir.getPath());
        }
        this.modelLoader = modelLoader;

        MdrUtil.integrateTracing(FarragoTrace.getMdrTracer());

        if (!userRepos) {
            File reposFile = modelLoader.getSystemReposFile();
            try {
                new FarragoFileLockAllocation(allocations, reposFile, true);
            } catch (IOException ex) {
                throw FarragoResource.instance().newCatalogFileLockFailed(
                    reposFile.toString());
            }
        }

        if (FarragoReposUtil.isReloadNeeded()) {
            try {
                FarragoReposUtil.reloadRepository();
            } catch (Exception ex) {
                throw FarragoResource.instance().newCatalogReloadFailed(ex);
            }
        }

        FarragoPackage farragoPackage =
            modelLoader.loadModel("FarragoCatalog", userRepos);
        if (farragoPackage == null) {
            throw FarragoResource.instance().newCatalogUninitialized();
        }

        super.setRootPackage(farragoPackage);

        mdrRepository = modelLoader.getMdrRepos();

        // Create special in-memory storage for transient objects
        try {
            NBMDRepositoryImpl nbRepos = (NBMDRepositoryImpl) mdrRepository;
            Map props = new HashMap();
            memStorageId =
                nbRepos.mountStorage(
                    FarragoTransientStorageFactory.class.getName(),
                    props);
            beginReposTxn(true);
            boolean rollback = true;
            try {
                RefPackage memExtent =
                    nbRepos.createExtent(
                        "TransientCatalog",
                        getFarragoPackage().refMetaObject(),
                        null,
                        memStorageId);
                transientFarragoPackage = (FarragoPackage) memExtent;
                rollback = false;
            } finally {
                endReposTxn(rollback);
            }
            FarragoTransientStorage.ignoreCommit = true;
            fennelPackage = transientFarragoPackage.getFem().getFennel();
        } catch (Throwable ex) {
            throw FarragoResource.instance().newCatalogInitTransientFailed(ex);
        }

        // Load configuration
        Collection configs =
            getConfigPackage().getFemFarragoConfig().refAllOfClass();

        // TODO: multiple named configurations.  For now, build should have
        // imported exactly one configuration named Current.
        assert (configs.size() == 1);
        FemFarragoConfig defaultConfig =
            (FemFarragoConfig) configs.iterator().next();
        assert (defaultConfig.getName().equals("Current"));
        currentConfigMofId = defaultConfig.refMofId();
        isFennelEnabled = !defaultConfig.isFennelDisabled();

        localizedClassNames = new HashMap();
        resourceBundles = new ArrayList();

        modelGraph = new JmiModelGraph(farragoPackage);

        tracer.info("Catalog successfully loaded");
    }

    //~ Methods ---------------------------------------------------------------

    protected static String getLocalizedClassKey(RefClass refClass)
    {
        String className =
            refClass.refMetaObject().refGetValue("name").toString();
        return "Uml" + className;
    }

    private Map stringToMap(String propString)
    {
        // TODO:  find something industrial strength
        StringTokenizer st = new StringTokenizer(propString, "=;", true);
        Map map = new HashMap();
        while (st.hasMoreTokens()) {
            String name = st.nextToken();
            if (name.equals(";")) {
                continue;
            }
            if (name.equals("=")) {
                throw new IllegalArgumentException(propString);
            }
            String eq = st.nextToken();
            if (!eq.equals("=")) {
                throw new IllegalArgumentException(propString);
            }
            String value = st.nextToken();
            if (value.equals(";")) {
                value = "";
            }
            map.put(
                "org.netbeans.mdr.persistence.jdbcimpl."
                + name,
                value);
        }
        return map;
    }

    /**
     * @return MDRepository storing this Farrago repository
     */
    public MDRepository getMdrRepos()
    {
        return mdrRepository;
    }

    /**
     * @return root package for transient metadata
     */
    public FarragoPackage getTransientFarragoPackage()
    {
        return transientFarragoPackage;
    }

    // override FarragoMetadataFactory
    public FennelPackage getFennelPackage()
    {
        // NOTE jvs 5-May-2004:  return the package corresponding to
        // in-memory storage
        return fennelPackage;
    }

    /**
     * @return CwmCatalog representing this FarragoRepos
     */
    public CwmCatalog getSelfAsCatalog()
    {
        // TODO:  variable
        return getCatalog(LOCALDB_CATALOG_NAME);
    }

    /**
     * @return maximum identifier length in characters
     */
    public int getIdentifierPrecision()
    {
        return maxNameLength;
    }

    public FemFarragoConfig getCurrentConfig()
    {
        // TODO:  prevent updates
        return (FemFarragoConfig) mdrRepository.getByMofId(currentConfigMofId);
    }

    /**
     * @return the name of the default Charset for this repository
     */
    public String getDefaultCharsetName()
    {
        return SaffronProperties.instance().defaultCharset.get();
    }

    /**
     * @return the name of the default Collation for this repository
     */
    public String getDefaultCollationName()
    {
        return SaffronProperties.instance().defaultCollation.get();
    }

    /**
     * @return true iff Fennel support should be used
     */
    public boolean isFennelEnabled()
    {
        return isFennelEnabled;
    }

    /**
     * Formats the fully-qualified localized name for an existing object,
     * including its type.
     *
     * @param modelElement catalog object
     * @return localized name
     */
    public String getLocalizedObjectName(
        CwmModelElement modelElement)
    {
        return getLocalizedObjectName(modelElement, modelElement.refClass());
    }

    /**
     * Formats the localized name for an unqualified typeless object.
     *
     * @param name object name
     * @return localized name
     */
    public String getLocalizedObjectName(
        String name)
    {
        return getLocalizedObjectName(null, name, null);
    }

    /**
     * Formats the fully-qualified localized name for an existing object.
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
     * Formats the fully-qualified localized name for an object that may not
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

        // TODO:  escaping
        if (refClass != null) {
            sb.append(getLocalizedClassName(refClass));
            sb.append(" ");
        }
        if (qualifierName != null) {
            sb.append("\"");
            sb.append(qualifierName);
            sb.append("\".");
        }
        sb.append("\"");
        sb.append(objectName);
        sb.append("\"");
        return sb.toString();
    }

    /**
     * Looks up the localized name for a class of metadata.
     *
     * @param refClass class of metadata, e.g. CwmTableClass
     *
     * @return localized name,  e.g. "table"
     */
    public String getLocalizedClassName(RefClass refClass)
    {
        String umlKey = getLocalizedClassKey(refClass);
        String name = (String) localizedClassNames.get(umlKey);
        if (name != null) {
            return name;
        } else {
            // NOTE jvs 12-Jan-2005:  we intentionally return something
            // nasty so that if it shows up in user-level error messages,
            // someone nice will maybe log a bug and get it fixed
            return "NOT_YET_LOCALIZED_" + umlKey;
        }
    }

    /**
     * Looks up a catalog by name.
     *
     * @param catalogName name of catalog to find
     *
     * @return catalog definition, or null if not found
     */
    public CwmCatalog getCatalog(String catalogName)
    {
        Collection catalogs =
            getRelationalPackage().getCwmCatalog().refAllOfType();
        return (CwmCatalog) FarragoCatalogUtil.getModelElementByName(
            catalogs, catalogName);
    }

    /**
     * Gets an element's tag.
     *
     * @param element the tagged CwmModelElement
     * @param tagName name of tag to find
     *
     * @return tag, or null if not found
     */
    public CwmTaggedValue getTag(
        CwmModelElement element,
        String tagName)
    {
        Collection tags =
            getCorePackage().getTaggedElement().getTaggedValue(element);
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
     * Tags an element.
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
        CwmTaggedValue tag = getTag(element, tagName);
        if (tag == null) {
            tag = newCwmTaggedValue();
            tag.setTag(tagName);
            getCorePackage().getTaggedElement().add(element, tag);
        }
        tag.setValue(tagValue);
    }

    /**
     * Gets a value tagged to an element.
     *
     * @param element the tagged CwmModelElement
     * @param tagName name of tag to find
     *
     * @return tag value, or null if not found
     */
    public String getTagValue(
        CwmModelElement element,
        String tagName)
    {
        CwmTaggedValue tag = getTag(element, tagName);
        if (tag == null) {
            return null;
        } else {
            return tag.getValue();
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        allocations.closeAllocation();
        if (modelLoader == null) {
            return;
        }
        tracer.fine("Closing catalog");
        NBMDRepositoryImpl nbRepos = (NBMDRepositoryImpl) mdrRepository;
        if (memStorageId != null) {
            mdrRepository.beginTrans(true);
            FarragoTransientStorage.ignoreCommit = false;
            if (transientFarragoPackage != null) {
                transientFarragoPackage.refDelete();
            }
            mdrRepository.endTrans();
            memStorageId = null;
        }
        modelLoader.close();
        modelLoader = null;
        tracer.info("Catalog successfully closed");
    }

    /**
     * Defines localization for this repository.
     *
     * @param bundles list of ResourceBundle instances to add for
     * localization.
     */
    public void addResourceBundles(List bundles)
    {
        resourceBundles.addAll(bundles);
        Iterator iter = bundles.iterator();
        while (iter.hasNext()) {
            ResourceBundle resourceBundle = (ResourceBundle) iter.next();
            Enumeration e = resourceBundle.getKeys();
            while (e.hasMoreElements()) {
                // NOTE jvs 12-Apr-2005:  This early binding won't
                // work once we have sessions with different locales, but
                // I'll leave that for someone wiser in the ways of i18n.
                String key = (String) e.nextElement();
                if (key.startsWith("Uml")) {
                    localizedClassNames.put(
                        key,
                        resourceBundle.getString(key));
                }
            }
        }
    }

    private void defineTypeAlias(
        String aliasName,
        CwmSqlsimpleType type)
    {
        CwmTypeAlias typeAlias = newCwmTypeAlias();
        typeAlias.setName(aliasName);
        typeAlias.setType(type);
    }

    /**
     * Creates objects owned by the system.  This is only done once during
     * database creation.
     */
    public void createSystemObjects()
    {
        tracer.info("Creating system-owned catalog objects");
        boolean rollback = true;
        try {
            beginReposTxn(true);
            initCatalog();
            rollback = false;
        } finally {
            endReposTxn(rollback);
        }
        tracer.info("Creation of system-owned catalog objects committed");
    }

    // implement FarragoTransientTxnContext
    public void beginTransientTxn()
    {
        tracer.fine("Begin transient repository transaction");
        mdrRepository.beginTrans(true);
    }

    // implement FarragoTransientTxnContext
    public void endTransientTxn()
    {
        tracer.fine("End transient repository transaction");
        mdrRepository.endTrans(false);
    }

    /**
     * Begins a metadata transaction on the repository.
     *
     * @param writable true for read/write; false for read-only
     */
    public void beginReposTxn(boolean writable)
    {
        if (writable) {
            tracer.fine("Begin read/write repository transaction");
        } else {
            tracer.fine("Begin read-only repository transaction");
        }
        mdrRepository.beginTrans(writable);
    }

    /**
     * Ends a metadata transaction on the repository.
     *
     * @param rollback true to rollback; false to commit
     */
    public void endReposTxn(boolean rollback)
    {
        if (rollback) {
            tracer.fine("Rollback repository transaction");
        } else {
            tracer.fine("Commit repository transaction");
        }
        mdrRepository.endTrans(rollback);
    }

    private void initCatalog()
    {
        createSystemCatalogs();
        createSystemTypes();
    }

    private void createSystemCatalogs()
    {
        // TODO:  default character set and collation name
        CwmCatalog catalog;

        catalog = newCwmCatalog();
        catalog.setName(SYSBOOT_CATALOG_NAME);
        initializeCatalog(catalog);

        catalog = newCwmCatalog();
        catalog.setName(LOCALDB_CATALOG_NAME);
        initializeCatalog(catalog);
    }

    public void initializeCatalog(CwmCatalog catalog)
    {
        catalog.setDefaultCharacterSetName(getDefaultCharsetName());
        catalog.setDefaultCollationName(getDefaultCollationName());
    }

    private void createSystemTypes()
    {
        CwmSqlsimpleType simpleType;

        // This is where all the builtin types are defined.  To add a new
        // builtin type, you have to:
        // (1) add a definition here
        // (2) add mappings in FarragoTypeFactoryImpl and maybe
        // SqlTypeName/SqlTypeFamily
        // (3) add Fennel mappings in
        // FennelRelUtil.convertSqlTypeNumberToFennelTypeOrdinal
        // (4) since I've already done all the easy cases, you'll probably
        // need lots of extra fancy semantics elsewhere
        simpleType = newCwmSqlsimpleType();
        simpleType.setName("BOOLEAN");
        simpleType.setTypeNumber(new Integer(Types.BOOLEAN));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("TINYINT");
        simpleType.setTypeNumber(new Integer(Types.TINYINT));
        simpleType.setNumericPrecision(new Integer(8));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("SMALLINT");
        simpleType.setTypeNumber(new Integer(Types.SMALLINT));
        simpleType.setNumericPrecision(new Integer(16));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("INTEGER");
        simpleType.setTypeNumber(new Integer(Types.INTEGER));
        simpleType.setNumericPrecision(new Integer(32));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));
        defineTypeAlias("INT", simpleType);

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("BIGINT");
        simpleType.setTypeNumber(new Integer(Types.BIGINT));
        simpleType.setNumericPrecision(new Integer(64));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("REAL");
        simpleType.setTypeNumber(new Integer(Types.REAL));
        simpleType.setNumericPrecision(new Integer(23));
        simpleType.setNumericPrecisionRadix(new Integer(2));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("DOUBLE");
        simpleType.setTypeNumber(new Integer(Types.DOUBLE));
        simpleType.setNumericPrecision(new Integer(52));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        defineTypeAlias("DOUBLE PRECISION", simpleType);
        defineTypeAlias("FLOAT", simpleType);

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("VARCHAR");
        simpleType.setTypeNumber(new Integer(Types.VARCHAR));

        // NOTE: this is an upper bound based on usage of 2-byte length
        // indicators in stored tuples; there are further limits based on page
        // size (imposed during table creation)
        simpleType.setCharacterMaximumLength(new Integer(65535));
        defineTypeAlias("CHARACTER VARYING", simpleType);

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("VARBINARY");
        simpleType.setTypeNumber(new Integer(Types.VARBINARY));
        simpleType.setCharacterMaximumLength(new Integer(65535));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("CHAR");
        simpleType.setTypeNumber(new Integer(Types.CHAR));
        simpleType.setCharacterMaximumLength(new Integer(65535));
        defineTypeAlias("CHARACTER", simpleType);

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("BINARY");
        simpleType.setTypeNumber(new Integer(Types.BINARY));
        simpleType.setCharacterMaximumLength(new Integer(65535));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("DATE");
        simpleType.setTypeNumber(new Integer(Types.DATE));
        simpleType.setDateTimePrecision(new Integer(0));

        // TODO jvs 26-July-2004: Support fractional precision for TIME and
        // TIMESTAMP.  Currently, most of the support is there for up to
        // milliseconds, but JDBC getString conversion is missing (see comments
        // in SqlDateTimeWithoutTZ).  SQL99 says default precision for
        // TIMESTAMP is microseconds, so some more work is required to
        // support that.  Default precision for TIME is seconds,
        // which is already the case.
        simpleType = newCwmSqlsimpleType();
        simpleType.setName("TIME");
        simpleType.setTypeNumber(new Integer(Types.TIME));
        simpleType.setDateTimePrecision(new Integer(0));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("TIMESTAMP");
        simpleType.setTypeNumber(new Integer(Types.TIMESTAMP));
        simpleType.setDateTimePrecision(new Integer(0));

        simpleType = newCwmSqlsimpleType();
        simpleType.setName("DECIMAL");
        simpleType.setTypeNumber(new Integer(Types.DECIMAL));
        simpleType.setNumericPrecision(new Integer(39));
        simpleType.setNumericPrecisionRadix(new Integer(10));
        defineTypeAlias("DEC", simpleType);

        FemSqlcollectionType collectType;
        collectType = newFemSqlmultisetType();
        collectType.setName("MULTISET");
        // a multiset has the same type# as an array for now
        collectType.setTypeNumber(new Integer(Types.ARRAY));
    }
}


// End FarragoRepos.java
