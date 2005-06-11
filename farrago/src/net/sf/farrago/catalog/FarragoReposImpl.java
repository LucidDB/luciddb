/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.SaffronProperties;
import org.eigenbase.jmi.*;

import java.util.logging.Logger;

/**
 * Implementation of {@link FarragoRepos} using a MDR repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoReposImpl extends FarragoMetadataFactoryImpl
    implements FarragoRepos
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = FarragoTrace.getReposTracer();

    /** TODO:  look this up from repository */
    private static final int maxNameLength = 128;

    //~ Instance fields -------------------------------------------------------

    private boolean isFennelEnabled;

    protected final FarragoCompoundAllocation allocations =
        new FarragoCompoundAllocation();

    private final Map localizedClassNames = new HashMap();

    private final List resourceBundles = new ArrayList();

    private JmiModelGraph modelGraph;

    private JmiModelView modelView;

    //~ Constructors ----------------------------------------------------------

    /**
     * Opens a Farrago repository.
     *
     * After the constructor is complete, the caller must call {@link #init}.
     */
    public FarragoReposImpl(
        FarragoAllocationOwner owner)
    {
        owner.addAllocation(this);
    }

    public void init()
    {
        isFennelEnabled = !getDefaultConfig().isFennelDisabled();
        modelGraph = new JmiModelGraph(getRootPackage());
        modelView = new JmiModelView(modelGraph);
    }

    //~ Methods ---------------------------------------------------------------

    protected FemFarragoConfig getDefaultConfig()
    {
        // TODO: multiple named configurations.  For now, build should have
        // imported exactly one configuration named Current.
        Collection configs =
            getConfigPackage().getFemFarragoConfig().refAllOfClass();

        assert (configs.size() == 1);
        FemFarragoConfig defaultConfig =
            (FemFarragoConfig) configs.iterator().next();
        assert (defaultConfig.getName().equals("Current"));
        return defaultConfig;
    }

    protected static String getLocalizedClassKey(RefClass refClass)
    {
        String className =
            refClass.refMetaObject().refGetValue("name").toString();
        return "Uml" + className;
    }

    /**
     * @return model graph for repository metamodel
     */
    public JmiModelGraph getModelGraph()
    {
        return modelGraph;
    }

    /**
     * @return model view for repository metamodel
     */
    public JmiModelView getModelView()
    {
        return modelView;
    }

    /**
     * @return CwmCatalog representing this FarragoRepos
     */
    public CwmCatalog getSelfAsCatalog()
    {
        // TODO:  variable
        return getCatalog(FarragoCatalogInit.LOCALDB_CATALOG_NAME);
    }

    /**
     * @return maximum identifier length in characters
     */
    public int getIdentifierPrecision()
    {
        return maxNameLength;
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
}


// End FarragoReposImpl.java
