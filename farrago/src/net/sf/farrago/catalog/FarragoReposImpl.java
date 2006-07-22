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

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;
import org.eigenbase.jmi.*;
import org.netbeans.mdr.handlers.BaseObjectHandler;

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

    private Map<String, FarragoSequenceAccessor> sequenceMap;

    //~ Constructors ----------------------------------------------------------

    /**
     * Opens a Farrago repository.
     */
    public FarragoReposImpl(
        FarragoAllocationOwner owner)
    {
        owner.addAllocation(this);
        sequenceMap = new HashMap<String, FarragoSequenceAccessor>();
    }

    // TODO jvs 30-Nov-2005:  rename these methods; initGraph initializes
    // other stuff besides the model graph

    /**
     * Initializes the model graph. The constructor of a concrete subclass must
     * call this after the repository has been initialized, and
     * {@link #getRootPackage()} is available.
     */
    protected void initGraph()
    {
        isFennelEnabled = !getDefaultConfig().isFennelDisabled();
        initGraphOnly();
    }

    protected void initGraphOnly()
    {
        ClassLoader classLoader = BaseObjectHandler.getDefaultClassLoader();
        modelGraph = new JmiModelGraph(getRootPackage(), classLoader, true);
        modelView = new JmiModelView(modelGraph);
    }

    //~ Methods ---------------------------------------------------------------

    protected FemFarragoConfig getDefaultConfig()
    {
        // TODO: multiple named configurations.  For now, build should have
        // imported exactly one configuration named Current.
        Collection<FemFarragoConfig> configs =
            (Collection<FemFarragoConfig>) 
            getConfigPackage().getFemFarragoConfig().refAllOfClass();

        assert (configs.size() == 1);
        FemFarragoConfig defaultConfig = configs.iterator().next();
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
        return FarragoCatalogUtil.getModelElementByName(
            allOfType(CwmCatalog.class), catalogName);
    }

    // implement FarragoRepos
    public FemTagAnnotation getTagAnnotation(
        FemAnnotatedElement element,
        String tagName)
    {
        Collection tags = element.getTagAnnotation();
        Iterator iter = tags.iterator();
        while (iter.hasNext()) {
            FemTagAnnotation tag = (FemTagAnnotation) iter.next();
            if (tag.getName().equals(tagName)) {
                return tag;
            }
        }
        return null;
    }

    // implement FarragoRepos
    public void setTagAnnotationValue(
        FemAnnotatedElement element,
        String tagName,
        String tagValue)
    {
        FemTagAnnotation tag = getTagAnnotation(element, tagName);
        if (tag == null) {
            tag = newFemTagAnnotation();
            tag.setName(tagName);
            element.getTagAnnotation().add(tag);
        }
        tag.setValue(tagValue);
    }

    // implement FarragoRepos
    public String getTagAnnotationValue(
        FemAnnotatedElement element,
        String tagName)
    {
        FemTagAnnotation tag = getTagAnnotation(element, tagName);
        if (tag == null) {
            return null;
        } else {
            return tag.getValue();
        }
    }

    // implement FarragoRepos
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

    // implement FarragoRepos
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

    // implement FarragoRepos
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

    // implement FarragoRepos
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

    public Object getMetadataFactory(String prefix)
    {
        if (prefix.equals("Fem")) {
            return (FarragoMetadataFactory) this;
        }
        throw Util.newInternal("Unknown metadata factory '" + prefix + "'");
    }
    
    public FarragoSequenceAccessor getSequenceAccessor(
        String mofId)
    {
        synchronized(sequenceMap) {
            FarragoSequenceAccessor sequence = sequenceMap.get(mofId);
            if (sequence != null) {
                return sequence;
            }
            sequence = new FarragoSequenceAccessor(this, mofId);
            allocations.addAllocation(sequence);
            sequenceMap.put(mofId, sequence);
            return sequence;
        }
    }

    /* (non-Javadoc)
     * @see net.sf.farrago.catalog.FarragoRepos#expandProperties(java.lang.String)
     */
    public String expandProperties(String value)
    {
        return FarragoProperties.instance().expandProperties(value);
    }

    private RefClass findRefClass(
        Class<? extends RefObject> clazz)
    {
        JmiClassVertex vertex = modelGraph.getVertexForJavaInterface(clazz);
        return vertex.getRefClass();
    }

    public <T extends RefObject>
    Collection<T> allOfClass(Class<T> clazz)
    {
        RefClass refClass = findRefClass(clazz);
        return (Collection<T>) refClass.refAllOfClass();
    }

    public <T extends RefObject>
    Collection<T> allOfType(Class<T> clazz)
    {
        RefClass refClass = findRefClass(clazz);
        return (Collection<T>) refClass.refAllOfType();
    }

    // implement FarragoRepos
    public FarragoModelLoader getModelLoader()
    {
        return null;
    }
}


// End FarragoReposImpl.java
