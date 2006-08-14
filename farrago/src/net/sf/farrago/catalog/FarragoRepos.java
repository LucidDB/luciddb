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
import net.sf.farrago.util.*;

import org.eigenbase.jmi.*;

import org.netbeans.api.mdr.*;


/**
 * FarragoRepos represents a loaded repository containing Farrago metadata.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoRepos
    extends FarragoAllocation,
        FarragoTransientTxnContext,
        FarragoMetadataFactory
{

    //~ Methods ----------------------------------------------------------------

    /**
     * @return MDRepository storing this Farrago repository
     */
    public MDRepository getMdrRepos();

    /**
     * @return model graph for repository metamodel
     */
    public JmiModelGraph getModelGraph();

    /**
     * @return model view for repository metamodel
     */
    public JmiModelView getModelView();

    /**
     * @return root package for transient metadata
     */
    public FarragoPackage getTransientFarragoPackage();

    /**
     * @return CwmCatalog representing this FarragoRepos
     */
    public CwmCatalog getSelfAsCatalog();

    /**
     * @return maximum identifier length in characters
     */
    public int getIdentifierPrecision();

    /**
     * @return element describing the configuration parameters
     */
    public FemFarragoConfig getCurrentConfig();

    /**
     * @return the name of the default {@link Charset} for this repository
     */
    public String getDefaultCharsetName();

    /**
     * @return the name of the default collation name for this repository. The
     * value is of the form <i>charset$locale$strength</i>, as per {@link
     * SqlParserUtil#parseCollation(String)}. The default is "ISO-8859-1$en_US".
     */
    public String getDefaultCollationName();

    /**
     * @return true iff Fennel support should be used
     */
    public boolean isFennelEnabled();

    /**
     * Formats the fully-qualified localized name for an existing object,
     * including its type.
     *
     * @param modelElement catalog object
     *
     * @return localized name
     */
    public String getLocalizedObjectName(
        CwmModelElement modelElement);

    /**
     * Formats the localized name for an unqualified typeless object.
     *
     * @param name object name
     *
     * @return localized name
     */
    public String getLocalizedObjectName(
        String name);

    /**
     * Formats the fully-qualified localized name for an existing object.
     *
     * @param modelElement catalog object
     * @param refClass if non-null, use this as the type of the object, e.g.
     * "table SCHEMA.TABLE"; if null, don't include type (e.g. just
     * "SCHEMA.TABLE")
     *
     * @return localized name
     */
    public String getLocalizedObjectName(
        CwmModelElement modelElement,
        RefClass refClass);

    /**
     * Formats the fully-qualified localized name for an object that may not
     * exist yet.
     *
     * @param qualifierName name of containing object, or null for unqualified
     * name
     * @param objectName name of object
     * @param refClass if non-null, the object type to use in the name; if null,
     * no type is prepended
     *
     * @return localized name
     */
    public String getLocalizedObjectName(
        String qualifierName,
        String objectName,
        RefClass refClass);

    /**
     * Looks up the localized name for a class of metadata.
     *
     * @param refClass class of metadata, e.g. CwmTableClass
     *
     * @return localized name, e.g. "table"
     */
    public String getLocalizedClassName(RefClass refClass);

    /**
     * Looks up a catalog by name.
     *
     * @param catalogName name of catalog to find
     *
     * @return catalog definition, or null if not found
     */
    public CwmCatalog getCatalog(String catalogName);

    /**
     * Gets an element's tag.
     *
     * @param element the tagged element
     * @param tagName name of tag to find
     *
     * @return tag, or null if not found
     *
     * @deprecated use getTagAnnotation instead
     */
    public CwmTaggedValue getTag(
        CwmModelElement element,
        String tagName);

    /**
     * Tags an element.
     *
     * @param element the element to tag
     * @param tagName name of tag to create; if a tag with this name already
     * exists, it will be updated
     * @param tagValue value to set
     *
     * @deprecated use setTagAnnotationValue instead
     */
    public void setTagValue(
        CwmModelElement element,
        String tagName,
        String tagValue);

    /**
     * Gets a value tagged to an element.
     *
     * @param element the tagged element
     * @param tagName name of tag to find
     *
     * @return tag value, or null if not found
     *
     * @deprecated use getTagAnnotationValue instead
     */
    public String getTagValue(
        CwmModelElement element,
        String tagName);

    /**
     * Gets an element's annotation tag.
     *
     * @param element the tagged element
     * @param tagName name of tag to find
     *
     * @return tag, or null if not found
     */
    public FemTagAnnotation getTagAnnotation(
        FemAnnotatedElement element,
        String tagName);

    /**
     * Tags an annotated element.
     *
     * @param element the element to tag
     * @param tagName name of tag to create; if a tag with this name already
     * exists, it will be updated
     * @param tagValue value to set
     */
    public void setTagAnnotationValue(
        FemAnnotatedElement element,
        String tagName,
        String tagValue);

    /**
     * Gets a value tagged to an annotated element.
     *
     * @param element the tagged element
     * @param tagName name of tag to find
     *
     * @return tag value, or null if not found
     */
    public String getTagAnnotationValue(
        FemAnnotatedElement element,
        String tagName);

    /**
     * Defines localization for this repository.
     *
     * @param bundles list of {@link ResourceBundle} instances to add for
     * localization.
     */
    public void addResourceBundles(List bundles);

    /**
     * Begins a metadata transaction on the repository.
     *
     * @param writable true for read/write; false for read-only
     */
    public void beginReposTxn(boolean writable);

    /**
     * Ends a metadata transaction on the repository.
     *
     * @param rollback true to rollback; false to commit
     */
    public void endReposTxn(boolean rollback);

    /**
     * Returns the metadata factory for a particular plugin. In particular,
     * <code>getMetadataFactory("Fem")</code> returns this.
     *
     * @param prefix The name of the prefix which identifies the metadata
     * factory
     */
    Object getMetadataFactory(String prefix);

    /**
     * Returns the an accessor for a sequence stored in the repository
     *
     * @param mofId the identifier for the sequence
     */
    public FarragoSequenceAccessor getSequenceAccessor(String mofId);

    /**
     * Returns the input string with property values substituted for variables
     * of the form <code>${VARNAME}</code>, such as that done by {@link
     * FarragoProperties#expandProperties(String)}..
     *
     * @param value String we want to expand
     *
     * @return expanded string, if value(s) were known
     */
    public String expandProperties(String value);

    /**
     * Returns a collection of all instances of a given class.
     *
     * <p>This method has the same effect as {@link RefClass#refAllOfClass()},
     * but is preferable because it returns a typed collection.
     */
    public <T extends RefObject> Collection<T> allOfClass(Class<T> clazz);

    /**
     * Returns a collection of all instances of a given type.
     *
     * <p>This method has the same effect as {@link RefClass#refAllOfType()},
     * but is preferable because it returns a typed collection.
     */
    public <T extends RefObject> Collection<T> allOfType(Class<T> clazz);

    /**
     * Returns the FarragoModelLoader for this repos. May return null if not
     * supported by implementation.
     */
    public FarragoModelLoader getModelLoader();
}

// End FarragoRepos.java
