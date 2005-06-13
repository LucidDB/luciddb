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

import net.sf.farrago.FarragoPackage;
import net.sf.farrago.FarragoMetadataFactory;
import net.sf.farrago.cwm.core.CwmModelElement;
import net.sf.farrago.cwm.core.CwmTaggedValue;
import net.sf.farrago.cwm.relational.CwmCatalog;
import net.sf.farrago.fem.config.FemFarragoConfig;
import net.sf.farrago.util.FarragoAllocation;
import net.sf.farrago.util.FarragoTransientTxnContext;
import org.eigenbase.jmi.JmiModelGraph;
import org.eigenbase.jmi.JmiModelView;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.netbeans.api.mdr.MDRepository;

import javax.jmi.reflect.RefClass;
import java.util.List;
import java.util.ResourceBundle;
import java.nio.charset.Charset;
import java.text.CollationKey;

/**
 * FarragoRepos represents a loaded repository containing Farrago metadata.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoRepos extends FarragoAllocation,
    FarragoTransientTxnContext,
    FarragoMetadataFactory
{
    //~ Methods ---------------------------------------------------------------

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
     * @return the name of the default collation name for this repository.
     * The value is of the form <i>charset$locale$strength</i>, as per
     * {@link SqlParserUtil#parseCollation(String)}.
     * The default is "ISO-8859-1$en_US".
     *
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
     * @return localized name
     */
    public String getLocalizedObjectName(
        CwmModelElement modelElement);

    /**
     * Formats the localized name for an unqualified typeless object.
     *
     * @param name object name
     * @return localized name
     */
    public String getLocalizedObjectName(
        String name);

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
        RefClass refClass);

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
        RefClass refClass);

    /**
     * Looks up the localized name for a class of metadata.
     *
     * @param refClass class of metadata, e.g. CwmTableClass
     *
     * @return localized name,  e.g. "table"
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
     * @param element the tagged CwmModelElement
     * @param tagName name of tag to find
     *
     * @return tag, or null if not found
     */
    public CwmTaggedValue getTag(
        CwmModelElement element,
        String tagName);

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
        String tagValue);

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
}


// End FarragoRepos.java
