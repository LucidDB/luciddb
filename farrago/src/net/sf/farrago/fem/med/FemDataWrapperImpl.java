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
package net.sf.farrago.fem.med;

import java.io.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.FarragoProperties;

import org.eigenbase.util.*;
import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;


/**
 * FemDataWrapperImpl is a custom implementation for FemDataWrapper.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FemDataWrapperImpl extends InstanceHandler
    implements FemDataWrapper,
        DdlValidatedElement,
        DdlStoredElement
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FemDataWrapperImpl object.
     *
     * @param storable .
     */
    protected FemDataWrapperImpl(StorableObject storable)
    {
        super(storable);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlValidatedElement
    public void validateDefinition(
        DdlValidator validator,
        boolean creation)
    {
        FarragoRepos repos = validator.getRepos();
        Properties props = getStorageOptionsAsProperties(this);

        FarragoMedDataWrapper wrapper;
        try {
            if (!getLibraryFile().startsWith(FarragoDataWrapperCache.LIBRARY_CLASS_PREFIX)) {
                // convert library filename to absolute path, if necessary
                String libraryFile = getLibraryFile();

                String expandedLibraryFile =
                    FarragoProperties.instance().expandProperties(libraryFile);

                // REVIEW: SZ: 7/20/2004: Maybe the library should
                // always be an absolute path?  (e.g. Always report an
                // error if the path given by the user is relative.)
                // If a user installs a thirdparty Data Wrapper we
                // probably don't want them using relative paths to
                // call out its location.
                if (libraryFile.equals(expandedLibraryFile)) {
                    // No properties were expanded, so make the path
                    // absolute if it isn't already absolute.
                    File file = new File(libraryFile);
                    setLibraryFile(file.getAbsolutePath());
                } else {
                    // Test that the expanded library file is an
                    // aboslute path.  We don't set the absolute path
                    // because we want to keep the property in the
                    // library name.
                    File file = new File(expandedLibraryFile);
                    if (!file.isAbsolute()) {
                        throw new IOException(libraryFile
                            + " does not evaluate to an absolute path");
                    }
                }
            }

            // validate that we can successfully initialize the wrapper
            wrapper = loadFromCache(validator.getDataWrapperCache());
        } catch (Throwable ex) {
            throw validator.res.newValidatorDataWrapperInvalid(
                repos.getLocalizedObjectName(this, null),
                ex);
        }

        if (isForeign()) {
            if (!wrapper.isForeign()) {
                throw validator.res.newValidatorForeignWrapperHasLocalImpl(
                    repos.getLocalizedObjectName(this, null));
            }
        } else {
            if (wrapper.isForeign()) {
                throw validator.res.newValidatorLocalWrapperHasForeignImpl(
                    repos.getLocalizedObjectName(this, null));
            }
        }
    }

    // implement DdlValidatedElement
    public void validateDeletion(
        DdlValidator validator,
        boolean truncation)
    {
    }

    /**
     * Loads and caches an accessor for this wrapper, or uses a cached
     * instance.
     *
     * @param cache .
     *
     * @return loaded wrapper accessor
     */
    public FarragoMedDataWrapper loadFromCache(FarragoDataWrapperCache cache)
    {
        Properties props = getStorageOptionsAsProperties(this);
        return cache.loadWrapper(
            refMofId(),
            getLibraryFile(),
            props);
    }

    // TODO:  move somewhere else
    static Properties getStorageOptionsAsProperties(
        FemElementWithStorageOptions element)
    {
        Properties props = new Properties();

        // TODO:  validate no duplicates
        Iterator iter = element.getStorageOptions().iterator();
        while (iter.hasNext()) {
            FemStorageOption option = (FemStorageOption) iter.next();
            props.setProperty(
                option.getName(),
                option.getValue());
        }
        return props;
    }

    // implement DdlStoredElement
    public void createStorage(DdlValidator validator)
    {
    }

    // implement DdlStoredElement
    public void deleteStorage(DdlValidator validator)
    {
        validator.discardDataWrapper(this);
    }

    // implement DdlStoredElement
    public void truncateStorage(DdlValidator validator)
    {
    }
}


// End FemDataWrapperImpl.java
