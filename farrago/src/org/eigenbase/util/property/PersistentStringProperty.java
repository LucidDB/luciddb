/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by the
// Free Software Foundation; either version 2 of the License, or (at your
// option) any later version approved by The Eigenbase Project.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package org.eigenbase.util.property;

import java.io.*;

import java.util.*;
import java.util.logging.*;


/**
 * Definition and accessor for a string property that is capable of storing
 * itself in a <code>.properties</code> file.
 *
 * @author Stephan Zuercher
 * @version $Id$
 * @since December 3, 2004
 */
public class PersistentStringProperty
    extends StringProperty
{
    //~ Static fields/initializers ---------------------------------------------

    // NOTE jvs 2-Oct-2005:  have to avoid dragging in dependencies.
    /*
    private static final Logger tracer = EigenbaseTrace.getPropertyTracer();
     */
    private static final Logger tracer =
        Logger.getLogger(Property.class.getName());

    //~ Instance fields --------------------------------------------------------

    private StringProperty propertyFileLocation;
    private PersistentPropertyStorage storage;
    private boolean storageInitialized;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a persistent string property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     * @param propertyFileLocation Location of the property file where this
     * property's value should be persisted.
     */
    public PersistentStringProperty(
        Properties properties,
        String path,
        String defaultValue,
        StringProperty propertyFileLocation)
    {
        super(properties, path, defaultValue);

        // Delay initialization of storage: the property file location
        // may not be initialized until later (e.g. its value may change
        // in the constructor of the given properties object).
        this.propertyFileLocation = propertyFileLocation;
        this.storage = null;
        this.storageInitialized = false;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the value of this property.
     *
     * <p/>This method is synchronized to prevent multiple threads from
     * attempting to initialize the property storage ({@link #storage})
     * simultaneously.
     *
     * @return The previous value, or the default value if not set.
     */
    public synchronized String set(String value)
    {
        final String prevValue = super.set(value);

        if (!storageInitialized) {
            storageInitialized = true;

            if (propertyFileLocation.get() == null) {
                tracer.warning(
                    "Cannot store property '" + getPath()
                    + "' because storage location is not set");
                return prevValue;
            }
            try {
                storage =
                    PersistentPropertyStorage.newPersistentPropertyStorage(
                        propertyFileLocation.get());
            } catch (IOException e) {
                tracer.warning(
                    "Unable to initialize persistent property storage for '"
                    + getPath() + "'");
                tracer.throwing("PersistentPropertyStorage", "<init>", e);
                return prevValue;
            }
        }

        if (storage != null) {
            try {
                storage.storeProperty(this);
            } catch (IOException e) {
                tracer.warning(
                    "Unable to persist property '" + getPath() + "'");
                tracer.throwing("PersistentPropertyStorage", "set(String)", e);
            }
        }
        return prevValue;
    }
}

// End PersistentStringProperty.java
