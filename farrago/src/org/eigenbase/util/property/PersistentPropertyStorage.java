/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.util.property;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.regex.*;

/**
 * PersistentPropertyStorage handles storage for persistent property
 * objects.  For example, see {@link PersistentStringProperty}.
 *
 * @author stephan
 * @since December 3, 2004
 * @version $Id$
 */
class PersistentPropertyStorage
{
    //~ Static fields/initializers --------------------------------------------

    private static final HashMap propertyFileMap = new HashMap();

    //~ Fields ----------------------------------------------------------------

    private File propertyFile;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a PersistentPropertyStorage for the given property file.
     *
     * @param propertyFile the name of the property file to use
     */
    private PersistentPropertyStorage(File propertyFile)
    {
        this.propertyFile = propertyFile;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Factory method for PersistentPropertyStorage.  Guarantees that
     * only a single PersistentPropertyStorage object exists for any
     * property file.
     *
     * @param propertyFile the name of the property file to use
     * @throws IOException if <code>propertyFile</code> cannot be
     *                     converted into a canonical path name (via
     *                     {@link File#getCanonicalPath()}).
     */
    synchronized static PersistentPropertyStorage newPersistentPropertyStorage(
        String propertyFile)
        throws IOException
    {
        File file = new File(propertyFile);

        String canonicalName = file.getCanonicalPath();

        if (propertyFileMap.containsKey(canonicalName)) {
            return
                (PersistentPropertyStorage)propertyFileMap.get(canonicalName);
        }

        PersistentPropertyStorage storage =
            new PersistentPropertyStorage(file);

        propertyFileMap.put(canonicalName, storage);

        return storage;
    }

    /**
     * Store the given property's value in the property file.  Unlike
     * {@link java.util.Properties#store(java.io.OutputStream, String)}
     * this method does not obliterate the format of the existing
     * property file.
     *
     * @param property a {@link Property} value to store.
     * @throws IOException if a temporary file cannot be created
     *                     ({@link File#createTempFile(String, String)})
     *                     or written, or if the property file given
     *                     during construction cannot be created (if
     *                     it didn't already exist) or written.
     */
    synchronized void storeProperty(Property property)
        throws IOException
    {
        boolean propertyFileExists = propertyFile.exists();
        boolean propertyStored = false;

        File tempFile = null;
        if (propertyFileExists) {
            // Copy properties file to a temp file.
            tempFile = File.createTempFile("eigenbase", ".properties");

            FileReader fileReader = new FileReader(propertyFile);
            try {
                FileWriter fileWriter = new FileWriter(tempFile);
                try {
                    char[] buffer = new char[4096];
                    int read;
                    while((read = fileReader.read(buffer)) != -1) {
                        fileWriter.write(buffer, 0, read);
                    }
                    fileWriter.flush();
                } finally {
                    fileWriter.close();
                }
            } finally {
                fileReader.close();
            }

            // Copy the temp file back to properties file,
            // substituting our property's value for the existing one,
            // if any.
            Pattern pattern =
                Pattern.compile("^#?\\Q" + property.getPath() + "\\E=.*");
            Matcher matcher = pattern.matcher("");

            BufferedReader reader =
                new BufferedReader(new FileReader(tempFile));
            try {
                BufferedWriter writer =
                    new BufferedWriter(new FileWriter(propertyFile));
                try {
                    String line;
                    while((line = reader.readLine()) != null) {
                        matcher.reset(line);

                        if (matcher.matches()) {
                            // Found the property -- output our value.
                            writePropertyValue(writer, property);
                            propertyStored = true;
                        } else {
                            // Simply copy the existing line to the output.
                            writer.write(line);
                        }
                        writer.newLine();
                    }

                    writer.flush();
                } finally {
                    writer.close();
                }
            } finally {
                reader.close();

                // Delete the temp file, we're done with it.
                tempFile.delete();
            }
        }

        if (!propertyStored) {
            // The property does not currently exist in the file.
            // Simply append property=value to the property file.
            BufferedWriter writer =
                new BufferedWriter(new FileWriter(propertyFile, true));
            try {
                writer.newLine();
                writePropertyValue(writer, property);
                writer.newLine();
                writer.flush();
            } finally {
                writer.close();
            }
        }
    }

    private void writePropertyValue(Writer writer, Property property)
        throws IOException
    {
        writer.write(property.getPath());
        writer.write('=');

        String value = property.getInternal(null, false);
        if (value != null) {
            writer.write(value);
        }
    }
}
