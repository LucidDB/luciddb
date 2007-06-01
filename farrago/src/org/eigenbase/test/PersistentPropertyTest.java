/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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
package org.eigenbase.test;

import java.io.*;

import java.util.*;

import org.eigenbase.util.property.*;


/**
 * PersistentPropertyTest tests persistent properties using temporary files.
 *
 * @author Stephan Zuercher
 * @version $Id$
 * @since December 3, 2004
 */
public class PersistentPropertyTest
    extends EigenbaseTestCase
{
    //~ Constructors -----------------------------------------------------------

    public PersistentPropertyTest(String name)
        throws Exception
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testPersistentStringProperty()
        throws Exception
    {
        final String DEFAULT_VALUE = "default value";
        final String NEW_VALUE = "new value";
        final String PROP_NAME = "test.eigenbase.persistent.string";
        final String EXISTING_PROP_NAME1 = "test.eigenbase.existing1";
        final String EXISTING_PROP_VALUE1 = "existing value 1";
        final String EXISTING_PROP_NAME2 = "test.eigenbase.existing2";
        final String EXISTING_PROP_VALUE2 = "existing value 2";
        final String EXISTING_PROP_NAME3 = "test.eigenbase.existing3";
        final String EXISTING_PROP_VALUE3 = "existing value 3";
        final String EXISTING_NEW_VALUE = "new value for existing prop";
        final String EXISTING_DEFAULT_VALUE = "existing default value";

        File tempPropFile = File.createTempFile("eigenbaseTest", ".properties");
        BufferedWriter writer =
            new BufferedWriter(new FileWriter(tempPropFile));
        writer.write("# Test config file");
        writer.newLine();
        writer.newLine();
        writer.write(EXISTING_PROP_NAME1 + "=" + EXISTING_PROP_VALUE1);
        writer.newLine();
        writer.write(EXISTING_PROP_NAME2 + "=" + EXISTING_PROP_VALUE2);
        writer.newLine();
        writer.write(EXISTING_PROP_NAME3 + "=" + EXISTING_PROP_VALUE3);
        writer.newLine();
        writer.flush();
        writer.close();

        Properties props = new Properties();
        props.load(new FileInputStream(tempPropFile));

        StringProperty propertyFileLocation =
            new StringProperty(
                props,
                "test.eigenbase.properties",
                tempPropFile.getAbsolutePath());

        PersistentStringProperty persistentProperty =
            new PersistentStringProperty(
                props,
                PROP_NAME,
                DEFAULT_VALUE,
                propertyFileLocation);

        PersistentStringProperty persistentExistingProperty =
            new PersistentStringProperty(
                props,
                EXISTING_PROP_NAME2,
                EXISTING_DEFAULT_VALUE,
                propertyFileLocation);

        assertEquals(
            DEFAULT_VALUE,
            persistentProperty.get());
        assertNull(props.getProperty(PROP_NAME));
        assertEquals(
            EXISTING_PROP_VALUE1,
            props.getProperty(EXISTING_PROP_NAME1));
        assertEquals(
            EXISTING_PROP_VALUE2,
            persistentExistingProperty.get());
        assertEquals(
            EXISTING_PROP_VALUE2,
            props.getProperty(EXISTING_PROP_NAME2));
        assertEquals(
            EXISTING_PROP_VALUE3,
            props.getProperty(EXISTING_PROP_NAME3));

        persistentProperty.set(NEW_VALUE);

        assertEquals(
            NEW_VALUE,
            persistentProperty.get());
        assertEquals(
            NEW_VALUE,
            props.getProperty(PROP_NAME));

        persistentExistingProperty.set(EXISTING_NEW_VALUE);

        assertEquals(
            EXISTING_NEW_VALUE,
            persistentExistingProperty.get());
        assertEquals(
            EXISTING_NEW_VALUE,
            props.getProperty(EXISTING_PROP_NAME2));

        // reset properties, location and persistent property (reloads
        // properties stored in file)
        props = new Properties();
        props.load(new FileInputStream(tempPropFile));

        propertyFileLocation =
            new StringProperty(
                props,
                "test.eigenbase.properties",
                tempPropFile.getAbsolutePath());

        persistentProperty =
            new PersistentStringProperty(
                props,
                PROP_NAME,
                DEFAULT_VALUE,
                propertyFileLocation);

        assertEquals(
            NEW_VALUE,
            persistentProperty.get());
        assertEquals(
            NEW_VALUE,
            props.getProperty(PROP_NAME));

        assertEquals(
            EXISTING_NEW_VALUE,
            persistentExistingProperty.get());
        assertEquals(
            EXISTING_NEW_VALUE,
            props.getProperty(EXISTING_PROP_NAME2));

        assertEquals(
            EXISTING_PROP_VALUE1,
            props.getProperty(EXISTING_PROP_NAME1));
        assertEquals(
            EXISTING_PROP_VALUE3,
            props.getProperty(EXISTING_PROP_NAME3));

        // delete file if test succeeded
        tempPropFile.delete();
    }
}

// End PersistentPropertyTest.java
