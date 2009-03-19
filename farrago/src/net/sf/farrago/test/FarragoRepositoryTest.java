/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.test;

import java.io.*;

import java.util.*;

import javax.xml.parsers.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.util.*;

import org.xml.sax.*;
import org.xml.sax.helpers.*;


/**
 * FarragoRepositoryTest contains unit tests for the repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRepositoryTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoRepositoryTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoRepositoryTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoRepositoryTest.class);
    }

    /**
     * Tests {@link FarragoRepos} interface for tags manipulation.
     */
    public void testTags()
    {
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        try {
            txn.beginWriteTxn();

            FemAnnotatedElement element =
                (FemAnnotatedElement) repos.getSelfAsCatalog();

            String TAG_NAME = "SHIP_TO";
            String TAG_VALUE = "BUGS_BUNNY";

            assertNull(repos.getTagAnnotation(element, TAG_NAME));
            repos.setTagAnnotationValue(element, TAG_NAME, TAG_VALUE);
            assertEquals(
                TAG_VALUE,
                repos.getTagAnnotationValue(element, TAG_NAME));

            FemTagAnnotation tag = repos.getTagAnnotation(element, TAG_NAME);
            assertNotNull(tag);
            assertEquals(
                TAG_NAME,
                tag.getName());
            assertEquals(
                TAG_VALUE,
                tag.getValue());

            // Clean up the repo
            tag.refDelete();
        } finally {
            txn.commit();
        }
    }

    public void testObjIntegrityVerificationPass()
    {
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        try {
            txn.beginReadTxn();

            // Verify an existing object
            CwmCatalog catalog = repos.getSelfAsCatalog();
            CwmSchema schema =
                (CwmSchema) FarragoCatalogUtil.getModelElementByName(
                    catalog.getOwnedElement(),
                    "SALES");
            CwmTable tbl =
                (CwmTable) FarragoCatalogUtil.getModelElementByName(
                    schema.getOwnedElement(),
                    "DEPTS");

            List<FarragoReposIntegrityErr> errs = repos.verifyIntegrity(tbl);
            assertEquals(0, errs.size());
        } finally {
            txn.commit();
        }
    }

    public void testObjIntegrityVerificationFail()
    {
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        try {
            txn.beginWriteTxn();

            CwmTable tbl = repos.newCwmTable();
            tbl.setName("BOOFAR");

            // Intentionaly leave visibility unset; it's mandatory,
            // so this should cause an Attribute error
            tbl.setTemporary(false);
            tbl.setSystem(false);
            tbl.setAbstract(false);

            CwmColumn col = repos.newCwmColumn();
            tbl.getFeature().add(col);

            // Intentionally leave type unset; it's mandatory,
            // so this should cause an AssociationEnd error
            col.setName("SNEE");
            col.setCharacterSetName("ASCII");
            col.setCollationName("DEFAULT");
            col.setVisibility(VisibilityKindEnum.VK_PUBLIC);
            col.setChangeability(ChangeableKindEnum.CK_CHANGEABLE);
            CwmExpression nullExpression = repos.newCwmExpression();
            nullExpression.setLanguage("SQL");
            nullExpression.setBody("NULL");
            col.setInitialValue(nullExpression);
            col.setIsNullable(NullableTypeEnum.COLUMN_NO_NULLS);

            // Now run verification on table
            List<FarragoReposIntegrityErr> errs = repos.verifyIntegrity(tbl);
            assertEquals(1, errs.size());

            FarragoReposIntegrityErr err = errs.get(0);

            assertEquals(
                "javax.jmi.reflect.WrongSizeException, "
                + "Attribute = visibility, Table = BOOFAR",
                stripDollarImpl(err.getDescription()));

            // Now run verification on column
            errs = repos.verifyIntegrity(col);
            assertEquals(1, errs.size());

            err = errs.get(0);

            // description has a MOFID in the middle, so compare
            // prefix and suffix
            assertTrue(
                err.getDescription().startsWith(
                    "javax.jmi.reflect.WrongSizeException:  "
                    + "Not enough objects linked to "));
            assertTrue(
                stripDollarImpl(err.getDescription()).endsWith(
                    "at end 'structuralFeature'., "
                    + "AssociationEnd = type, Column = SNEE"));
        } finally {
            // Always rollback to clean up repo
            txn.rollback();
        }
    }

    private String stripDollarImpl(String str)
    {
        // Handle Enki Hibernate vs. Netbeans difference in class names
        return str.replaceAll("\\$Impl", "");
    }

    public void testDefaultCharacterSet()
    {
        assertEquals("ISO-8859-1", repos.getDefaultCharsetName());
    }

    public void testInvalidCharFilter()
        throws Exception
    {
        String [] validFiles =
        {
            "valid-utf8.xml",
            "valid-utf8-bom.xml",
            "valid-utf16be.xml",
            "valid-utf16be-bom.xml",
            "valid-utf16le.xml",
            "valid-utf16le-bom.xml",
        };

        String [] invalidFiles =
        {
            "invalid-ascii.xml",
            "invalid-utf8.xml",
            "invalid-utf8-bom.xml",
            "invalid-utf16be.xml",
            "invalid-utf16be-bom.xml",
            "invalid-utf16le.xml",
            "invalid-utf16le-bom.xml",
        };

        String [] allFiles =
            new String[validFiles.length + invalidFiles.length];
        for (int i = 0; i < validFiles.length; i++) {
            allFiles[i] = validFiles[i];
        }
        for (int i = 0; i < invalidFiles.length; i++) {
            allFiles[i + validFiles.length] = invalidFiles[i];
        }

        SAXParserFactory spf = SAXParserFactory.newInstance();
        SAXParser parser = spf.newSAXParser();

        // Verify that these files pass XML parsing.
        final String DIR =
            FarragoProperties.instance().expandProperties(
                "${FARRAGO_HOME}/testcases/xml");
        for (String filename : validFiles) {
            File file = new File(DIR, filename);

            parser.reset();

            // will throw on invalid file
            parser.parse(file, new DefaultHandler());
        }

        // Verify that these files cause XML parsing to fail.
        for (String filename : invalidFiles) {
            File file = new File(DIR, filename);

            parser.reset();
            try {
                parser.parse(file, new DefaultHandler());

                fail("Missing expected exception");
            } catch (SAXParseException e) {
                // Expected.
            }
        }

        // Verify that we're able to filter out the invalid chars and parse
        // all the files.
        for (String filename : allFiles) {
            File file = new File(DIR, filename);

            FileInputStream in = new FileInputStream(file);
            FarragoReposUtil.InvalidXmlCharFilterInputStream filter =
                new FarragoReposUtil.InvalidXmlCharFilterInputStream(in);

            parser.reset();
            parser.parse(
                filter,
                new DefaultHandler() {
                    private int elemNum = -1;

                    @Override public void startElement(
                        String uri,
                        String localName,
                        String name,
                        Attributes attributes)
                    {
                        String value;
                        elemNum++;

                        switch (elemNum) {
                        case 0: // test
                            return;

                        case 1:
                        case 2: // elem
                            value = attributes.getValue(0);
                            break;

                        default:
                            fail("too many elements");
                            return;
                        }

                        assertTrue(value.startsWith("a" + elemNum));
                        if (value.length() > 2) {
                            assertEquals(3, value.length());
                            assertTrue(value.endsWith(":"));
                        }
                    }

                    @Override public void characters(
                        char [] ch,
                        int start,
                        int length)
                    {
                        if ((elemNum < 1) || (elemNum > 2)) {
                            return;
                        }

                        String value = new String(ch, start, length);
                        value = value.trim();
                        if (value.length() == 0) {
                            return;
                        }

                        assertTrue(value.startsWith("e" + elemNum));
                        if (value.length() > 2) {
                            assertEquals(3, value.length());
                            assertTrue(value.endsWith(":"));
                        }
                    }
                });

            filter.close();
        }
    }
}

// End FarragoRepositoryTest.java
