/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.core.VisibilityKindEnum;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.sql2003.*;


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
    }

    public void testObjIntegrityVerificationPass()
    {
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
    }

    public void testObjIntegrityVerificationFail()
    {
        FarragoReposTxnContext txn = repos.newTxnContext();
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
                + "Attribute$Impl = visibility, Table = BOOFAR",
                err.getDescription());

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
                err.getDescription().endsWith(
                    "at end 'structuralFeature'., "
                    + "AssociationEnd$Impl = type, Column = SNEE"));
        } finally {
            // Always rollback to clean up repo
            txn.rollback();
        }
    }
}

// End FarragoRepositoryTest.java
