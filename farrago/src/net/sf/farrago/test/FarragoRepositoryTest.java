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

import net.sf.farrago.catalog.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;

import junit.framework.*;

/**
 * FarragoRepositoryTest contains unit tests for the repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRepositoryTest extends FarragoTestCase
{
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
    
    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoRepositoryTest.class);
    }

    public void testTags()
    {
        FemAnnotatedElement element = (FemAnnotatedElement)
            repos.getSelfAsCatalog();

        String TAG_NAME = "SHIP_TO";
        String TAG_VALUE = "BUGS_BUNNY";

        assertNull(repos.getTagAnnotation(element, TAG_NAME));
        repos.setTagAnnotationValue(element, TAG_NAME, TAG_VALUE);
        assertEquals(
            TAG_VALUE,
            repos.getTagAnnotationValue(element, TAG_NAME));

        FemTagAnnotation tag = repos.getTagAnnotation(element, TAG_NAME);
        assertNotNull(tag);
        assertEquals(TAG_NAME, tag.getName());
        assertEquals(TAG_VALUE, tag.getValue());

        // Clean up the repo
        tag.refDelete();
    }
}

// End FarragoRepositoryTest.java
