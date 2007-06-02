/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import java.util.*;

import junit.framework.*;

import org.eigenbase.resource.*;


/**
 * Tests generated package org.eigenbase.resource (mostly a sanity check for
 * resgen infrastructure).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class EigenbaseResourceTest
    extends TestCase
{
    //~ Constructors -----------------------------------------------------------

    public EigenbaseResourceTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Verifies that resource properties such as SQLSTATE are available at
     * runtime.
     */
    public void testSqlstateProperty()
    {
        Properties props =
            EigenbaseResource.instance().IllegalIntervalLiteral.getProperties();
        assertEquals(
            "42000",
            props.get("SQLSTATE"));
    }
}

// End EigenbaseResourceTest.java
