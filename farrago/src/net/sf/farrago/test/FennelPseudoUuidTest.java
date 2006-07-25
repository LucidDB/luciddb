/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.test;

import net.sf.farrago.fennel.*;

import org.eigenbase.util.*;


/**
 * FennelPseudoUuidTest tests the FennelPseudoUuid class.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FennelPseudoUuidTest
    extends FarragoTestCase
{

    //~ Static fields/initializers ---------------------------------------------

    static {
        Util.loadLibrary("farrago");
    }

    //~ Constructors -----------------------------------------------------------

    public FennelPseudoUuidTest(String name)
        throws Exception
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testValidPseudoUuid()
        throws Exception
    {
        FennelPseudoUuid uuid1 =
            new FennelPseudoUuid(FennelPseudoUuidGenerator.validUuid());

        assertEquals(uuid1, uuid1);
        assertNotNull(uuid1.toString());
        assertTrue(uuid1.toString().length() > 0);
    }

    public void testInvalidPseudoUuid()
        throws Exception
    {
        FennelPseudoUuid uuid1 =
            new FennelPseudoUuid(FennelPseudoUuidGenerator.validUuid());
        FennelPseudoUuid uuid2 =
            new FennelPseudoUuid(FennelPseudoUuidGenerator.invalidUuid());

        assertFalse(uuid1.equals(uuid2));
        assertNotNull(uuid2.toString());
        assertTrue(uuid2.toString().length() > 0);
    }

    public void testPseudoUuidSymmetry()
        throws Exception
    {
        FennelPseudoUuid uuid1 =
            new FennelPseudoUuid(FennelPseudoUuidGenerator.validUuid());
        FennelPseudoUuid uuid2 = new FennelPseudoUuid(uuid1.toString());
        assertEquals(uuid1, uuid2);

        String uuidStr = "01234567-89ab-cdef-0123-456789abcdef";
        FennelPseudoUuid uuid3 = new FennelPseudoUuid(uuidStr);

        byte [] uuidBytes = new byte[16];
        byte val = 0;
        for (int i = 0; i < 16; i++) {
            if ((i == 0) || (i == 8)) {
                val = 0x01;
            }

            uuidBytes[i] = val;
            val += 0x22;
        }
        FennelPseudoUuid uuid4 = new FennelPseudoUuid(uuidBytes);

        assertEquals(uuid3, uuid4);
    }
}

// End FennelPseudoUuidTest.java
