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

import junit.framework.*;

import net.sf.farrago.util.*;


/**
 * FarragoPropertiesTest tests the {@link FarragoProperties} class.
 */
public class FarragoPropertiesTest
    extends TestCase
{

    //~ Constructors -----------------------------------------------------------

    public FarragoPropertiesTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testPropertyFields()
        throws Exception
    {
        assertNotNull(FarragoProperties.instance().homeDir.get());

        assertNotNull(FarragoProperties.instance().getCatalogDir());
    }

    public void testPropertyExpansion()
        throws Exception
    {
        FarragoProperties props = FarragoProperties.instance();

        String [] expectUnchanged =
            new String[] {
                "foo", "foo ${", "} foo", "${foo}", "${}", "${", "$}", "{}",
                "$ {FARRAGO_HOME}", "${ FARRAGO_HOME}", "${FARRAGO_HOME }",
                "${ FARRAGO_HOME }", "$ { FARRAGO_HOME }", "$FARRAGO_HOME",
                "${FARRAGO_HOME", "$FARRAGO_HOME}", "{FARRAGO_HOME}"
            };

        for (int i = 0; i < expectUnchanged.length; i++) {
            assertSame(
                expectUnchanged[i],
                props.expandProperties(expectUnchanged[i]));
        }

        String home = props.homeDir.get();

        String [][] expectChanged =
            new String[][] {
                { "${FARRAGO_HOME}", home },
                { "{${FARRAGO_HOME}}", "{" + home + "}" },
                { "${FARRAGO_HOME}/foo", home + "/foo" },
                { "${FARRAGO_HOME}${FARRAGO_HOME}", home + home },
                { "${FARRAGO_HOME} ${FARRAGO_HOME}", home + " " + home },
                { " ${FARRAGO_HOME} ${FARRAGO_HOME}", " " + home + " " + home },
                {
                    " ${FARRAGO_HOME} ${FARRAGO_HOME} ",
                    " " + home + " " + home + " "
                },
                { "${X}${FARRAGO_HOME}", "${X}" + home },
                { "${FARRAGO_HOME}${X}", home + "${X}" },
                { "${X}${FARRAGO_HOME}${X}", "${X}" + home + "${X}" },
                { "${FARRAGO_HOME} is ${FARRAGO_HOME}", home + " is " + home },
                { "No place like ${FARRAGO_HOME}", "No place like " + home },
            };

        for (int i = 0; i < expectChanged.length; i++) {
            assertEquals(
                expectChanged[i][1],
                props.expandProperties(expectChanged[i][0]));
        }
    }
}

// End FarragoPropertiesTest.java
