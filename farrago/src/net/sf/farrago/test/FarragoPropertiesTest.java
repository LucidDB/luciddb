/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
