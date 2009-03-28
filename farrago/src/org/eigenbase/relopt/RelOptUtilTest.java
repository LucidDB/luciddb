/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Portions Copyright (C) 2006-2009 John V. Sichi
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
package org.eigenbase.relopt;

import junit.framework.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Unit test for {@link RelOptUtil} and other classes in this package.
 *
 * @author jhyde
 * @version $Id$
 */
public class RelOptUtilTest
    extends TestCase
{
    //~ Constructors -----------------------------------------------------------

    public RelOptUtilTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testTypeDump()
    {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
        RelDataType t1 =
            typeFactory.createStructType(
                new RelDataType[] {
                    typeFactory.createSqlType(SqlTypeName.DECIMAL, 5, 2),
                    typeFactory.createSqlType(SqlTypeName.VARCHAR, 10),
                },
                new String[] {
                    "f0",
                    "f1"
                });
        TestUtil.assertEqualsVerbose(
            TestUtil.fold(
                new String[] {
                    "f0 DECIMAL(5, 2) NOT NULL,",
                    "f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL"
                }),
            RelOptUtil.dumpType(t1));

        RelDataType t2 =
            typeFactory.createStructType(
                new RelDataType[] {
                    t1,
                    typeFactory.createMultisetType(t1, -1),
                },
                new String[] {
                    "f0",
                    "f1"
                });
        TestUtil.assertEqualsVerbose(
            TestUtil.fold(
                new String[] {
                    "f0 RECORD (",
                    "  f0 DECIMAL(5, 2) NOT NULL,",
                    "  f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL) NOT NULL,",
                    "f1 RECORD (",
                    "  f0 DECIMAL(5, 2) NOT NULL,",
                    "  f1 VARCHAR(10) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL) NOT NULL MULTISET NOT NULL"
                }),
            RelOptUtil.dumpType(t2));
    }

    /**
     * Tests the rules for how we name rules.
     */
    public void testRuleGuessDescription()
    {
        assertEquals("Bar", RelOptRule.guessDescription("com.foo.Bar"));
        assertEquals("Baz", RelOptRule.guessDescription("com.flatten.Bar$Baz"));

        // yields "1" (which as an integer is an invalid
        try {
            Util.discard(RelOptRule.guessDescription("com.foo.Bar$1"));
            fail("expected exception");
        } catch (RuntimeException e) {
            assertEquals(
                "Derived description of rule class com.foo.Bar$1 is an " +
                    "integer, not valid. Supply a description manually.",
                e.getMessage());
        }
    }
}

// End RelOptUtilTest.java
