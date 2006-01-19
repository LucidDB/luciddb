/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.test;

import junit.framework.TestCase;

import org.eigenbase.sarg.*;
import org.eigenbase.rex.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

import java.math.*;
import java.util.*;

/**
 * SargTest tests the {@link org.eigenbase.sarg} class library.
 *
 *<p>
 *
 * NOTE jvs 17-Jan-2006:  This class lives in org.eigenbase.test rather
 * than org.eigenbase.sarg by design:  we want to make sure we're only
 * testing via the public interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargTest extends TestCase
{
    private SargFactory sargFactory;
    
    private RelDataType intType;
    
    private RexNode intLiteral7;
    
    private RexNode intLiteral8point5;
    
    private RexNode intLiteral490;
    
    /**
     * Initializes a new SargTest.
     *
     * @param testCaseName JUnit test case name
     */
    public SargTest(String testCaseName)
        throws Exception
    {
        super(testCaseName);
    }

    // implement TestCase
    public void setUp()
    {
        // create some reusable fixtures
        
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
        intType = typeFactory.createSqlType(SqlTypeName.Integer);
        
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        intLiteral7 = rexBuilder.makeExactLiteral(
            new BigDecimal(7), intType);
        intLiteral490 = rexBuilder.makeExactLiteral(
            new BigDecimal(490), intType);
        intLiteral8point5 = rexBuilder.makeExactLiteral(
            new BigDecimal("8.5"), intType);

        sargFactory = new SargFactory(rexBuilder);
    }
    
    public void testDefaultEndpoint()
    {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);
        assertEquals("-infinity", ep.toString());
    }
    
    public void testInfiniteEndpoint()
    {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);
        ep.setInfinity(1);
        assertEquals("+infinity", ep.toString());
        ep.setInfinity(-1);
        assertEquals("-infinity", ep.toString());
    }

    public void testFiniteEndpoint()
    {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);
        
        ep.setFinite(
            SargBoundType.LOWER,
            intLiteral7,
            true);
        assertEquals("> 7", ep.toString());
        
        ep.setFinite(
            SargBoundType.LOWER,
            intLiteral7,
            false);
        assertEquals(">= 7", ep.toString());
        
        ep.setFinite(
            SargBoundType.UPPER,
            intLiteral7,
            true);
        assertEquals("< 7", ep.toString());
        
        ep.setFinite(
            SargBoundType.UPPER,
            intLiteral7,
            false);
        assertEquals("<= 7", ep.toString());

        // after rounding, "> 8.5" is equivalent to ">= 9" over the domain
        // of integers
        ep.setFinite(
            SargBoundType.LOWER,
            intLiteral8point5,
            true);
        assertEquals(">= 9", ep.toString());
        
        ep.setFinite(
            SargBoundType.LOWER,
            intLiteral8point5,
            false);
        assertEquals(">= 9", ep.toString());

        ep.setFinite(
            SargBoundType.UPPER,
            intLiteral8point5,
            true);
        assertEquals("< 9", ep.toString());
        
        ep.setFinite(
            SargBoundType.UPPER,
            intLiteral8point5,
            true);
        assertEquals("< 9", ep.toString());
    }

    public void testNullEndpoint()
    {
        SargMutableEndpoint ep = sargFactory.newEndpoint(intType);

        ep.setFinite(SargBoundType.LOWER, sargFactory.newNullLiteral(), true);
        assertEquals("> null", ep.toString());
    }

    public void testTouchingEndpoint()
    {
        SargMutableEndpoint ep1 = sargFactory.newEndpoint(intType);
        SargMutableEndpoint ep2 = sargFactory.newEndpoint(intType);

        // "-infinity" does not touch "-infinity" (seems like something you
        // could argue for hours late at night in a college dorm)
        assertFalse(ep1.isTouching(ep2));
        
        // "< 7" does not touch "> 7"
        ep1.setFinite(
            SargBoundType.UPPER,
            intLiteral7,
            true);
        ep2.setFinite(
            SargBoundType.LOWER,
            intLiteral7,
            true);
        assertFalse(ep1.isTouching(ep2));
        assertTrue(ep1.compareTo(ep2) < 0);

        // "< 7" does touch ">= 7"
        ep2.setFinite(
            SargBoundType.LOWER,
            intLiteral7,
            false);
        assertTrue(ep1.isTouching(ep2));
        assertTrue(ep1.compareTo(ep2) < 0);

        // "<= 7" does touch ">= 7"
        ep1.setFinite(
            SargBoundType.LOWER,
            intLiteral7,
            false);
        assertTrue(ep1.isTouching(ep2));
        assertEquals(0, ep1.compareTo(ep2));

        // "<= 7" does not touch ">= 490"
        ep2.setFinite(
            SargBoundType.LOWER,
            intLiteral490,
            false);
        assertFalse(ep1.isTouching(ep2));
        assertTrue(ep1.compareTo(ep2) < 0);
    }

    public void testDefaultIntervalExpr()
    {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        assertEquals("(-infinity, +infinity)", interval.toString());
    }

    public void testPointExpr()
    {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setPoint(intLiteral7);
        assertTrue(interval.isPoint());
        assertFalse(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
        assertEquals("[7]", interval.toString());
        assertEquals("[7]", interval.evaluate().toString());
    }

    public void testRangeIntervalExpr()
    {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);

        interval.setLower(intLiteral7, false);
        interval.setUpper(intLiteral490, false);
        assertRange(interval);
        assertEquals("[7, 490]", interval.toString());
        assertEquals("[7, 490]", interval.evaluate().toString());

        interval.unsetLower();
        assertRange(interval);
        assertEquals("(-infinity, 490]", interval.toString());
        assertEquals("(null, 490]", interval.evaluate().toString());
        
        interval.setLower(intLiteral7, false);
        interval.unsetUpper();
        assertRange(interval);
        assertEquals("[7, +infinity)", interval.toString());
        assertEquals("[7, +infinity)", interval.evaluate().toString());

        interval.setUpper(intLiteral490, true);
        assertRange(interval);
        assertEquals("[7, 490)", interval.toString());
        assertEquals("[7, 490)", interval.evaluate().toString());

        interval.setLower(intLiteral7, true);
        assertRange(interval);
        assertEquals("(7, 490)", interval.toString());
        assertEquals("(7, 490)", interval.evaluate().toString());
    }

    private void assertRange(SargIntervalExpr interval)
    {
        assertFalse(interval.isPoint());
        assertFalse(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
    }

    public void testNullExpr()
    {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setNull();
        assertTrue(interval.isPoint());
        assertFalse(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
        assertEquals("[null] NULL_MATCHES_NULL", interval.toString());
        assertEquals("[null]", interval.evaluate().toString());
    }

    public void testEmptyExpr()
    {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setEmpty();
        assertTrue(interval.isEmpty());
        assertFalse(interval.isUnconstrained());
        assertEquals("()", interval.toString());
        assertEquals("()", interval.evaluate().toString());
    }

    public void testUnconstrainedExpr()
    {
        SargIntervalExpr interval = sargFactory.newIntervalExpr(intType);
        interval.setEmpty();
        assertFalse(interval.isUnconstrained());
        interval.setUnconstrained();
        assertTrue(interval.isUnconstrained());
        assertFalse(interval.isEmpty());
        assertEquals("(-infinity, +infinity)", interval.toString());
        assertEquals("(-infinity, +infinity)", interval.evaluate().toString());
    }

    public void testSetExpr()
    {
        SargIntervalExpr interval1 = sargFactory.newIntervalExpr(intType);
        SargIntervalExpr interval2 = sargFactory.newIntervalExpr(intType);

        interval1.setLower(intLiteral7, true);
        interval2.setUpper(intLiteral490, true);

        SargSetExpr intersectExpr = sargFactory.newSetExpr(
            intType, SargSetOperator.INTERSECTION);
        intersectExpr.addChild(interval1);
        intersectExpr.addChild(interval2);
        assertEquals(
            "INTERSECTION( (7, +infinity) (-infinity, 490) )",
            intersectExpr.toString());
        
        SargSetExpr unionExpr = sargFactory.newSetExpr(
            intType, SargSetOperator.UNION);
        unionExpr.addChild(interval1);
        unionExpr.addChild(interval2);
        assertEquals(
            "UNION( (7, +infinity) (-infinity, 490) )",
            unionExpr.toString());
        assertEquals(
            "(null, +infinity)",
            unionExpr.evaluate().toString());
        
        SargSetExpr complementExpr = sargFactory.newSetExpr(
            intType, SargSetOperator.COMPLEMENT);
        complementExpr.addChild(interval1);
        assertEquals(
            "COMPLEMENT( (7, +infinity) )",
            complementExpr.toString());
        assertEquals(
            "(-infinity, 7]",
            complementExpr.evaluate().toString());
    }
}

// End SargTest.java
