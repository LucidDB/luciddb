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
package org.eigenbase.sarg;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

import java.util.*;
import java.math.*;

/**
 * SargEndpoint represents an endpoint of a ({@link SargInterval}).
 *
 *<p>
 *
 * Instances of SargEndpoint are immutable from outside this package.
 * Subclass {@link SargMutableEndpoint} is provided for manipulation
 * from outside the package.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargEndpoint implements Comparable<SargEndpoint>
{
    // TODO jvs 16-Jan-2006:  special pattern prefix support for LIKE operator

    /**
     * Factory which produced this endpoint.
     */
    protected final SargFactory factory;
    
    /**
     * Datatype for endpoint value.
     */
    protected final RelDataType dataType;
    
    /**
     * If non-zero, coordinate is null.
     *
     * @see getInfinitude
     */
    protected int infinitude;
    
    /**
     * Coordinate for this endpoint,
     * constrained to be either {@link RexLiteral} or {@link RexDynamicParam}.
     */
    protected RexNode coordinate;

    /**
     * @see getBoundType
     */
    protected SargBoundType boundType;

    /**
     * Strictness before adjustment for rounding.
     *
     * @see getStrictness
     */
    protected int strictness;

    /**
     * @see SargFactory.newEndpoint
     */
    SargEndpoint(SargFactory factory, RelDataType dataType)
    {
        this.factory = factory;
        this.dataType = dataType;
        boundType = SargBoundType.LOWER;
        strictness = 0;
        infinitude = -1;
    }

    void copyFrom(SargEndpoint other)
    {
        assert(getDataType() == other.getDataType());
        if (other.isFinite()) {
            setFinite(
                boundType,
                other.getCoordinate(),
                !other.isExact());
        } else {
            setInfinity(other.getInfinitude());
        }
    }
    
    /**
     * @see SargMutableEndpoint.setInfinity
     */
    void setInfinity(int infinitude)
    {
        assert((infinitude == -1) || (infinitude == 1));

        if (infinitude == -1) {
            boundType = SargBoundType.LOWER;
            strictness = 1;
        } else {
            boundType = SargBoundType.UPPER;
            strictness = -1;
        }
        this.infinitude = infinitude;
        coordinate = null;
    }

    /**
     * @see SargMutableEndpoint.setFinite
     */
    void setFinite(
        SargBoundType boundType,
        RexNode coordinate,
        boolean strict)
    {
        // validate the input
        if (coordinate instanceof RexDynamicParam) {
            // REVIEW jvs 16-Jan-2006:  may need to ignore nullability
            assert(coordinate.getType().equals(dataType));
        } else {
            assert(coordinate instanceof RexLiteral);
            RexLiteral literal = (RexLiteral) coordinate;
            if (!RexLiteral.isNullLiteral(literal)) {
                assert(SqlTypeUtil.canAssignFrom(
                           dataType,
                           literal.getType()));
            }
        }

        infinitude = 0;
        this.boundType = boundType;
        this.coordinate = coordinate;
        strictness = !strict ? 0
            : ((boundType == SargBoundType.LOWER) ? 1 : -1);
        applyRounding();
    }

    private void applyRounding()
    {
        if (!(coordinate instanceof RexLiteral)) {
            return;
        }
        RexLiteral literal = (RexLiteral) coordinate;
        
        if (!(literal.getValue() instanceof BigDecimal)) {
            return;
        }

        // For numbers, we have to deal with rounding fun and
        // games.
        
        if (SqlTypeUtil.isApproximateNumeric(dataType)) {
            // REVIEW jvs 18-Jan-2006:  is it necessary to do anything
            // for approx types here?  Wait until someone complains.
            return;
        }
        
        // NOTE: defer overflow checks until cast execution.  Broadbase did it
        // here, but the effect should be the same.  Really, instead of
        // overflowing at all, right here we should convert the interval to
        // either unconstrained or empty (e.g. "less than overflow value" is
        // equivalent to "less than +infinity").
        
        BigDecimal bd = (BigDecimal) literal.getValue();
        BigDecimal bdRounded = bd.setScale(
            dataType.getScale(),
            RoundingMode.HALF_UP);
        coordinate = factory.getRexBuilder().makeExactLiteral(bdRounded);

        // The sign of roundingCompensation should be the opposite of the
        // rounding direction, so subtract post-rounding value from
        // pre-rounding.
        int roundingCompensation = bd.compareTo(bdRounded);

        // rounding takes precedence over the strictness flag.
        //  Input        round    strictness    output    effective strictness
        //    >5.9        down            -1       >=6        0
        //    >=5.9       down             0       >=6        0
        //    >6.1          up            -1        >6        1
        //    >=6.1         up             0        >6        1
        //    <6.1          up             1       <=6        0
        //    <=6.1         up             0       <=6        0
        //    <5.9        down             1        <6       -1
        //    <=5.9       down             0        <6       -1
        if (roundingCompensation == 0) {
            return;
        }
        if (boundType == SargBoundType.LOWER) {
            if (roundingCompensation < 0) {
                strictness = 0;
            } else {
                strictness = 1;
            }
        } else if (boundType == SargBoundType.UPPER) {
            if (roundingCompensation > 0) {
                strictness = 0;
            } else {
                strictness = -1;
            }
        }
    }

    /**
     * @return the boundary strictness: -1 for infinitesimally below (open
     * upper bound, strictly less than), 0 for exact equality (closed
     * bound), 1 for infinitesimally above (open lower bound, strictly greater
     * than)
     */
    public int getStrictness()
    {
        return strictness;
    }

    /**
     * @return true if this endpoint represents an exact (closed) bound; false
     * if inexact (open)
     */
    public boolean isExact()
    {
        return strictness == 0;
    }

    /**
     * @return true if this endpoint represents infinity (either positive or
     * negative); false if a finite coordinate
     */
    public boolean isFinite()
    {
        return infinitude == 0;
    }

    /**
     * @return -1 for negative infinity, +1 for positive infinity, 0 for
     * a finite endpoint
     */
    public int getInfinitude()
    {
        return infinitude;
    }

    /**
     * @return coordinate of this endpoint
     */
    public RexNode getCoordinate()
    {
        return coordinate;
    }

    /**
     * @return true if this endpoint has the null value for its coordinate
     */
    public boolean isNull()
    {
        if (!isFinite()) {
            return false;
        }
        return RexLiteral.isNullLiteral(coordinate);
    }

    /**
     * @return target datatype for coordinate
     */
    public RelDataType getDataType()
    {
        return dataType;
    }

    /**
     * @return boundary type this endpoint represents
     */
    public SargBoundType getBoundType()
    {
        return boundType;
    }

    /**
     * Tests whether this endpoint "touches" another one (not necessarily
     * overlapping).  For example, the upper bound of the interval (1, 10)
     * touches the lower bound of the interval [10, 20), but not
     * of the interval (10, 20).
     *
     *<p>
     *
     * This method will assert if called on an endpoint defined
     * by a dynamic parameter.  REVIEW:  maybe move it elsewhere
     * to prevent this possibility, or make it non-public.
     *
     * @param other the other endpoint to test
     *
     * @return true if touching; false if discontinuous
     */
    public boolean isTouching(SargEndpoint other)
    {
        assert(getDataType() == other.getDataType());
        return
            (infinitude == 0)
            && (other.infinitude == 0)
            && (compareCoordinates(coordinate, other.coordinate) == 0)
            && ((getStrictness() == 0) || (other.getStrictness() == 0));
    }

    static int compareCoordinates(RexNode coord1, RexNode coord2)
    {
        assert(coord1 instanceof RexLiteral);
        assert(coord2 instanceof RexLiteral);
        
        // null values always sort lowest
        boolean isNull1 = RexLiteral.isNullLiteral(coord1);
        boolean isNull2 = RexLiteral.isNullLiteral(coord2);
        if (isNull1 && isNull2) {
            return 0;
        } else if (isNull1) {
            return -1;
        } else if (isNull2) {
            return 1;
        } else {
            RexLiteral lit1 = (RexLiteral) coord1;
            RexLiteral lit2 = (RexLiteral) coord2;
            return lit1.getValue().compareTo(lit2.getValue());
        }
    }

    // implement Object
    public String toString()
    {
        if (infinitude == -1) {
            return "-infinity";
        } else if (infinitude == 1) {
            return "+infinity";
        }
        StringBuilder sb = new StringBuilder();
        if (boundType == SargBoundType.LOWER) {
            if (getStrictness() == 0) {
                sb.append(">=");
            } else {
                sb.append(">");
            }
        } else {
            if (getStrictness() == 0) {
                sb.append("<=");
            } else {
                sb.append("<");
            }
        }
        sb.append(" ");
        sb.append(coordinate);
        return sb.toString();
    }

    // implement Comparable
    public int compareTo(SargEndpoint other)
    {
        if (getInfinitude() != other.getInfinitude()) {
            // at least one is infinite; result is based on comparison of
            // infinitudes
            return getInfinitude() - other.getInfinitude();
        }

        if (!isFinite()) {
            // both are the same infinity:  equals
            return 0;
        }

        // both are finite:  compare coordinates
        int c = compareCoordinates(
            getCoordinate(), other.getCoordinate());

        if (c != 0) {
            return c;
        }

        // if coordinates are the same, then result is based on comparison of
        // strictness
        return getStrictness() - other.getStrictness();
    }

    // override Object
    public boolean equals(Object other)
    {
        if (!(other instanceof SargEndpoint)) {
            return false;
        }
        return compareTo((SargEndpoint) other) == 0;
    }

    // override Object
    public int hashCode()
    {
        return toString().hashCode();
    }
}

// End SargEndpoint.java
