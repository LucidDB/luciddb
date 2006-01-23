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
     * Coordinate for this endpoint,
     * constrained to be either {@link RexLiteral} or {@link RexDynamicParam},
     * or null to represent infinity (positive or negative infinity is
     * implied by boundType).
     */
    protected RexNode coordinate;

    /**
     * @see getBoundType
     */
    protected SargBoundType boundType;

    /**
     * @see getStrictness
     */
    protected SargStrictness strictness;

    /**
     * @see SargFactory.newEndpoint
     */
    SargEndpoint(SargFactory factory, RelDataType dataType)
    {
        this.factory = factory;
        this.dataType = dataType;
        boundType = SargBoundType.LOWER;
        strictness = SargStrictness.OPEN;
    }

    void copyFrom(SargEndpoint other)
    {
        assert(getDataType() == other.getDataType());
        if (other.isFinite()) {
            setFinite(
                other.getBoundType(),
                other.getStrictness(),
                other.getCoordinate());
        } else {
            setInfinity(other.getInfinitude());
        }
    }
    
    /**
     * Sets this endpoint to either negative or positive infinity.  An infinite
     * endpoint implies an open bound (negative infinity implies a lower bound,
     * while positive infinity implies an upper bound).
     *
     * @param infinitude either -1 or +1
     */
    void setInfinity(int infinitude)
    {
        assert((infinitude == -1) || (infinitude == 1));

        if (infinitude == -1) {
            boundType = SargBoundType.LOWER;
        } else {
            boundType = SargBoundType.UPPER;
        }
        strictness = SargStrictness.OPEN;
        coordinate = null;
    }
    
    /**
     * Sets a finite value for this endpoint.
     *
     * @param boundType bound type (upper/lower)
     *
     * @param strictness boundary strictness
     *
     * @param coordinate endpoint position
     */
    void setFinite(
        SargBoundType boundType,
        SargStrictness strictness,
        RexNode coordinate)
    {
        // validate the input
        assert(coordinate != null);
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

        this.boundType = boundType;
        this.coordinate = coordinate;
        this.strictness = strictness;
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
                strictness = SargStrictness.CLOSED;
            } else {
                strictness = SargStrictness.OPEN;
            }
        } else if (boundType == SargBoundType.UPPER) {
            if (roundingCompensation > 0) {
                strictness = SargStrictness.CLOSED;
            } else {
                strictness = SargStrictness.OPEN;
            }
        }
    }

    /**
     * @return true if this endpoint represents a closed (exact) bound; false
     * if open (strict)
     */
    public boolean isClosed()
    {
        return strictness == SargStrictness.CLOSED;
    }

    /**
     * @return opposite of isClosed
     */
    public boolean isOpen()
    {
        return strictness == SargStrictness.OPEN;
    }

    /**
     * @return true if this endpoint represents infinity (either positive or
     * negative); false if a finite coordinate
     */
    public boolean isFinite()
    {
        return coordinate != null;
    }

    /**
     * @return -1 for negative infinity, +1 for positive infinity, 0 for
     * a finite endpoint
     */
    public int getInfinitude()
    {
        if (coordinate == null) {
            if (boundType == SargBoundType.LOWER) {
                return -1;
            } else {
                return 1;
            }
        } else {
            return 0;
        }
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
            isFinite()
            && other.isFinite()
            && (compareCoordinates(coordinate, other.coordinate) == 0)
            && (isClosed() || other.isClosed());
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
        if (coordinate == null) {
            if (boundType == SargBoundType.LOWER) {
                return "-infinity";
            } else {
                return "+infinity";
            }
        }
        StringBuilder sb = new StringBuilder();
        if (boundType == SargBoundType.LOWER) {
            if (isClosed()) {
                sb.append(">=");
            } else {
                sb.append(">");
            }
        } else {
            if (isClosed()) {
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
        return getStrictnessSign() - other.getStrictnessSign();
    }

    /**
     * @return SargStrictness of this bound
     */
    public SargStrictness getStrictness()
    {
        return strictness;
    }

    /**
     * @return complement of SargStrictness of this bound
     */
    public SargStrictness getStrictnessComplement()
    {
        return (strictness == SargStrictness.OPEN)
            ? SargStrictness.CLOSED
            : SargStrictness.OPEN;
    }

    /**
     * @return -1 for infinitesimally below (open upper bound,
     * strictly less than), 0 for exact equality (closed bound), 1 for
     * infinitesimally above (open lower bound, strictly greater than)
     */
    public int getStrictnessSign()
    {
        if (strictness == SargStrictness.CLOSED) {
            return 0;
        } else {
            if (boundType == SargBoundType.LOWER) {
                return 1;
            } else {
                return -1;
            }
        }
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
