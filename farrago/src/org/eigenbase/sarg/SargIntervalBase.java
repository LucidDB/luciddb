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
import org.eigenbase.rex.*;


/**
 * SargIntervalBase is a common base for {@link SargInterval} and {@link
 * SargIntervalExpr}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SargIntervalBase
{
    //~ Instance fields --------------------------------------------------------

    protected final SargFactory factory;

    protected final SargMutableEndpoint lowerBound;

    protected final SargMutableEndpoint upperBound;

    //~ Constructors -----------------------------------------------------------

    /**
     * @see SargFactory#newIntervalExpr
     */
    SargIntervalBase(
        SargFactory factory,
        RelDataType dataType)
    {
        this.factory = factory;
        lowerBound = factory.newEndpoint(dataType);
        upperBound = factory.newEndpoint(dataType);
        unsetLower();
        unsetUpper();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return an immutable reference to the endpoint representing this
     * interval's lower bound
     */
    public SargEndpoint getLowerBound()
    {
        return lowerBound;
    }

    /**
     * @return an immutable reference to the endpoint representing this
     * interval's upper bound
     */
    public SargEndpoint getUpperBound()
    {
        return upperBound;
    }

    /**
     * @return whether this represents a single point
     */
    public boolean isPoint()
    {
        return lowerBound.isClosed() && upperBound.isClosed()
            && lowerBound.isTouching(upperBound);
    }

    /**
     * @return whether this represents the empty interval
     */
    public boolean isEmpty()
    {
        return !lowerBound.isClosed() && !upperBound.isClosed()
            && lowerBound.isNull() && upperBound.isNull();
    }

    /**
     * @return whether this represents a (non-empty, non-point) range interval
     */
    public boolean isRange()
    {
        return (!isPoint() && !isEmpty());
    }

    /**
     * @return whether this represents the universal set
     */
    public boolean isUnconstrained()
    {
        return !lowerBound.isFinite() && !upperBound.isFinite();
    }

    /**
     * @return the factory which produced this expression
     */
    public SargFactory getFactory()
    {
        return factory;
    }

    /**
     * Sets this interval to represent a single point (possibly the null value).
     *
     * @param coordinate coordinate of point to set, or null for the null value
     */
    void setPoint(RexNode coordinate)
    {
        setLower(coordinate, SargStrictness.CLOSED);
        setUpper(coordinate, SargStrictness.CLOSED);
    }

    /**
     * Sets this interval to represent a single point matching the null value.
     */
    void setNull()
    {
        setPoint(factory.newNullLiteral());
    }

    /**
     * Sets the lower bound for this interval.
     *
     * @param coordinate coordinate of point to set, must not be null
     * @param strictness strictness
     */
    void setLower(RexNode coordinate, SargStrictness strictness)
    {
        lowerBound.setFinite(
            SargBoundType.LOWER,
            strictness,
            coordinate);
    }

    /**
     * Sets the upper bound for this interval.
     *
     * @param coordinate coordinate of point to set
     * @param strictness boundary strictness
     */
    void setUpper(RexNode coordinate, SargStrictness strictness)
    {
        upperBound.setFinite(
            SargBoundType.UPPER,
            strictness,
            coordinate);
    }

    /**
     * Removes the lower bound for this interval, setting it to -infinity.
     */
    void unsetLower()
    {
        lowerBound.setInfinity(-1);
    }

    /**
     * Removes the upper bound for this interval, setting it to +infinity.
     */
    void unsetUpper()
    {
        upperBound.setInfinity(1);
    }

    /**
     * Sets this interval to unconstrained (matching everything, including
     * null).
     */
    void setUnconstrained()
    {
        unsetLower();
        unsetUpper();
    }

    /**
     * Sets this interval to empty (matching nothing at all).
     */
    void setEmpty()
    {
        setLower(
            factory.newNullLiteral(),
            SargStrictness.OPEN);
        setUpper(
            factory.newNullLiteral(),
            SargStrictness.OPEN);
    }

    public RelDataType getDataType()
    {
        return lowerBound.getDataType();
    }

    // implement SargExpr
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (lowerBound.isClosed()) {
            sb.append("[");
        } else {
            sb.append("(");
        }

        if (!isEmpty()) {
            printBound(sb, lowerBound);

            if (isPoint()) {
                // point has both endpoints same; don't repeat
            } else {
                sb.append(", ");
                printBound(sb, upperBound);
            }
        }

        if (upperBound.isClosed()) {
            sb.append("]");
        } else {
            sb.append(")");
        }

        return sb.toString();
    }

    private void printBound(StringBuilder sb, SargEndpoint endpoint)
    {
        if (endpoint.isFinite()) {
            sb.append(endpoint.getCoordinate().toString());
        } else {
            sb.append(endpoint);
        }
    }
}

// End SargIntervalBase.java
