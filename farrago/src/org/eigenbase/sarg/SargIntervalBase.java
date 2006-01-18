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
import org.eigenbase.sql.*;
import org.eigenbase.rex.*;

/**
 * SargIntervalBase is a common base for {@link SargInterval} and
 * {@link SargIntervalExpr}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SargIntervalBase
{
    protected final SargFactory factory;
    
    protected final SargMutableEndpoint lowerBound;
    
    protected final SargMutableEndpoint upperBound;

    /**
     * @see SargFactory.newIntervalExpr
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
        return lowerBound.isExact() && upperBound.isExact()
            && lowerBound.isTouching(upperBound);
    }

    /**
     * @return whether this represents the empty interval
     */
    public boolean isEmpty()
    {
        return !lowerBound.isExact() && !upperBound.isExact()
            && lowerBound.isNull() && upperBound.isNull();
    }
    
    /**
     * @return whether this represents the universal set
     */
    public boolean isUnconstrained()
    {
        return !lowerBound.isFinite() && !upperBound.isFinite();
    }
    
    /**
     * @see SargIntervalExpr.setPoint
     */
    void setPoint(RexNode coordinate)
    {
        setLower(coordinate, false);
        setUpper(coordinate, false);
    }

    /**
     * @see SargIntervalExpr.setNull
     */
    void setNull()
    {
        setPoint(factory.newNullLiteral());
    }

    /**
     * @see SargIntervalExpr.setLower
     */
    void setLower(RexNode coordinate, boolean strict)
    {
        lowerBound.setFinite(
            SargBoundType.LOWER,
            coordinate,
            strict);
    }

    /**
     * @see SargIntervalExpr.setUpper
     */
    void setUpper(RexNode coordinate, boolean strict)
    {
        upperBound.setFinite(
            SargBoundType.UPPER,
            coordinate,
            strict);
    }

    /**
     * @see SargIntervalExpr.unsetLower
     */
    void unsetLower()
    {
        lowerBound.setInfinity(-1);
    }

    /**
     * @see SargIntervalExpr.unsetUpper
     */
    void unsetUpper()
    {
        upperBound.setInfinity(1);
    }

    /**
     * @see SargIntervalExpr.setUnconstrained
     */
    void setUnconstrained()
    {
        unsetLower();
        unsetUpper();
    }

    /**
     * @see SargIntervalExpr.setEmpty
     */
    void setEmpty()
    {
        setLower(factory.newNullLiteral(), true);
        setUpper(factory.newNullLiteral(), true);
    }

    // implement SargExpr
    public RelDataType getDataType()
    {
        return lowerBound.getDataType();
    }
    
    // implement SargExpr
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (lowerBound.isExact()) {
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
        
        if (upperBound.isExact()) {
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
