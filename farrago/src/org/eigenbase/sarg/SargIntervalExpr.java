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

import java.util.*;

/**
 * SargIntervalExpr represents an expression which can be resolved to a
 * fixed {@link SargInterval}.
 *
 *<p>
 *
 * Null values require special treatment in expressions.  Normally, for
 * intervals of any kind, nulls are not considered to be within the
 * domain of search values.  This behavior can be modified by setting the
 * {@link SqlNullSemantics} to a value other than the default.  This happens
 * implicitly when a point interval is created matching the null value.  When
 * null values are considered to be part of the domain, the ordering is defined
 * as for {@link SargInterval}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargIntervalExpr extends SargIntervalBase implements SargExpr
{
    private SqlNullSemantics nullSemantics;
    
    /**
     * @see SargFactory.newIntervalExpr
     */
    SargIntervalExpr(
        SargFactory factory,
        RelDataType dataType,
        SqlNullSemantics nullSemantics)
    {
        super(factory, dataType);
        this.nullSemantics = nullSemantics;
    }

    /**
     * @return null semantics which apply for searches on this interval
     */
    public SqlNullSemantics getNullSemantics()
    {
        return nullSemantics;
    }

    // publicize SargIntervalBase
    public void setPoint(RexNode coordinate)
    {
        super.setPoint(coordinate);
        if (RexLiteral.isNullLiteral(coordinate)) {
            // since they explicitly asked for the null value as a point,
            // adjust the null semantics to match
            if (nullSemantics == SqlNullSemantics.NULL_MATCHES_NOTHING) {
                nullSemantics = SqlNullSemantics.NULL_MATCHES_NULL;
            }
        }
    }

    // publicize SargIntervalBase
    public void setNull()
    {
        super.setNull();
    }

    // publicize SargIntervalBase
    public void setLower(RexNode coordinate, SargStrictness strictness)
    {
        super.setLower(coordinate, strictness);
    }

    // publicize SargIntervalBase
    public void setUpper(RexNode coordinate, SargStrictness strictness)
    {
        super.setUpper(coordinate, strictness);
    }

    // publicize SargIntervalBase
    public void unsetLower()
    {
        super.unsetLower();
    }

    // publicize SargIntervalBase
    public void unsetUpper()
    {
        super.unsetUpper();
    }

    // publicize SargIntervalBase
    public void setUnconstrained()
    {
        super.setUnconstrained();
    }

    // publicize SargIntervalBase
    public void setEmpty()
    {
        super.setEmpty();
    }
    
    // implement SargExpr
    public String toString()
    {
        String s = super.toString();
        if (nullSemantics == SqlNullSemantics.NULL_MATCHES_NOTHING) {
            // default semantics, so omit them for brevity
            return s;
        } else {
            return s + " " + nullSemantics;
        }
    }
    
    // implement SargExpr
    public SargIntervalSequence evaluate()
    {
        SargIntervalSequence seq = new SargIntervalSequence();

        // If at least one of the bounds got flipped by overflow, the
        // result is empty.
        if ((lowerBound.getBoundType() != SargBoundType.LOWER) 
            || (upperBound.getBoundType() != SargBoundType.UPPER))
        {
            // empty sequence
            return seq;
        }

        // Under the default null semantics, if one of the endpoints is
        // known to be null, the result is empty.
        if ((nullSemantics == SqlNullSemantics.NULL_MATCHES_NOTHING)
            && (lowerBound.isNull() || upperBound.isNull()))
        {
            // empty sequence
            return seq;
        }

        // Copy the endpoints to the new interval.
        SargInterval interval = new SargInterval(factory, getDataType());
        interval.copyFrom(this);

        // Then adjust for null semantics.

        // REVIEW jvs 17-Jan-2006:  For unconstrained intervals, we
        // include null values.  Why is this?
        
        if ((nullSemantics == SqlNullSemantics.NULL_MATCHES_NOTHING)
            && getDataType().isNullable()
            && (lowerBound.isFinite() || upperBound.isFinite())
            && (!lowerBound.isFinite() || lowerBound.isNull()))
        {
            // The test above says that this is a constrained range
            // with no lower bound (or null for the lower bound).  Since nulls
            // aren't supposed to match anything, adjust the lower bound
            // to exclude null.
            interval.setLower(
                factory.newNullLiteral(),
                SargStrictness.OPEN);
        } else if (nullSemantics == SqlNullSemantics.NULL_MATCHES_ANYTHING) {
            if (!lowerBound.isFinite() || lowerBound.isNull()
                || upperBound.isNull())
            {
                // Since null is supposed to match anything, and it
                // is included in the interval, expand the interval to
                // match anything.
                interval.setUnconstrained();
            }
        }

        // NOTE jvs 27-Jan-2006: We don't currently filter out the empty
        // interval here because we rely on being able to create them
        // explicitly.  See related comment in FennelRelUtil.convertSargExpr;
        // if that changes, we could filter out the empty interval here.
        seq.addInterval(interval);
        
        return seq;
    }

    // implement SargExpr
    public void collectDynamicParams(Set<RexDynamicParam> dynamicParams)
    {
        if (lowerBound.getCoordinate() instanceof RexDynamicParam) {
            dynamicParams.add((RexDynamicParam) lowerBound.getCoordinate());
        }
        if (upperBound.getCoordinate() instanceof RexDynamicParam) {
            dynamicParams.add((RexDynamicParam) upperBound.getCoordinate());
        }
    }
}

// End SargIntervalExpr.java
