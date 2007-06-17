/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
import org.eigenbase.sql.*;


/**
 * SargFactory creates new instances of various sarg-related objects.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargFactory
{
    //~ Instance fields --------------------------------------------------------

    private final RexBuilder rexBuilder;

    private final RexNode rexNull;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SargFactory.
     *
     * @param rexBuilder factory for instances of {@link RexNode}, needed
     * internally in the sarg representation, and also for recomposing sargs
     * into equivalent rex trees
     */
    public SargFactory(RexBuilder rexBuilder)
    {
        this.rexBuilder = rexBuilder;
        rexNull = rexBuilder.constantNull();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a new endpoint. Initially, the endpoint represents a lower bound
     * of negative infinity.
     *
     * @param dataType datatype for domain
     *
     * @return new endpoint
     */
    public SargMutableEndpoint newEndpoint(RelDataType dataType)
    {
        return new SargMutableEndpoint(this, dataType);
    }

    /**
     * Creates a new interval expression. The interval starts out as unbounded
     * (meaning it includes every non-null value of the datatype), with
     * SqlNullSemantics.NULL_MATCHES_NOTHING.
     *
     * @param dataType datatype for domain
     */
    public SargIntervalExpr newIntervalExpr(RelDataType dataType)
    {
        return newIntervalExpr(
            dataType,
            SqlNullSemantics.NULL_MATCHES_NOTHING);
    }

    /**
     * Creates a new unbounded interval expression with non-default null
     * semantics.
     *
     * @param dataType datatype for domain
     * @param nullSemantics null semantics governing searches on this interval
     */
    public SargIntervalExpr newIntervalExpr(
        RelDataType dataType,
        SqlNullSemantics nullSemantics)
    {
        return new SargIntervalExpr(
            this,
            dataType,
            nullSemantics);
    }

    /**
     * Creates a new set expression, initially with no children.
     *
     * @param dataType datatype for domain
     * @param setOp set operator
     */
    public SargSetExpr newSetExpr(RelDataType dataType, SargSetOperator setOp)
    {
        return new SargSetExpr(this, dataType, setOp);
    }

    /**
     * @return new analyzer for rex expressions
     */
    public SargRexAnalyzer newRexAnalyzer()
    {
        return new SargRexAnalyzer(this, false);
    }

    /**
     * @param simpleMode if true, the analyzer restrictes the types of
     * predicates it allows; only one predicate is allowed per RexInputRef, and
     * only one range predicate is allowed
     *
     * @return new analyzer for rex expressions
     */
    public SargRexAnalyzer newRexAnalyzer(boolean simpleMode)
    {
        return new SargRexAnalyzer(this, simpleMode);
    }

    /**
     * @return the null literal, which can be used to represent a range matching
     * the null value
     */
    public RexNode newNullLiteral()
    {
        return rexNull;
    }

    /**
     * @return RexBuilder used by this factory
     */
    public RexBuilder getRexBuilder()
    {
        return rexBuilder;
    }
}

// End SargFactory.java
