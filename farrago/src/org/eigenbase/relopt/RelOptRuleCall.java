/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package org.eigenbase.relopt;

import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.trace.*;


/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link RelOptRule}
 * with a set of {@link RelNode relational expression}s as arguments.
 */
public abstract class RelOptRuleCall
{
    //~ Static fields/initializers --------------------------------------------

    protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields -------------------------------------------------------

    public final RelOptRuleOperand operand0;
    public final RelOptRule rule;
    public final RelNode [] rels;
    public final RelOptPlanner planner;

    //~ Constructors ----------------------------------------------------------

    protected RelOptRuleCall(
        RelOptPlanner planner,
        RelOptRuleOperand operand,
        RelNode [] rels)
    {
        this.planner = planner;
        this.operand0 = operand;
        this.rule = operand.rule;
        this.rels = rels;
        assert (rels.length == rule.operands.length);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Called by the rule whenever it finds a match.
     */
    public abstract void transformTo(RelNode rel);
}


// End RelOptRuleCall.java
