/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.core;

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.trace.SaffronTrace;

import java.util.logging.Logger;


/**
 * A <code>SaffronPlanner</code> is a query optimizer: it transforms a
 * relational expression into a semantically equivalent relational
 * expression, according to a given set of rules and a cost model.
 */
public interface SaffronPlanner
{
    static final Logger tracer = SaffronTrace.getPlannerTracer();

    //~ Methods ---------------------------------------------------------------

    void setRoot(SaffronRel rel);

    SaffronRel getRoot();

    /**
     * Registers a calling convention.
     */
    void addCallingConvention(CallingConvention convention);

    /**
     * Registers a rule.
     */
    void addRule(VolcanoRule rule);

    /**
     * Changes a relational expression to an equivalent one of a different
     * calling convention. The return is never null, but may be abstract.
     *
     * @pre rel.getConvention() != toConvention
     * @post return != null
     */
    SaffronRel changeConvention(SaffronRel rel,CallingConvention toConvention);

    /**
     * Negotiates an appropriate planner to deal with distributed queries.
     * The idea is that the schemas decide among themselves which has the
     * most knowledge.  Right now, the local planner retains control.
     */
    SaffronPlanner chooseDelegate();

    /**
     * Find the most efficient expression to implement this query.
     */
    SaffronRel findBestExp();

    /**
     * Create a cost object.
     */
    PlanCost makeCost(double dRows,double dCpu,double dIo);

    /**
     * Create a cost object representing an enormous non-infinite cost.
     */
    PlanCost makeHugeCost();

    /**
     * Create a cost object representing infinite cost.
     */
    PlanCost makeInfiniteCost();

    /**
     * Create a cost object representing a small positive cost.
     */
    PlanCost makeTinyCost();

    /**
     * Create a cost object representing zero cost.
     */
    PlanCost makeZeroCost();

    /**
     * Registers a relational expression in the expression bank. After it has
     * been registered, you may not modify it.
     *
     * @param rel Relational expression to register
     * @param equivRel Relational expression it is equivalent to (may be null)
     *
     * @return the same expression, or an equivalent existing expression
     */
    SaffronRel register(SaffronRel rel,SaffronRel equivRel);

    /**
     * Tells this planner that a schema exists. This is the schema's chance to
     * tell the planner about all of the special transformation rules.
     */
    void registerSchema(SaffronSchema schema);
}


// End SaffronPlanner.java
