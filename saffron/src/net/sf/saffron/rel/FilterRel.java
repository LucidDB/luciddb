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

package net.sf.saffron.rel;

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;


/**
 * A <code>FilterRel</code> is a relational expression which iterates over its
 * input, and returns elements for which <code>condition</code> evaluates to
 * <code>true</code>.
 */
public class FilterRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    public RexNode condition;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a filter.
     *
     * @param cluster {@link VolcanoCluster} this relational expression
     *        belongs to
     * @param child input relational expression
     * @param condition boolean expression which determines whether a row is
     *        allowed to pass
     */
    public FilterRel(
        VolcanoCluster cluster,
        SaffronRel child,
        RexNode condition)
    {
        super(cluster,child);
        this.condition = condition;
    }

    //~ Methods ---------------------------------------------------------------

    public RexNode [] getChildExps()
    {
        return new RexNode [] { condition };
    }

    public Object clone()
    {
        return new FilterRel(
            cluster,
            OptUtil.clone(child),
            RexUtil.clone(condition));
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows = getRows();
        double dCpu = child.getRows();
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    // override SaffronRel
    public double getRows()
    {
        return child.getRows() * RexUtil.getSelectivity(condition);
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(this,new String [] { "child","condition" });
    }
}


// End FilterRel.java
