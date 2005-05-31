/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;

/**
 * <code>CalcRelBase</code> is an abstract base class for implementations
 * of {@link CalcRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CalcRelBase extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    public final RexNode [] projectExprs;
    public final RexNode conditionExpr;

    protected CalcRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelDataType rowType,
        RexNode [] projectExprs,
        RexNode conditionExpr)
    {
        super(cluster, traits, child);
        this.rowType = rowType;
        this.projectExprs = projectExprs;
        this.conditionExpr = conditionExpr;
    }
    
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = child.getRows();
        int nExprs = projectExprs.length;
        if (conditionExpr != null) {
            ++nExprs;
        }
        double dCpu = child.getRows() * nExprs;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public static void explainCalc(
        RelNode rel,
        RelOptPlanWriter pw,
        RexNode conditionExpr,
        RexNode [] projectExprs)
    {
        String [] terms = getExplainTerms(rel, projectExprs, conditionExpr);
        pw.explain(rel, terms, Util.emptyObjectArray);
    }

    private static String [] getExplainTerms(
        RelNode rel,
        RexNode [] projectExprs,
        RexNode conditionExpr)
    {
        ArrayList termList = new ArrayList(projectExprs.length + 2);
        termList.add("child");
        final RelDataTypeField [] fields = rel.getRowType().getFields();
        assert fields.length == projectExprs.length : "fields.length="
        + fields.length + ", projectExprs.length=" + projectExprs.length;
        for (int i = 0; i < fields.length; i++) {
            termList.add(fields[i].getName());
        }
        if (conditionExpr != null) {
            termList.add("condition");
        }
        final String [] terms =
            (String []) termList.toArray(new String[termList.size()]);
        return terms;
    }

    public RexNode [] getChildExps()
    {
        final ArrayList list = new ArrayList(Arrays.asList(projectExprs));
        if (conditionExpr != null) {
            list.add(conditionExpr);
        }
        return (RexNode []) list.toArray(new RexNode[list.size()]);
    }

    public void explain(RelOptPlanWriter pw)
    {
        explainCalc(this, pw, conditionExpr, projectExprs);
    }
}

// End CalcRelBase.java
