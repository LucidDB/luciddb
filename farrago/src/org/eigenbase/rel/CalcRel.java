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

package org.eigenbase.rel;

import java.util.ArrayList;
import java.util.Arrays;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Util;


// TODO jvs 10-May-2004:  inherit from a new CalcRelBase
// REVIEW jvs 16-June-2004:  also, consider renaming this
// to ComputeRel, to avoid confusion with the Fennel calculator?

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link ProjectRel} and {@link FilterRel}. It should be created in the
 * latter stages of optimization, by merging consecutive {@link ProjectRel}
 * and {@link FilterRel} together.
 *
 * <p>The following rules relate to <code>CalcRel</code>:<ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link MergeFilterOntoCalcRule} merges this with a
 *     {@link FilterRel}</li>
 * <li>{@link MergeProjectOntoCalcRule} merges this with a
 *     {@link ProjectRel}</li>
 * </ul></p>
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public class CalcRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    public final RexNode [] _projectExprs;
    public final RexNode _conditionExpr;

    //~ Constructors ----------------------------------------------------------

    public CalcRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexNode [] projectExprs,
        RexNode conditionExpr)
    {
        super(cluster, child);
        this.rowType = rowType;
        _projectExprs = projectExprs;
        _conditionExpr = conditionExpr;
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new CalcRel(cluster, child, rowType, _projectExprs,
            _conditionExpr);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = child.getRows();
        int nExprs = _projectExprs.length;
        if (_conditionExpr != null) {
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
        final ArrayList list = new ArrayList(Arrays.asList(_projectExprs));
        if (_conditionExpr != null) {
            list.add(_conditionExpr);
        }
        return (RexNode []) list.toArray(new RexNode[list.size()]);
    }

    public void explain(RelOptPlanWriter pw)
    {
        explainCalc(this, pw, _conditionExpr, _projectExprs);
    }
}


// End CalcRel.java
