/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
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
import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.util.Util;

import java.util.ArrayList;
import java.util.Arrays;

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
public class CalcRel extends SingleRel {
    public final RexNode[] _projectExprs;
    public final RexNode _conditionExpr;

    public CalcRel(VolcanoCluster cluster, SaffronRel child,
            SaffronType rowType, RexNode[] projectExprs,
            RexNode conditionExpr) {
        super(cluster, child);
        this.rowType = rowType;
        _projectExprs = projectExprs;
        _conditionExpr = conditionExpr;
    }

    public Object clone() {
        return new CalcRel(cluster, child, rowType, _projectExprs,
                _conditionExpr);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows = child.getRows();
        int nExprs = _projectExprs.length;
        if (_conditionExpr != null) {
            ++nExprs;
        }
        double dCpu = child.getRows() * nExprs;
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public static void explainCalc(SaffronRel rel, PlanWriter pw,
            RexNode conditionExpr, RexNode[] projectExprs) {
        String[] terms = getExplainTerms(rel, projectExprs, conditionExpr);
        pw.explain(rel, terms, Util.emptyObjectArray);
    }

    private static String[] getExplainTerms(SaffronRel rel,
            RexNode[] projectExprs, RexNode conditionExpr) {
        ArrayList termList = new ArrayList(projectExprs.length + 2);
        termList.add("child");
        final SaffronField[] fields = rel.getRowType().getFields();
        assert fields.length == projectExprs.length :
                "fields.length=" + fields.length +
                ", projectExprs.length=" + projectExprs.length;
        for (int i = 0; i < fields.length; i++) {
            termList.add(fields[i].getName());
        }
        if (conditionExpr != null) {
            termList.add("condition");
        }
        final String[] terms = (String[])
                termList.toArray(new String[termList.size()]);
        return terms;
    }

    public RexNode[] getChildExps() {
        final ArrayList list = new ArrayList(Arrays.asList(_projectExprs));
        if (_conditionExpr != null) {
            list.add(_conditionExpr);
        }
        return (RexNode[]) list.toArray(new RexNode[list.size()]);
    }

    public void explain(PlanWriter pw) {
        explainCalc(this, pw, _conditionExpr, _projectExprs);
    }
}

// End CalcRel.java
