/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

package net.sf.farrago.query;

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.rel.CalcRel;
import net.sf.saffron.rel.RelFieldCollation;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.util.Util;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * FennelCalcRel is the relational expression corresponding to a Calc
 * implemented inside of Fennel.
 *
 * <p>Rules:<ul>
 * <li>{@link FennelCalcRule} creates this from a
 *     {@link net.sf.saffron.rel.CalcRel}</li>
 * </ul></p>
 *
 * @author jhyde
 * @since Apr 8, 2004
 * @version $Id$
 */
public abstract class FennelCalcRel extends FennelSingleRel {
    //~ Instance fields -------------------------------------------------------
    private final RexNode[] _projectExprs;
    private final RexNode _conditionExpr;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelCalcRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param child rel producing rows to be Calced
     * @param rowType Row type
     * @param projectExprs Expressions returned by the calculator
     * @param conditionExpr Filter condition, may be null
     */
    protected FennelCalcRel(
            VolcanoCluster cluster,
            SaffronRel child,
            SaffronType rowType, RexNode[] projectExprs,
            RexNode conditionExpr) {
        super(cluster, child);
        Util.pre(rowType != null, "rowType != null");
        Util.pre(projectExprs != null, "projectExprs != null");
        _projectExprs = projectExprs;
        _conditionExpr = conditionExpr;
        this.rowType = rowType;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return expressions computed by this calculator
     */
    public RexNode [] getProjectExprs() {
        return _projectExprs;
    }

    /**
     * @return filter expression for this calculator, or null if unconditional
     */
    public RexNode getConditionExpr() {
        return _conditionExpr;
    }

    public RexNode[] getChildExps() {
        final ArrayList list = new ArrayList(Arrays.asList(_projectExprs));
        if (_conditionExpr != null) {
            list.add(_conditionExpr);
        }
        return (RexNode[]) list.toArray(new RexNode[list.size()]);
    }

    public void explain(PlanWriter pw) {
        CalcRel.explainCalc(this, pw, _conditionExpr, _projectExprs);
    }

    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner) {
        // TODO:  the real thing
        double rowCount = getRows();

        // NOTE jvs 26-July-2004: factor of 2 is to make sure cost always comes
        // out higher than IterCalcRel (making it at least deterministic until
        // we have proper costing, and giving preference to Java since it's
        // currently more reliable)
        
        return planner.makeCost(
            rowCount,
            rowCount * _projectExprs.length * 2,
            0);
    }

    public boolean isDistinct() {
        // todo: If child is distinct, and project-list is identity map,
        //   then this is distinct also
        return super.isDistinct();
    }

    // implement FennelRel
    public RelFieldCollation[] getCollations() {
        // todo: If child is sorted, and the project list is not too
        // destructive, then this output will be too. Some examples:
        //
        //   select x from (select x, y from t order by x, y)
        // is ordered by x
        //
        //   select f(x) from (select x, y from t order by x, y)
        // is clumped by x but not ordered by x
        //
        //   select y from (select x, y from t order by x, y)
        // is not ordered
        return super.getCollations();
    }
}

// End FennelCalcRel.java
