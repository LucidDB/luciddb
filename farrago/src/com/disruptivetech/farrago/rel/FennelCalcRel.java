/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package com.disruptivetech.farrago.rel;

import java.util.ArrayList;
import java.util.Arrays;

import net.sf.farrago.query.*;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Util;


/**
 * FennelCalcRel is the relational expression corresponding to a Calc
 * implemented inside of Fennel.
 *
 * <p>Rules:<ul>
 * <li>{@link FennelCalcRule} creates this from a
 *     {@link org.eigenbase.rel.CalcRel}</li>
 * </ul></p>
 *
 * @author jhyde
 * @since Apr 8, 2004
 * @version $Id$
 */
public abstract class FennelCalcRel extends FennelSingleRel
{
    //~ Instance fields -------------------------------------------------------

    private final RexNode [] projectExprs;
    private final RexNode conditionExpr;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelCalcRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child rel producing rows to be Calced
     * @param rowType Row type
     * @param projectExprs Expressions returned by the calculator
     * @param conditionExpr Filter condition, may be null
     */
    protected FennelCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexNode [] projectExprs,
        RexNode conditionExpr)
    {
        super(cluster, child);
        Util.pre(rowType != null, "rowType != null");
        Util.pre(projectExprs != null, "projectExprs != null");
        this.projectExprs = projectExprs;
        this.conditionExpr = conditionExpr;
        this.rowType = rowType;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return expressions computed by this calculator
     */
    public RexNode [] getProjectExprs()
    {
        return projectExprs;
    }

    /**
     * @return filter expression for this calculator, or null if unconditional
     */
    public RexNode getConditionExpr()
    {
        return conditionExpr;
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
        CalcRel.explainCalc(this, pw, conditionExpr, projectExprs);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  the real thing
        double rowCount = getRows();

        // NOTE jvs 26-July-2005: factor of 2 is to make sure cost always comes
        // out higher than IterCalcRel (making it at least deterministic until
        // we have proper costing, and giving preference to Java since it's
        // currently more reliable)
        return planner.makeCost(rowCount, rowCount * projectExprs.length * 2,
            0);
    }

    public boolean isDistinct()
    {
        // todo: If child is distinct, and project-list is identity map,
        //   then this is distinct also
        return super.isDistinct();
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
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
