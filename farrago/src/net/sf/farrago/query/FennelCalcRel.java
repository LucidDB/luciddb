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

import net.sf.farrago.fem.fennel.FemCalcTupleStreamDef;
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.CalcRel;
import net.sf.saffron.rel.RelFieldCollation;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
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
public class FennelCalcRel extends FennelSingleRel {
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
    public FennelCalcRel(
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

    public Object clone() {
        return new FennelCalcRel(
                cluster, OptUtil.clone(child), rowType,
                RexUtil.clone(_projectExprs), RexUtil.clone(_conditionExpr));
    }

    public CallingConvention getConvention() {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    protected SaffronType deriveRowType() {
        return rowType;
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
        double bytesPerRow = 1;
        return planner.makeCost(
                rowCount,
                rowCount,
                rowCount * bytesPerRow);
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FarragoRelImplementor implementor) {
        FemCalcTupleStreamDef calcStream =
                getCatalog().newFemCalcTupleStreamDef();

        // TODO jvs 8-May-2004: account for scratch page in provisioning, not
        // here
        calcStream.setCachePageMin(1);
        calcStream.setCachePageMax(1);

        calcStream.getInput().add(
                implementor.implementFennelRel(child));
        calcStream.setFilter(_conditionExpr != null);
        final CalcRelImplementor.Rex2CalcTranslator translator =
                new CalcRelImplementor(cluster.rexBuilder)
                .newTranslator(_projectExprs, _conditionExpr);
        final String program = translator.getProgram(child.getRowType());
        calcStream.setProgram(program);
        return calcStream;
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
