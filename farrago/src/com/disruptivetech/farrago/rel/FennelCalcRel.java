/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 The Eigenbase Project
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

package com.disruptivetech.farrago.rel;

import com.disruptivetech.farrago.calc.*;

import net.sf.farrago.query.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
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
public class FennelCalcRel extends FennelSingleRel
{
    //~ Instance fields -------------------------------------------------------

    private final RexProgram program;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelCalcRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child rel producing rows to be Calced
     * @param rowType Row type
     * @param program Set of common expressions, projections, and optional
     *   filter, to be calculated by the calculator
     */
    public FennelCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexProgram program)
    {
        super(cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION), child);
        Util.pre(rowType != null, "rowType != null");
        Util.pre(program != null, "program != null");
        this.program = program;
        this.rowType = rowType;
        assert program.isValid(true);
        assert RelOptUtil.equal(
            "program's input type", program.getInputRowType(),
            "child's output type", child.getRowType(), true);
        assert RelOptUtil.equal( // TODO: use stronger 'eq'
            "program's output type", program.getOutputRowType(),
            "fennelCalcRel's output rowtype", rowType,
            true);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        FennelCalcRel clone = new FennelCalcRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            rowType,
            program);
        clone.inheritTraitsFrom(this);
        return clone;
    }


    /**
     * @return Program
     */
    public RexProgram getProgram()
    {
        return program;
    }

    public void explain(RelOptPlanWriter pw)
    {
        program.explainCalc(this, pw);
    }

    public double getRows()
    {
        return FilterRel.estimateFilteredRows(
            getChild(), program.getCondition());
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  the real thing

        // NOTE jvs 26-July-2004: factor of 2 is to make sure cost always comes
        // out higher than IterCalcRel (making it at least deterministic until
        // we have proper costing, and giving preference to Java since it's
        // currently more reliable)
        int exprCount = program.getExprCount();
        return planner.makeCost(
            RelMetadataQuery.getRowCount(this),
            RelMetadataQuery.getRowCount(getChild()) * exprCount * 2, 0);
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

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemCalcTupleStreamDef calcStream =
            implementor.getRepos().newFemCalcTupleStreamDef();

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()), 
            calcStream);
        
        calcStream.setFilter(program.getCondition() != null);
        final RexToCalcTranslator translator =
            new RexToCalcTranslator(
                getCluster().getRexBuilder(), this);
        final String program = translator.generateProgram(
            getChild().getRowType(),
            getProgram());
        calcStream.setProgram(program);
        return calcStream;
    }
}


// End FennelCalcRel.java
