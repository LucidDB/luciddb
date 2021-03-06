/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.calc.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * FennelCalcRel is the relational expression corresponding to a Calc
 * implemented inside of Fennel.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link FennelCalcRule} creates this from a {@link
 * org.eigenbase.rel.CalcRel}</li>
 * </ul>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 * @since Apr 8, 2004
 */
public class FennelCalcRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final RexProgram program;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelCalcRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child rel producing rows to be Calced
     * @param rowType Row type
     * @param program Set of common expressions, projections, and optional
     * filter, to be calculated by the calculator
     */
    public FennelCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexProgram program)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            child);
        Util.pre(rowType != null, "rowType != null");
        Util.pre(program != null, "program != null");
        this.program = program;
        this.rowType = rowType;
        assert program.isValid(true);
        assert RelOptUtil.equal(
            "program's input type",
            program.getInputRowType(),
            "child's output type",
            child.getRowType(),
            true);
        // TODO: use stronger 'eq'
        assert RelOptUtil.equal(
            "program's output type",
            program.getOutputRowType(),
            "fennelCalcRel's output rowtype",
            rowType,
            true);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public FennelCalcRel clone()
    {
        FennelCalcRel clone =
            new FennelCalcRel(
                getCluster(),
                getChild().clone(),
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
            getChild(),
            program);
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
            RelMetadataQuery.getRowCount(getChild()) * exprCount * 2,
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
        // If child is sorted, and the project list is not too
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
        RelFieldCollation [] childCollation =
            ((FennelRel) getChild()).getCollations();
        int nChildFields = getChild().getRowType().getFieldCount();
        List<RelFieldCollation> retList = new ArrayList<RelFieldCollation>();
        List<RexLocalRef> projList = program.getProjectList();
        for (int i = 0; i < projList.size(); i++) {
            if (i >= childCollation.length) {
                break;
            }
            int projIdx = projList.get(i).getIndex();
            if ((projIdx >= nChildFields)
                || (projIdx != childCollation[i].getFieldIndex()))
            {
                break;
            }
            retList.add(new RelFieldCollation(i));
        }
        return retList.toArray(
            new RelFieldCollation[retList.size()]);
    }

    // for toStreamDef():
    protected void fillStreamDef(
        FemCalcTupleStreamDef calcStream,
        FennelRelImplementor implementor)
    {
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            calcStream);

        calcStream.setFilter(program.getCondition() != null);
        final RexToCalcTranslator translator =
            new RexToCalcTranslator(
                getCluster().getRexBuilder(),
                this);
        final String programString =
            translator.generateProgram(
                getChild().getRowType(),
                getProgram());
        calcStream.setProgram(programString);
        for (String dynamicParamIdStr : program.getCorrelVariableNames()) {
            final FemDynamicParamUse dynamicParamUse =
                implementor.getRepos().newFemDynamicParamUse();
            dynamicParamUse.setRead(true);
            final int dynamicParamId =
                RelOptQuery.getCorrelOrdinal(dynamicParamIdStr);
            dynamicParamUse.setDynamicParamId(dynamicParamId);
            calcStream.getDynamicParamUse().add(dynamicParamUse);
        }
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemCalcTupleStreamDef calcStream =
            implementor.getRepos().newFemCalcTupleStreamDef();
        fillStreamDef(calcStream, implementor);
        return calcStream;
    }
}

// End FennelCalcRel.java
