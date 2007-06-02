/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


/**
 * FennelReshapeRel represents the Fennel implementation of an execution stream
 * that does projections, simple casting, and simple filtering.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FennelReshapeRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    Integer [] projection;
    RelDataType outputRowType;
    CompOperatorEnum compOp;
    Integer [] filterOrdinals;
    List<RexLiteral> literals;
    RelDataType filterRowType;

    RexNode condition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelReshapeRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child child input
     * @param projection ordinals of the columns to be projected from the input
     * @param outputRowType row type of the output
     * @param compOp comparison operator
     * @param filterOrdinals ordinals corresponding to filter inputs
     * @param literals list of literals to be used in the filtering
     * @param filterRowType row type corresponding to the filter columns
     */
    public FennelReshapeRel(
        RelOptCluster cluster,
        RelNode child,
        Integer [] projection,
        RelDataType outputRowType,
        CompOperatorEnum compOp,
        Integer [] filterOrdinals,
        List<RexLiteral> literals,
        RelDataType filterRowType)
    {
        super(cluster, child);
        this.projection = projection;
        this.outputRowType = outputRowType;
        this.compOp = compOp;
        this.filterOrdinals = filterOrdinals;
        this.literals = literals;
        this.filterRowType = filterRowType;

        condition = null;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelReshapeRel clone()
    {
        FennelReshapeRel clone =
            new FennelReshapeRel(
                getCluster(),
                getChild().clone(),
                projection,
                outputRowType,
                compOp,
                filterOrdinals,
                new ArrayList<RexLiteral>(literals),
                filterRowType);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = RelMetadataQuery.getRowCount(this);

        // multiple by .5 so this calc comes out smaller than both java
        // calc and fennel calc
        double dCpu =
            RelMetadataQuery.getRowCount(getChild())
            * (projection.length + filterOrdinals.length) * .5;

        return planner.makeCost(
            dRows,
            dCpu,
            0);
    }

    // implement RelNode
    public double getRows()
    {
        // reconstruct a RexNode filter based on the filter ordinals and
        // the literals they're being compared against
        if ((condition == null) && (filterOrdinals.length > 0)) {
            RexBuilder rexBuilder = getCluster().getRexBuilder();
            RelDataTypeField [] fields = getChild().getRowType().getFields();
            List<RexNode> filterList = new ArrayList<RexNode>();
            for (int i = 0; i < filterOrdinals.length; i++) {
                RexNode input =
                    rexBuilder.makeInputRef(
                        fields[filterOrdinals[i]].getType(),
                        filterOrdinals[i]);
                filterList.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.equalsOperator,
                        input,
                        literals.get(i)));
            }
            condition = RexUtil.andRexNodeList(rexBuilder, filterList);
        }

        return FilterRel.estimateFilteredRows(
            getChild(),
            condition);
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        if (filterOrdinals.length == 0) {
            pw.explain(
                this,
                new String[] { "child", "projection", "outputRowType" },
                new Object[] {
                    Arrays.asList(projection),
                    outputRowType.getFullTypeString()
                });
        } else {
            // need to include the output rowtype in the digest to properly
            // handle casting
            pw.explain(
                this,
                new String[] {
                    "child",
                    "projection",
                    "filterOp",
                    "filterOrdinals",
                    "filterTuple",
                    "outputRowType"
                },
                new Object[] {
                    Arrays.asList(projection),
                    compOp.toString(),
                    Arrays.asList(filterOrdinals),
                    literals.toString(),
                    outputRowType.getFullTypeString()
                });
        }
    }

    public RelDataType deriveRowType()
    {
        return outputRowType;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemReshapeStreamDef streamDef = repos.newFemReshapeStreamDef();

        FemExecutionStreamDef childInput =
            implementor.visitFennelChild((FennelRel) getChild());
        implementor.addDataFlowFromProducerToConsumer(
            childInput,
            streamDef);

        streamDef.setCompareOp(compOp);

        streamDef.setOutputProjection(
            FennelRelUtil.createTupleProjection(repos, projection));
        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getRexBuilder().getTypeFactory(),
                outputRowType));

        streamDef.setInputCompareProjection(
            FennelRelUtil.createTupleProjection(repos, filterOrdinals));

        if (filterOrdinals.length == 0) {
            streamDef.setTupleCompareBytesBase64("");
        } else {
            List<List<RexLiteral>> compareTuple =
                new ArrayList<List<RexLiteral>>();
            compareTuple.add(literals);
            streamDef.setTupleCompareBytesBase64(
                FennelRelUtil.convertTuplesToBase64String(
                    filterRowType,
                    compareTuple));
        }

        return streamDef;
    }
}

// End FennelReshapeRel.java
