/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;


/**
 * FennelReshapeRel represents the Fennel implementation of an execution stream
 * that does projections, simple casting, and simple filtering. Filtering can
 * done against either literal values passed in through a stream parameter, or
 * against dynamic parameters read during runtime.
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
    List<RexLiteral> filterLiterals;
    FennelRelParamId [] dynamicParamIds;
    Integer [] paramCompareOffsets;
    BitSet paramOutput;

    RexNode condition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelReshapeRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child child input
     * @param projection ordinals of the columns to be projected from the input
     * @param outputRowType row type of the output (includes dynamic parameters
     * that are to be outputted) with dynamic parameters appearing at the end of
     * the row
     * @param compOp comparison operator
     * @param filterOrdinals ordinals corresponding to inputs that need to be
     * filtered; they're filtered against either literals or dynamic parameters;
     * in the case where dynamic parameters are being compared, the trailing
     * ordinals represent the columns to be compared against the parameters
     * @param filterLiterals list of literals to be compared against the leading
     * columns specified by filterOrdinals
     * @param dynamicParamIds dynamic parameters to be read by this rel
     * @param paramCompareOffsets array of offsets within the input tuple that
     * each dynamic parameter should be compared against; if the dynamic
     * parameter doesn't need to be compared, then the offset is set to -1
     * @param paramOutput bitset indicating whether each dynamic parameter
     * should be outputted
     */
    public FennelReshapeRel(
        RelOptCluster cluster,
        RelNode child,
        Integer [] projection,
        RelDataType outputRowType,
        CompOperatorEnum compOp,
        Integer [] filterOrdinals,
        List<RexLiteral> filterLiterals,
        FennelRelParamId [] dynamicParamIds,
        Integer [] paramCompareOffsets,
        BitSet paramOutput)
    {
        super(cluster, child);
        this.projection = projection;
        this.outputRowType = outputRowType;
        this.compOp = compOp;
        this.filterOrdinals = filterOrdinals;
        this.filterLiterals = filterLiterals;
        this.dynamicParamIds = dynamicParamIds;
        this.paramCompareOffsets = paramCompareOffsets;
        this.paramOutput = paramOutput;

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
                new ArrayList<RexLiteral>(filterLiterals),
                dynamicParamIds,
                paramCompareOffsets,
                paramOutput);
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
        // Reconstruct a RexNode filter containing all the filtering
        // conditions
        if (condition == null) {
            RexBuilder rexBuilder = getCluster().getRexBuilder();
            RelDataTypeField [] fields = getChild().getRowType().getFields();
            List<RexNode> filterList = new ArrayList<RexNode>();

            if (filterLiterals.size() > 0) {
                assert (filterLiterals.size() == filterOrdinals.length);
                for (int i = 0; i < filterOrdinals.length; i++) {
                    addFilter(
                        filterOrdinals[i],
                        fields,
                        compOp,
                        filterLiterals.get(i),
                        (i == (filterOrdinals.length - 1)),
                        filterList,
                        rexBuilder);
                }
            }

            int nFilterParams = 0;
            for (int i = 0; i < paramCompareOffsets.length; i++) {
                if (paramCompareOffsets[i] >= 0) {
                    nFilterParams++;
                }
            }
            assert ((nFilterParams + filterLiterals.size())
                == filterOrdinals.length);

            // For the filters being compared to dynamic parameters, just
            // use placeholder dynamic parameters in the filter expression
            if (nFilterParams > 0) {
                int count = 0;
                for (int i = 0; i < paramCompareOffsets.length; i++) {
                    if (paramCompareOffsets[i] >= 0) {
                        count++;
                        addFilter(
                            paramCompareOffsets[i],
                            fields,
                            compOp,
                            rexBuilder.makeDynamicParam(
                                fields[paramCompareOffsets[i]].getType(),
                                0),
                            (count == nFilterParams),
                            filterList,
                            rexBuilder);
                    }
                }
            }
            condition = RexUtil.andRexNodeList(rexBuilder, filterList);
        }

        return FilterRel.estimateFilteredRows(
            getChild(),
            condition);
    }

    private void addFilter(
        Integer filterOrdinal,
        RelDataTypeField [] fields,
        CompOperatorEnum compOp,
        RexNode filterOperand,
        boolean lastFilter,
        List<RexNode> filterList,
        RexBuilder rexBuilder)
    {
        RexNode input =
            rexBuilder.makeInputRef(
                fields[filterOrdinal].getType(),
                filterOrdinal);

        SqlBinaryOperator sqlOp = null;

        // The comparison operator is really only relevant to the last
        // filter.  All preceeding filters are always equality.
        if (!lastFilter || (compOp == CompOperatorEnum.COMP_EQ)) {
            sqlOp = SqlStdOperatorTable.equalsOperator;
        } else if (compOp == CompOperatorEnum.COMP_GE) {
            sqlOp = SqlStdOperatorTable.greaterThanOrEqualOperator;
        } else if (compOp == CompOperatorEnum.COMP_GT) {
            sqlOp = SqlStdOperatorTable.greaterThanOperator;
        } else if (compOp == CompOperatorEnum.COMP_LE) {
            sqlOp = SqlStdOperatorTable.lessThanOrEqualOperator;
        } else if (compOp == CompOperatorEnum.COMP_LT) {
            sqlOp = SqlStdOperatorTable.lessThanOperator;
        } else if (compOp == CompOperatorEnum.COMP_NE) {
            sqlOp = SqlStdOperatorTable.notEqualsOperator;
        }
        filterList.add(
            rexBuilder.makeCall(sqlOp, input, filterOperand));
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        int nTerms = 2;
        if (filterOrdinals.length > 0) {
            nTerms += 2;
        }
        if (filterLiterals.size() > 0) {
            nTerms++;
        }
        if (dynamicParamIds.length > 0) {
            nTerms += 2;
        }
        String [] nameList = new String[nTerms + 1];
        Object [] objects = new Object[nTerms];

        nameList[0] = "child";
        nameList[1] = "projection";
        objects[0] = Arrays.asList(projection);
        int idx = 2;
        if (filterOrdinals.length > 0) {
            objects[idx - 1] = compOp;
            nameList[idx++] = "filterOp";
            objects[idx - 1] = Arrays.asList(filterOrdinals);
            nameList[idx++] = "filterOrdinals";
            if (filterLiterals.size() > 0) {
                objects[idx - 1] = filterLiterals;
                nameList[idx++] = "filterTuple";
            }
        }
        if (dynamicParamIds.length > 0) {
            objects[idx - 1] = Arrays.asList(dynamicParamIds);
            nameList[idx++] = "dynamicParameters";
            objects[idx - 1] = Arrays.asList(paramCompareOffsets);
            nameList[idx++] = "paramCompareOffsets";
        }

        // need to include the output rowtype in the digest to properly
        // handle casting
        objects[idx - 1] = outputRowType.getFullTypeString();
        nameList[idx] = "outputRowType";
        pw.explain(
            this,
            nameList,
            objects);
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
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();

        FemExecutionStreamDef childInput =
            implementor.visitFennelChild((FennelRel) getChild(), 0);
        implementor.addDataFlowFromProducerToConsumer(
            childInput,
            streamDef);

        streamDef.setCompareOp(compOp);

        streamDef.setOutputProjection(
            FennelRelUtil.createTupleProjection(repos, projection));
        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                typeFactory,
                outputRowType));

        streamDef.setInputCompareProjection(
            FennelRelUtil.createTupleProjection(repos, filterOrdinals));

        if (filterLiterals.size() == 0) {
            streamDef.setTupleCompareBytesBase64("");
        } else {
            int nFilters = filterOrdinals.length;
            RelDataType [] types = new RelDataType[nFilters];
            String [] fieldNames = new String[nFilters];
            RelDataTypeField [] inputFields =
                getChild().getRowType().getFields();
            for (int i = 0; i < nFilters; i++) {
                // the type needs to be nullable in case the filter is a
                // IS NULL filter
                types[i] =
                    typeFactory.createTypeWithNullability(
                        inputFields[filterOrdinals[i]].getType(),
                        true);
                fieldNames[i] = inputFields[i].getName();
            }
            RelDataType filterRowType =
                typeFactory.createStructType(types, fieldNames);
            List<List<RexLiteral>> compareTuple =
                new ArrayList<List<RexLiteral>>();
            compareTuple.add(filterLiterals);
            streamDef.setTupleCompareBytesBase64(
                FennelRelUtil.convertTuplesToBase64String(
                    filterRowType,
                    compareTuple));
        }

        for (int i = 0; i < dynamicParamIds.length; i++) {
            FemReshapeParameter reshapeParam = repos.newFemReshapeParameter();
            reshapeParam.setDynamicParamId(
                implementor.translateParamId(dynamicParamIds[i]).intValue());
            reshapeParam.setCompareOffset(paramCompareOffsets[i]);
            reshapeParam.setOutputParam(paramOutput.get(i));
            streamDef.getReshapeParameter().add(reshapeParam);
        }

        return streamDef;
    }
}

// End FennelReshapeRel.java
