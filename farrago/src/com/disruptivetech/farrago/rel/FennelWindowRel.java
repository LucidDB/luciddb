/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2004-2006 Disruptive Tech
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

import com.disruptivetech.farrago.calc.RexToCalcTranslator;
import net.sf.farrago.FarragoMetadataFactory;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.math.BigDecimal;


/**
 * FennelWindowRel is the relational expression which computes windowed
 * aggregates inside of Fennel.
 *
 * <p>A window rel can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Rules:<ul>
 * <li>{@link FennelWindowRule} creates this from a {@link CalcRel}</li>
 * <li>{@link WindowedAggSplitterRule} decomposes a {@link CalcRel} which
 *     contains windowed aggregates into a {@link FennelWindowRel} and zero or
 *     more {@link CalcRel}s which do not contain windowed aggregates</li>
 * </ul></p>
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 6, 2004
 */
public class FennelWindowRel extends FennelSingleRel
{
    private final RexProgram inputProgram;
    private final RexProgram outputProgram;
    private final Window[] windows;

    /**
     * Creates a window relational expression.
     *
     * <p>Each {@link Window} has a set of {@link Partition} objects,
     * and each {@link Partition} object has a set of {@link RexOver}
     * objects.
     *
     * @param cluster
     * @param child
     * @param rowType
     * @param inputProgram
     * @param windows
     * @param outputProgram
     *
     * @pre inputProgram.getCondition() == null
     */
    protected FennelWindowRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexProgram inputProgram,
        Window[] windows,
        RexProgram outputProgram)
    {
        super(
            cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child);
        assert rowType != null : "precondition: rowType != null";
        assert outputProgram != null : "precondition: outputExprs != null";
        assert inputProgram != null : "precondition: inputProgram != null";
        assert inputProgram.getCondition() == null :
            "precondition: inputProgram.getCondition() == null";
        assert windows != null : "precondition: windows != null";
        assert windows.length > 0 : "precondition : windows.length > 0";
        assert child.getConvention() == FennelRel.FENNEL_EXEC_CONVENTION;
        assert !RexOver.containsOver(inputProgram);
        assert !RexOver.containsOver(outputProgram);
        assert RelOptUtil.getFieldTypeList(outputProgram.getInputRowType()).
            equals(outputProgramInputTypes(child.getRowType(), windows));
        assert RelOptUtil.eq(
            "type1", outputProgram.getOutputRowType(), "type2", rowType, true);
        this.rowType = rowType;
        this.outputProgram = outputProgram;
        this.inputProgram = inputProgram;
        this.windows = windows;
    }

    private static List<RelDataType> outputProgramInputTypes(
        RelDataType rowType, Window[] windows)
    {
        List<RelDataType> typeList =
            new ArrayList<RelDataType>(RelOptUtil.getFieldTypeList(rowType));
        for (Window window : windows) {
            for (Partition partition : window.partitionList) {
                for (RexWinAggCall over : partition.overList) {
                    typeList.add(over.getType());
                }
            }
        }
        return typeList;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone()
    {
        FennelWindowRel clone = new FennelWindowRel(
            getCluster(), getChild(), rowType, inputProgram, windows,
            outputProgram);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public RexNode [] getChildExps()
    {
        // Do not return any child exps. inputExprs, outputExprs and
        // conditionExpr (which are RexNode[]s) are handled along with windows
        // (which is not a RexNode[]) by explain.
        return RexNode.EMPTY_ARRAY;
    }

    public void explain(RelOptPlanWriter pw)
    {
        final ArrayList valueList = new ArrayList();
        final List<String> termList = new ArrayList<String>();
        getExplainTerms(termList, valueList);
        pw.explain(
            this,
            termList.toArray(new String[termList.size()]),
            valueList.toArray(new Object[valueList.size()]));
    }

    private void getExplainTerms(
        List termList,
        List valueList)
    {
        termList.add("child");
        inputProgram.collectExplainTerms("in-", termList, valueList);
        outputProgram.collectExplainTerms("out-", termList, valueList);
        for (int i = 0; i < windows.length; i++) {
            Window window = windows[i];
            termList.add("window#" + i);
            valueList.add(window.toString());
        }
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // Cost is proportional to the number of rows and the number of
        // components (windows, partitions, and aggregate functions). There is
        // no I/O cost.
        //
        // TODO #1. Add memory cost. Memory cost is higher for MIN and MAX
        //    than say SUM and COUNT (because they maintain a binary tree).
        // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
        final double rowsIn = RelMetadataQuery.getRowCount(getChild());
        int count = windows.length;
        for (int i = 0; i < windows.length; i++) {
            Window window = windows[i];
            count += window.partitionList.size();
            for (Partition partition : window.partitionList) {
                count += partition.overList.size();
            }
        }
        if (outputProgram.getCondition() != null) {
            ++count;
        }
        return planner.makeCost(rowsIn, rowsIn * count, 0);
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        // Create a plan object.
        final FarragoMetadataFactory repos = implementor.getRepos();
        final FemWindowStreamDef windowStreamDef =
            repos.newFemWindowStreamDef();
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()),
            windowStreamDef);
        
        windowStreamDef.setFilter(outputProgram.getCondition() != null);

        // Generate output program.
        RexToCalcTranslator translator =
            new RexToCalcTranslator(getCluster().getRexBuilder(), this);
        String program = translator.generateProgram(
            outputProgram.getInputRowType(), outputProgram);
        windowStreamDef.setOutputProgram(program);

        // Setup sort list.
        Integer[] sortFields = {};
        final RelDataTypeField[] fields = getChild().getRowType().getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            // FIXME (jhyde, 2004/12/6) programmatically determine which are
            //   the sort keys of the underlying relexp.
            if (!field.getName().toUpperCase().equals("ROWTIME")) {
                continue;
            }
            sortFields = new Integer[] {new Integer(i)};
        }
        windowStreamDef.setInputOrderKeyList(
            FennelRelUtil.createTupleProjection(repos, sortFields));

        // For each window...
        for (int i = 0; i < windows.length; i++) {
            Window window = windows[i];
            final FemWindowDef windowDef = repos.newFemWindowDef();
            windowStreamDef.getWindow().add(windowDef);
            windowDef.setPhysical(window.physical);
            windowDef.setOrderKeyList(
                FennelRelUtil.createTupleProjection(repos, window.orderKeys));

            // Cases: (range, offset)
            // 3 preceding: (3, 0)
            // 3 following: (3, 3)
            // 10 preceding and 2 preceding: (8, -2)
            // 3 preceding and 2 following: (5, 2)
            // 2 following and 6 following: (4, 6)
            long[] offsetAndRange =
                getOffsetAndRange(window.lowerBound, window.upperBound);
            windowDef.setOffset((int) offsetAndRange[0]);
            windowDef.setRange(offsetAndRange[1] + "");

            RelDataType inputRowType = getChild().getRowType();
            assert inputRowType == inputProgram.getInputRowType();

            // For each partition...
            for (Partition partition : window.partitionList) {
                final FemWindowPartitionDef windowPartitionDef =
                    repos.newFemWindowPartitionDef();
                windowDef.getPartition().add(windowPartitionDef);
                translator = new RexToCalcTranslator(
                    getCluster().getRexBuilder());

                // Create a program for the window partition to init, add, drop
                // rows. Does not include the expression to form the output
                // record.
                final RexCall[] overs =
                    partition.overList.toArray(
                        new RexCall[partition.overList.size()]);
                RexProgram combinedProgram = makeProgram(inputProgram, overs);

                String[] programs = new String[3];
                translator.getAggProgram(combinedProgram, programs);
                windowPartitionDef.setInitializeProgram(programs[0]);
                windowPartitionDef.setAddProgram(programs[1]);
                windowPartitionDef.setDropProgram(programs[2]);

                RexNode[] dups = removeDuplicates(translator, overs);
                final FemTupleDescriptor bucketDesc = FennelRelUtil.
                        createTupleDescriptorFromRexNode(repos, dups);
                windowPartitionDef.setBucketDesc(bucketDesc);

                windowPartitionDef.setPartitionKeyList(
                    FennelRelUtil.createTupleProjection(
                        repos, partition.partitionKeys));
            }
        }

        return windowStreamDef;
    }

    /**
     * Creates a program with one output field per windowed aggregate
     * expression.
     *
     * @param bottomProgram Calculates the inputs to the program
     * @param overs Aggregate expressions
     * @return Combined program
     *
     * @pre bottomPogram.getCondition() == null
     * @post return.getProjectList().size() == overs.length
     * @see RexProgramBuilder#mergePrograms(RexProgram, RexProgram, RexBuilder) 
     */
    private RexProgram makeProgram(RexProgram bottomProgram, RexCall[] overs)
    {
        assert bottomProgram.getCondition() == null :
            "pre: bottomPogram.getCondition() == null";

        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RexProgramBuilder topProgramBuilder =
            new RexProgramBuilder(bottomProgram.getOutputRowType(), rexBuilder);
        for (int i = 0; i < overs.length; i++) {
            RexCall over = overs[i];
            topProgramBuilder.addProject(over, "$" + i);
        }
        final RexProgram topProgram = topProgramBuilder.getProgram();

        // Merge the programs.
        final RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(topProgram, bottomProgram, rexBuilder);

        assert mergedProgram.getProjectList().size() == overs.length :
            "post: return.getProjectList().size() == overs.length";
        return mergedProgram;
    }

    // TODO: add a duplicate-elimination feature to RexProgram, use that, and
    //   obsolete this method
    private RexNode[] removeDuplicates(
        RexToCalcTranslator translator,
        RexNode[] outputExps)
    {
        HashMap dups = new HashMap();
        for (int i = 0; i < outputExps.length; i++) {
            RexNode node = outputExps[i];
            if (node instanceof RexWinAggCall) {
                // This should be aggregate input.
                Object key = translator.getKey(node);
                if (dups.containsKey(key)) {
                    continue;
                }
                dups.put(key, node);
            }
        }
        RexNode[] nodes = new RexNode[dups.size()];
        int count = 0;
        for (int i = 0; i < outputExps.length; i++) {
            RexNode node = outputExps[i];
            if (!(node instanceof RexWinAggCall)) {
                continue;
            }
            Object key = translator.getKey(node);
            if (dups.containsKey(key)) {
                nodes[count] = node;
                dups.remove(key);
                count++;
            }
        }
        return nodes;
    }

    public static long[] getOffsetAndRange(
        final SqlNode lowerBound,
        final SqlNode upperBound)
    {
        long[] upper = getRangeOffset(upperBound, "PRECEDING");
        long[] lower = getRangeOffset(lowerBound, "FOLLOWING");
        long offset = upper[1] * upper[0];
        if (offset == 0 && lower[1] == -1) {
            lower[1] = -lower[1];
            offset = lower[1] * lower[0];
        }
        final long range = (lower[1] * lower[0] + upper[1] * upper[0]);
        return new long[] {offset, range};
    }

    private static long[] getRangeOffset(SqlNode node, String strCheck)
    {
        long[] out = new long[2];
        long val = 0;
        long sign = 1;
        if (node != null) {
            if (node instanceof SqlCall) {
                sign = ((SqlCall) node).getOperator().getName().
                        equals(strCheck) ? -1 : 1;
                SqlNode[] operands = ((SqlCall) node).getOperands();
                assert operands.length == 1 && operands[0] != null;
                SqlLiteral operand = (SqlLiteral) operands[0];
                Object obj = operand.getValue();
                if (obj instanceof BigDecimal) {
                    val = ((BigDecimal) obj).intValue();
                } else if (obj instanceof SqlIntervalLiteral.IntervalValue) {
                    val = SqlParserUtil.intervalToMillis((SqlIntervalLiteral.IntervalValue) obj);
                }
            }
        }
        out[0] = val;
        out[1] = sign;
        return out;
    }

    /**
     * A Window is a range of input rows, defined by an upper and lower bound.
     * It also contains a list of {@link Partition} objects.
     *
     * <p>A window is either logical or physical.
     * A physical window is measured in terms of row count.
     * A logical window is measured in terms of rows within a certain
     * distance from the current sort key.
     *
     * <p>For example:<ul>
     *
     * <li><code>ROWS BETWEEN 10 PRECEDING and 5 FOLLOWING</code> is
     *     a physical window with an upper and lower bound;
     *
     * <li><code>RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND UNBOUNDED
     *     FOLLOWING</code> is a logical window with only a lower bound;
     *
     * <li><code>RANGE INTERVAL '10' MINUTES PRECEDING</code>
     *     (which is equivalent to <code>RANGE BETWEEN INTERVAL '10' MINUTES
     *     PRECEDING AND CURRENT ROW</code>) is a logical window with an upper
     *     and lower bound.
     *
     * </ul>
     */
    static class Window {
        /** The partitions which make up this window. */
        final List<Partition> partitionList = new ArrayList<Partition>();
        final boolean physical;
        final SqlNode lowerBound;
        final SqlNode upperBound;
        public final Integer[] orderKeys;
        private String digest;

        Window(
            boolean physical,
            SqlNode lowerBound,
            SqlNode upperBound,
            Integer[] ordinals)
        {
            assert ordinals != null : "precondition: ordinals != null";
            this.physical = physical;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.orderKeys = ordinals;
        }

        public String toString()
        {
            return digest;
        }

        public void computeDigest()
        {
            final StringBuffer buf = new StringBuffer();
            computeDigest(buf);
            this.digest = buf.toString();
        }

        private void computeDigest(StringBuffer buf)
        {
            buf.append("window(");
            buf.append("order by {");
            for (int i = 0; i < orderKeys.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append("$");
                buf.append(orderKeys[i].intValue());
            }
            buf.append("}");
            buf.append(physical ? " rows " : " range ");
            if (lowerBound != null) {
                if (upperBound != null) {
                    buf.append("between ");
                    buf.append(lowerBound.toString());
                    buf.append(" and ");
                } else {
                    buf.append(lowerBound.toString());
                }
            }
            if (upperBound != null) {
                buf.append(upperBound.toString());
            }
            buf.append(" partitions(");
            int i = 0;
            for (Partition partition : partitionList) {
                if (i++ > 0) {
                    buf.append(", ");
                }
                partition.computeDigest(buf);
            }
            buf.append(")");
            buf.append(")");
        }

        public boolean equals(Object obj) {
            return obj instanceof Window &&
                this.digest.equals(((Window) obj).digest);
        }

        public Partition lookupOrCreatePartition(Integer[] partitionKeys)
        {
            for (Partition partition : partitionList) {
                if (Util.equal(partition.partitionKeys, partitionKeys)) {
                    return partition;
                }
            }
            Partition partition = new Partition(partitionKeys);
            partitionList.add(partition);
            return partition;
        }
    }

    /**
     * A Partition is a collection of windowed aggregate expressions which
     * belong to the same {@link Window} and have the same partitioning keys.
     */
    static class Partition
    {
        /**
         * Array of {@link RexWinAggCall} objects, each of which is a call to a
         * {@link SqlAggFunction}.
         */
        final List<RexWinAggCall> overList = new ArrayList<RexWinAggCall>();
        /**
         * The ordinals of the input columns which uniquely identify rows
         * in this partition. May be empty. Must not be null.
         */
        final Integer[] partitionKeys;

        Partition(Integer[] partitionKeys)
        {
            assert partitionKeys != null;
            this.partitionKeys = partitionKeys;
        }

        public boolean equals(Object obj)
        {
            if (obj instanceof Partition) {
                Partition that = (Partition) obj;
                if (Util.equal(this.partitionKeys, that.partitionKeys)) {
                    return true;
                }
            }
            return false;
        }

        private void computeDigest(StringBuffer buf)
        {
            buf.append("partition(");
            buf.append("partition by {");
            for (int i = 0; i < partitionKeys.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append("$");
                buf.append(partitionKeys[i].intValue());
            }
            buf.append("} aggs {");
            int i = -1;
            for (RexCall aggCall : overList) {
                if (++i > 0) {
                    buf.append(", ");
                }
                buf.append(aggCall.toString());
            }
            buf.append("}");
            buf.append(")");
        }

        public RexWinAggCall addOver(
            RelDataType type,
            SqlAggFunction operator,
            RexNode[] operands)
        {
            // Convert operands to inputRefs -- they will refer to the output
            // fields of a lower program.
            RexNode[] clonedOperands = operands.clone();
            for (int i = 0; i < operands.length; i++) {
                RexLocalRef localRef = (RexLocalRef) operands[i];
                clonedOperands[i] =
                    new RexInputRef(
                        localRef.getIndex(),
                        localRef.getType());
            }
            final RexWinAggCall aggCall =
                new RexWinAggCall(
                    operator, type, clonedOperands, overList.size());
            overList.add(aggCall);
            return aggCall;
        }
    }

    /**
     * A call to a windowed aggregate function.
     *
     * <p>Belongs to a {@link Partition}.
     *
     * <p>It's a bastard son of a {@link RexCall}; similar enough that it gets
     * visited by a {@link RexVisitor}, but it also has some extra data
     * members.
     */
    public static class RexWinAggCall extends RexCall
    {
        /**
         * Ordinal of this aggregate within its partition.
         */
        public int ordinal;

        RexWinAggCall(SqlAggFunction aggFun,
            RelDataType type,
            RexNode[] operands,
            int ordinal)
        {
            super(type, aggFun, operands);
            this.ordinal = ordinal;
        }
    }
}

// End FennelWindowRel.java
