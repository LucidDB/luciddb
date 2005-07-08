/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;


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
    private RexNode[] inputExprs;
    private RexNode[] outputExprs;
    private Window[] windows;
    private final RexNode conditionExpr;

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
     * @param inputExprs
     * @param windows
     * @param outputExprs
     * @param conditionExpr
     */
    protected FennelWindowRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexNode[] inputExprs,
        Window[] windows,
        RexNode[] outputExprs,
        RexNode conditionExpr)
    {
        super(
            cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child);
        assert rowType != null : "precondition: rowType != null";
        assert outputExprs != null : "precondition: outputExprs != null";
        for (int i = 0; i < outputExprs.length; i++) {
            assert outputExprs[i] != null : "outputExprs[i] != null";
        }
        assert inputExprs != null : "precondition: inputExprs != null";
        for (int i = 0; i < inputExprs.length; i++) {
            assert inputExprs[i] != null : "inputExprs[i] != null";
        }
        assert windows != null : "precondition: windows != null";
        assert windows.length > 0 : "precondition : windows.length > 0";
        assert child.getConvention() == FennelRel.FENNEL_EXEC_CONVENTION;
        assert !RexOver.containsOver(inputExprs, null);
        assert !RexOver.containsOver(outputExprs, conditionExpr);
        this.rowType = rowType;
        this.outputExprs = outputExprs;
        this.inputExprs = inputExprs;
        this.windows = windows;
        this.conditionExpr = conditionExpr;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone() {
        FennelWindowRel clone = new FennelWindowRel(
            getCluster(), getChild(), rowType, inputExprs, windows, outputExprs,
            conditionExpr);
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
        final ArrayList termList = new ArrayList();
        getExplainTerms(termList, valueList);
        pw.explain(
            this,
            (String[]) termList.toArray(new String[termList.size()]),
            (Object[]) valueList.toArray(new Object[valueList.size()]));
    }

    private void getExplainTerms(
        List termList,
        List valueList)
    {
        termList.add("child");
        for (int i = 0; i < inputExprs.length; i++) {
            RexNode inputExpr = inputExprs[i];
            termList.add("input#" + i);
            valueList.add(inputExpr);
        }
        for (int i = 0; i < outputExprs.length; i++) {
            RexNode outputExpr = outputExprs[i];
            termList.add("output#" + i);
            valueList.add(outputExpr);
        }
        if (conditionExpr != null) {
            termList.add("condition");
            valueList.add(conditionExpr);
        }
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
        RelOptCost childCost = planner.getCost(getChild());
        final double rowsIn = childCost.getRows();
        int count = windows.length;
        for (int i = 0; i < windows.length; i++) {
            Window window = windows[i];
            count += window.partitions.size();
            for (int j = 0; j < window.partitions.size(); j++) {
                Partition partition = (Partition) window.partitions.get(j);
                count += partition.overList.size();
            }
        }
        if (conditionExpr != null) {
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
        windowStreamDef.getInput().add(
            implementor.visitFennelChild((FennelRel) getChild()));
        windowStreamDef.setFilter(conditionExpr != null);

        // Generate output program.
        RexToCalcTranslator translator =
            new RexToCalcTranslator(getCluster().getRexBuilder());
        String program = translator.getProgram(
            // REVIEW: Is the input to the output program the buckets of all
            //   windows: [w0.b0] [w0.b1] [w1.b0] [w1.b1] [w1.b2]
            getChild().getRowType(),
            outputExprs,
            conditionExpr);
        windowStreamDef.setOutputProgram(program);

        // Setup sort list.
        Integer[] sortFields = {};
        final RelDataTypeField[] fields = getChild().getRowType().getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            // FIXME (jhyde, 2004/12/6) programmatically determine which are
            //   the sort keys of the underlying relexp.
            if (!field.getName().equals("ROWTIME")) {
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

            // For each partition...
            for (int j = 0; j < window.partitions.size(); j++) {
                Partition partition = (Partition) window.partitions.get(j);
                final FemWindowPartitionDef windowPartitionDef =
                    repos.newFemWindowPartitionDef();
                windowDef.getPartition().add(windowPartitionDef);
                translator = new RexToCalcTranslator(
                    getCluster().getRexBuilder());
                final RexCall[] overs = (RexCall[]) partition.overList.toArray(
                    new RexCall[partition.overList.size()]);
                RelDataType inputRowType = getChild().getRowType();
                String[] programs = new String[3];
                translator.getAggProgram(inputRowType, overs, programs);
                windowPartitionDef.setInitializeProgram(programs[0]);
                windowPartitionDef.setAddProgram(programs[1]);
                windowPartitionDef.setDropProgram(programs[2]);
                windowPartitionDef.setPartitionKeyList(
                    FennelRelUtil.createTupleProjection(
                        repos, partition.partitionKeys));
            }
        }

        return windowStreamDef;
    }

    /**
     * Splits an expression into a window aggregate, code before, and code
     * after. Also makes a list of distinct windows and partitions seen.
     */
    private static class WindowCollector extends RexShuttle
    {
        private final ArrayList windows = new ArrayList();
        private final RexShuttle subCollector = null;
        private final RexBuilder builder;
        private final ArrayList preExprs = new ArrayList();

        WindowCollector(RexBuilder builder) {
            this.builder = builder;
        }

        public void go(RexNode rex) {
            visit(rex);
        }

        public RexNode visit(RexCall call) {
            if (call instanceof RexOver) {
                RexOver over = (RexOver) call;
                registerWindow(over);
                RexNode[] clonedOperands = new RexNode[over.operands.length];
                for (int i = 0; i < over.operands.length; i++) {
                    RexNode operand = over.operands[i];
                    clonedOperands[i] = subCollector.visit(operand);
                }
                return builder.makeOver(
                    over.getType(),
                    over.getAggOperator(),
                    clonedOperands,
                    over.getWindow().partitionKeys,
                    over.getWindow().orderKeys,
                    over.getWindow().getLowerBound(),
                    over.getWindow().getUpperBound(),
                    over.getWindow().isRows());
            } else {
                RexNode[] clonedOperands = new RexNode[call.operands.length];
                for (int i = 0; i < call.operands.length; i++) {
                    RexNode operand = call.operands[i];
                    clonedOperands[i] = subCollector.visit(operand);
                }
                return builder.makeCall(call.getOperator(), clonedOperands);
            }
        }

        private void registerWindow(RexOver over) {
            Window newWindow = new Window(
                over.getWindow().isRows(),
                over.getWindow().getLowerBound(),
                over.getWindow().getUpperBound(),
                null /*todo*/);
            final int windowIndex = windows.indexOf(newWindow);
            Window window;
            if (windowIndex < 0) {
                window = newWindow;
                windows.add(window);
            } else {
                window = (Window) windows.get(windowIndex);
            }
            Partition newPartition = new Partition(null /*todo*/);
            int partitionIndex = window.partitions.indexOf(newPartition);
            Partition partition;
            if (partitionIndex < 0) {
                partition = newPartition;
                window.partitions.add(partition);
            } else {
                partition = (Partition) window.partitions.get(partitionIndex);
            }
            partition.overList.add(over);
        }

        public int lookupPre(RexNode node) {
            for (int i = 0; i < preExprs.size(); i++) {
                RexNode preExpr = (RexNode) preExprs.get(i);
                if (preExpr.equals(node)) {
                    return i;
                }
            }
            preExprs.add(node);
            return preExprs.size() - 1;
        }
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
    public static class Window {
        /** Array of {@link Partition}. */
        private final ArrayList partitions = new ArrayList();
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
                    buf.append("between ");
                } else {
                    buf.append(lowerBound.toString());
                }
            }
            if (upperBound != null) {
                buf.append(upperBound.toString());
            }
            buf.append(" partitions(");
            for (int i = 0; i < partitions.size(); i++) {
                Partition partition = (Partition) partitions.get(i);
                if (i > 0) {
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
            for (int i = 0; i < partitions.size(); i++) {
                Partition partition = (Partition) partitions.get(i);
                if (Util.equal(partition.partitionKeys, partitionKeys)) {
                    return partition;
                }
            }
            Partition partition = new Partition(partitionKeys);
            partitions.add(partition);
            return partition;
        }
    }

    /**
     * A Partition is a collection of windowed aggregate expressions which
     * belong to the same {@link Window} and have the same partitioning keys.
     */
    static class Partition {
        /**
         * Array of {@link RexWinAggCall} objects, each of which is a call to a
         * {@link SqlAggFunction}.
         */
        final ArrayList overList = new ArrayList();
        /**
         * The ordinals of the input columns which uniquely identify rows
         * in this partition. May be empty. Must not be null.
         */
        final Integer[] partitionKeys;

        Partition(Integer[] partitionKeys) {
            assert partitionKeys != null;
            this.partitionKeys = partitionKeys;
        }

        public boolean equals(Object obj) {
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
            for (int i = 0; i < overList.size(); i++) {
                RexCall aggCall = (RexCall) overList.get(i);
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(aggCall.toString());
            }
            buf.append("}");
            buf.append(")");
        }

        public void addOver(
            RelDataType type,
            SqlAggFunction operator,
            RexNode[] operands)
        {
            final RexNode aggCall =
                new RexWinAggCall(operator, type, operands, overList.size());
            overList.add(aggCall);
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
    public static class RexWinAggCall extends RexCall {
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

    static RexInputRef[] toInputRefs(int[] args, RelDataType rowType)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        final RexInputRef[] rexNodes = new RexInputRef[args.length];
        for (int i = 0; i < args.length; i++) {
            int fieldOrdinal = args[i];
            rexNodes[i] =
                new RexInputRef(fieldOrdinal, fields[fieldOrdinal].getType());
        }
        return rexNodes;
    }

}

// End FennelWindowRel.java
