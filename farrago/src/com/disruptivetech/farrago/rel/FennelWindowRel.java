/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.fem.fennel.FemWindowDef;
import net.sf.farrago.fem.fennel.FemWindowPartitionDef;
import net.sf.farrago.fem.fennel.FemWindowStreamDef;
import net.sf.farrago.query.FennelRel;
import net.sf.farrago.query.FennelRelImplementor;
import net.sf.farrago.query.FennelRelUtil;
import net.sf.farrago.query.FennelSingleRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlNode;

import java.util.ArrayList;


/**
 * FennelWindowRel is the relational expression which computes windowed
 * aggregates inside of Fennel.
 *
 * <p>Rules:<ul>
 * <li>{@link FennelWindowRule} creates this from a
 * {@link org.eigenbase.rel.CalcRel}</li>
 * </ul></p>
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 6, 2004
 */
public class FennelWindowRel extends FennelSingleRel
{
    private final RelDataType rowType;
    private final RexNode[] projectExprs;
    private final RexNode conditionExpr;

    protected FennelWindowRel(
            RelOptCluster cluster,
            RelNode child,
            RelDataType rowType,
            RexNode[] projectExprs,
            RexNode conditionExpr)
    {
        super(cluster, child);
        assert rowType != null : "precondition: rowType != null";
        assert projectExprs != null : "precondition: projectExprs != null";
        this.rowType = rowType;
        this.projectExprs = projectExprs;
        this.conditionExpr = conditionExpr;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone() {
        return new FennelWindowRel(
            cluster, child, rowType, projectExprs, conditionExpr);
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final WindowCollector windowCollector =
            new WindowCollector(cluster.rexBuilder);
        RexNode[] convertedProjectExprs = new RexNode[projectExprs.length];
        RexNode convertedConditionExpr = null;
        for (int i = 0; i < projectExprs.length; i++) {
            RexNode projectExpr = projectExprs[i];
            convertedProjectExprs[i] = windowCollector.visit(projectExpr);
        }
        if (conditionExpr != null) {
            convertedConditionExpr = windowCollector.visit(conditionExpr);
        }

        // Create a plan object.
        final FarragoRepos repos = getRepos();
        final FemWindowStreamDef windowStreamDef =
            repos.newFemWindowStreamDef();
        windowStreamDef.getInput().add(
            implementor.visitFennelChild((FennelRel) child));
        windowStreamDef.setFilter(convertedConditionExpr != null);

        // Generate output program.
        final RexToCalcTranslator translator =
            new RexToCalcTranslator(cluster.rexBuilder,
                convertedProjectExprs,
                convertedConditionExpr);
        final String program = translator.getProgram(child.getRowType());
        windowStreamDef.setOutputProgram(program);

        // Setup sort list.
        Integer[] sortFields = {};
        final RelDataTypeField[] fields = child.getRowType().getFields();
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
        for (int i = 0; i < windowCollector.windows.size(); i++) {
            Window window = (Window) windowCollector.windows.get(i);
            final FemWindowDef windowDef = repos.newFemWindowDef();
            windowStreamDef.getWindow().add(windowDef);
            windowDef.setPhysical(window.physical);
            ArrayList ordinalList = new ArrayList();
            for (int j = 0; j < window.orderExprs.length; j++) {
                RexNode orderItem = window.orderExprs[j];
                int z = windowCollector.lookupPre(orderItem);
                ordinalList.add(new Integer(z));
            }
            Integer[] ordinals = (Integer[])
                ordinalList.toArray(new Integer[ordinalList.size()]);
            windowDef.setOrderKeyList(
                FennelRelUtil.createTupleProjection(repos, ordinals));

            // For each partition...
            for (int j = 0; j < window.partitions.size(); j++) {
                Partition partition = (Partition) window.partitions.get(j);
                final FemWindowPartitionDef windowPartitionDef =
                    repos.newFemWindowPartitionDef();
                windowDef.getPartition().add(windowPartitionDef);
                final String todo = null;
                windowPartitionDef.setInitializeProgram(todo);
                windowPartitionDef.setAddProgram(todo);
                windowPartitionDef.setDropProgram(todo);
                ordinalList.clear();
                for (int k = 0; k < partition.partitionExprs.length; j++) {
                    RexNode partitionExpr = partition.partitionExprs[j];
                    int z = windowCollector.lookupPre(partitionExpr);
                    ordinalList.add(new Integer(z));
                }
                ordinals = (Integer[])
                    ordinalList.toArray(new Integer[ordinalList.size()]);
                windowPartitionDef.setPartitionKeyList(
                    FennelRelUtil.createTupleProjection(repos, ordinals));
            }
        }

        return windowStreamDef;
    }

    /**
     * Returns whether two {@link RexNode} arrays are identical.
     */
    private static boolean equal(RexNode[] exprs0, RexNode[] exprs1) {
        if (exprs0.length != exprs1.length) {
            return false;
        }
        for (int i = 0; i < exprs0.length; i++) {
            RexNode expr0 = exprs0[i];
            if (!expr0.equals(exprs1[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Splits an expression into a window aggregate, code before, and code
     * after. Also makes a list of distinct windows and partitions seen.
     */
    private static class WindowCollector extends RexShuttle {
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
                return builder.makeOver(over.getType(), over.op,
                    clonedOperands, over.window, over.window.getLowerBound(),
                    over.window.getUpperBound(), over.window.isRows());
            } else {
                RexNode[] clonedOperands = new RexNode[call.operands.length];
                for (int i = 0; i < call.operands.length; i++) {
                    RexNode operand = call.operands[i];
                    clonedOperands[i] = subCollector.visit(operand);
                }
                return builder.makeCall(call.op, clonedOperands);
            }
        }

        private void registerWindow(RexOver over) {
            Window newWindow = new Window(over.physical, new RexNode[0],
                over.window.getLowerBound(), over.window.getUpperBound());
            final int windowIndex = windows.indexOf(newWindow);
            Window window;
            if (windowIndex < 0) {
                window = newWindow;
                windows.add(window);
            } else {
                window = (Window) windows.get(windowIndex);
            }
            Partition newPartition = new Partition();
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

    private static class Window {
        final ArrayList partitions = new ArrayList();
        final boolean physical;
        final RexNode[] orderExprs;
        final SqlNode lowerBound;
        final SqlNode upperBound;


        Window(
            boolean physical,
            RexNode[] orderExprs,
            SqlNode lowerBound,
            SqlNode upperBound)
        {
            this.physical = physical;
            this.orderExprs = orderExprs;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Window)) {
                return false;
            }
            Window that = (Window) obj;
            if (equal(this.orderExprs, that.orderExprs) &&
                this.lowerBound.equals(that.lowerBound) &&
                this.upperBound.equals(that.upperBound) &&
                this.physical == that.physical) {
                return true;
            }
            return false;
        }
    }

    private static class Partition {
        final ArrayList overList = new ArrayList();
        RexNode[] partitionExprs;

        Partition() {}

        public boolean equals(Object obj) {
            if (obj instanceof Partition) {
                Partition that = (Partition) obj;
                if (equal(this.partitionExprs, that.partitionExprs)) {
                    return true;
                }
            }
            return false;
        }
    }
}

// End FennelWindowRel.java