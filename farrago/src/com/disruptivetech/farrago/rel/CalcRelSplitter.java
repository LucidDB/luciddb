/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
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

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.util.ArrayQueue;
import org.eigenbase.util.Util;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.StringWriter;
import java.io.PrintWriter;

import net.sf.farrago.trace.FarragoTrace;

/**
 * CalcRelSplitter operates on a CalcRel with multiple RexCalls that cannot
 * all be implemented by a single concrete Rel.  For example, the Java and
 * Fennel calculator do not implement an identical set of operators.  The
 * CalcRel can be used to split a single CalcRel with mixed Java- and
 * Fennel-only operators into a tree of CalcRel object that can each be
 * individually implemented by either Java or Fennel.and splits it into several
 * CalcRel instances.
 *
 * <p>Currently the splitter is only capable of handling two "rel types".  That
 * is, it can deal with Java vs. Fennel CalcRels, but not Java vs. Fennel vs.
 * some other type of CalcRel.
 *
 * <p>See {@link FarragoAutoCalcRule} for an example of how this class is
 * used.
 */
public abstract class CalcRelSplitter
{
    //~ Static fields/initializers --------------------------------------------

    public static final RelType REL_TYPE_EITHER =
        new RelType("REL_TYPE_EITHER");

    private static final Logger ruleTracer =
        FarragoTrace.getOptimizerRuleTracer();

    //~ Fields ----------------------------------------------------------------

    /** Used for iterative over forests by level. */
    private static final Object LEVEL_MARKER = new Object();

    /** The original CalcRel that is being transformed. */
    protected final CalcRel calc;

    protected final RelType relType1;
    protected final RelType relType2;

    private ArrayList nodeDataList;

    private int maxRelLevel;
    private RelType currentRelType;
    private boolean usedRelLevel;

    /**
     * Construct a CalcRelSplitter.  The parameters <code>relType1</code and
     * <code>relType2</code> must be different references.
     *
     * @param calc CalcRel to split
     * @param relType1 First "rel type" (e.g. Java)
     * @param relType2 Second "rel type" (e.g. Fennel)
     */
    CalcRelSplitter(CalcRel calc, RelType relType1, RelType relType2)
    {
        assert(relType1 != relType2): "Rel types must be distinct";

        this.calc = calc;
        this.relType1 = relType1;
        this.relType2 = relType2;

        this.maxRelLevel = -1;
        this.currentRelType = REL_TYPE_EITHER;
        this.usedRelLevel = false;
    }


    RelNode execute()
    {
        buildNodeDataTreeList();

        assignLevels();
        insertInputRefs();
        ArrayList[] levelExpressions = transform();

        if (ruleTracer.isLoggable(Level.FINER)) {
            traceLevelExpressions(levelExpressions);
        }

        return convertLevelExpressionsToCalcRels(levelExpressions);
    }

    /**
     * Traverses the forest of expressions in <code>calc</code>
     * breadth-first and assign a level to each node.  Each level
     * corresponds to a set of nodes that will be implemented
     * either in rel type 1 or rel type 2.  Nodes can either be
     * pulled up to their parent node's level (if both are
     * implementable in the same calc) or pushed down to a lower
     * level.  The type of level 0 is determined by the first node
     * that forces a specific calc.  If no node in the first level
     * of nodes requires a specific calc, level 0 is arbitrarily
     * implemented in rel type 1.
     */
    private void assignLevels()
    {
        // A queue used to perform a traversal of the forest,
        // level by level.
        ArrayQueue nodeDataQueue = new ArrayQueue();
        nodeDataQueue.addAll(nodeDataList);
        nodeDataQueue.add(LEVEL_MARKER);

        final AssignLevelsVisitor visitor = new AssignLevelsVisitor();

        int relLevel = 0;

        while (true) {
            // remove first node
            Object nodeDataItem = nodeDataQueue.poll();

            if (nodeDataItem == LEVEL_MARKER) {
                if (nodeDataQueue.isEmpty()) {
                    break;
                }

                if (currentRelType == REL_TYPE_EITHER) {
                    // Top-most level can be implemented as either
                    // rel type.  Arbitrarily pick one.
                    currentRelType = relType1;
                }

                if (usedRelLevel) {
                    // Alternate rel types after the first level.
                    if (currentRelType == relType1) {
                        currentRelType = relType2;
                    } else {
                        currentRelType = relType1;
                    }
                    relLevel++;

                    usedRelLevel = false;
                }

                nodeDataQueue.add(LEVEL_MARKER);
                continue;
            }

            NodeData nodeData = (NodeData) nodeDataItem;

            visitor.nodeData = nodeData;
            visitor.relLevel = relLevel;

            nodeData.node.accept(visitor);

            if (nodeData.children != null) {
                nodeDataQueue.addAll(nodeData.children);
            }
        }
    }




    /**
     * Given that rel type alternates with increasing level, two
     * levels have the same rel type if they are both odd or both
     * even.
     */
    private boolean sameRelType(int relLevelA, int relLevelB)
    {
        return (relLevelA & 0x1) == (relLevelB & 0x1);
    }

    /**
     * Internal method that handles adjusting the level at which a
     * particular expression will be implemented.
     *
     * <p>One of <code>isRelType1</code> or <code>isRelType2</code>
     * must be true.
     *
     * @param nodeData data about a particular RexNode
     * @param relLevel the current rel level
     * @param isRelType1 if this node can be implemented in rel type 1
     * @param isRelType2 if this node can be implemented in rel type 2
     */
    private void adjustLevel(
        NodeData nodeData,
        int relLevel,
        boolean isRelType1,
        boolean isRelType2)
    {
        if (currentRelType == REL_TYPE_EITHER) {
            assert (relLevel == 0);

            if (!isRelType1) {
                currentRelType = relType2;
            } else if (!isRelType2) {
                currentRelType = relType1;
            }

            nodeData.relLevel = relLevel;
            maxRelLevel = relLevel;
            usedRelLevel = true;
        } else if ((currentRelType == relType1 && !isRelType1)
                   || (currentRelType == relType2 && !isRelType2)) {
            // Node is mismatched for the rel we'll be using.
            if ((nodeData.parent != null)
                && (nodeData.parent.relLevel < relLevel)) {
                // Pull this node up to previous level.
                nodeData.relLevel = relLevel - 1;
            } else {
                // Push this node down into the next rel level.
                nodeData.relLevel = relLevel + 1;
                maxRelLevel = Math.max(maxRelLevel, relLevel + 1);
            }
        } else {
            if ((relLevel > 0) && isRelType1 && isRelType2) {
                // Pull this node up to the previous level.
                nodeData.relLevel = relLevel - 1;
            } else if (nodeData.parent != null
                       && sameRelType(relLevel,
                                      nodeData.parent.relLevel)) {
                // Pull this node up to the parent's level
                // (regardless of how far up that is).
                nodeData.relLevel = nodeData.parent.relLevel;
            } else {
                nodeData.relLevel = relLevel;
                maxRelLevel = Math.max(maxRelLevel, relLevel);
                usedRelLevel = true;
            }
        }
    }

    /**
     * Traverses the forest of expressions, creating new
     * RexInputRefs to pass data between levels.  When
     * RexInputRefs are encountered that at other than the lowest
     * level, we insert another RexInputRef as a child at the next
     * level down.  For RexCall nodes, if the node's level is
     * lower than the current level, we insert a RexInputRef at
     * the current level to refer to the RexCall at the lower
     * level.
     */
    private void insertInputRefs()
    {
        ArrayQueue nodeDataQueue = new ArrayQueue(nodeDataList);
        nodeDataQueue.add(LEVEL_MARKER);

        int position = 0;
        int expectedRelLevel = 0;

        // Assign positions and copy RexInputRefs down to the
        // deepest levels.  Also makes sure that RexDynamicParams
        // and RexFieldAccesses that cannot be implemented with
        // their parents have a RexInputRef inserted between the
        // parent and RexDynamicRef/RexFieldAccess (see
        // processCallChildren).
        while (true) {
            Object nodeDataItem = nodeDataQueue.poll();
            if (nodeDataItem == LEVEL_MARKER) {
                if (nodeDataQueue.isEmpty()) {
                    break;
                }

                position = 0;
                expectedRelLevel++;
                nodeDataQueue.add(LEVEL_MARKER);
                continue;
            }

            NodeData nodeData = (NodeData) nodeDataItem;
            RexNode node = nodeData.node;

            List children = Collections.EMPTY_LIST;

            if (node instanceof RexInputRef) {
                if (nodeData.relLevel < maxRelLevel) {
                    // Copy RexInputRefs down to the lowest level.
                    NodeData childNodeData =
                        new NodeData(
                            node, nodeData.isConditional, nodeData);
                    childNodeData.relLevel = nodeData.relLevel + 1;

                    nodeData.children = new ArrayList();
                    nodeData.children.add(childNodeData);

                    children = nodeData.children;
                }
            } else if (node instanceof RexCall
                       || node instanceof RexFieldAccess) {
                assert (nodeData.relLevel >= expectedRelLevel);

                if (nodeData.relLevel > expectedRelLevel) {
                    // insert an input reference
                    NodeData inputRefData =
                        new NodeData(
                            null, nodeData.isConditional, nodeData.parent);
                    inputRefData.children = new ArrayList();
                    inputRefData.children.add(nodeData);

                    if (nodeData.parent != null) {
                        int parentIndex =
                            nodeData.parent.children.indexOf(nodeData);
                        nodeData.parent.children.set(
                            parentIndex, inputRefData);
                    } else {
                        int index = nodeDataList.indexOf(nodeData);
                        nodeDataList.set(index, inputRefData);
                    }

                    nodeData = inputRefData;

                    children = nodeData.children;
                } else {
                    // pull children up into this NodeData, if necessary
                    children = new ArrayList();

                    processCallChildren(
                        children, nodeData, expectedRelLevel, maxRelLevel);
                }
            }

            nodeData.position = position++;

            if (children != null) {
                nodeDataQueue.addAll(children);
            }
        }
    }


    /**
     * Traverses the forest of expressions and converts them into
     * an array whose elements are an ArrayList containing the
     * RexNodes for a given level.
     *
     * @return the expressions for each level stored in an array
     *         of ArrayList objects.
     */
    private ArrayList[] transform()
    {
        ArrayQueue nodeDataQueue = new ArrayQueue(nodeDataList);
        nodeDataQueue.add(LEVEL_MARKER);

        int expectedRelLevel = 0;

        ArrayList[] levelExpressions = new ArrayList[maxRelLevel + 1];
        for (int i = 0; i <= maxRelLevel; i++) {
            levelExpressions[i] = new ArrayList();
        }

        // Figure out what the actual RexNode trees for each level and
        // projection are.
        while (true) {
            Object nodeDataItem = nodeDataQueue.poll();
            if (nodeDataItem == LEVEL_MARKER) {
                if (nodeDataQueue.isEmpty()) {
                    break;
                }

                expectedRelLevel++;
                nodeDataQueue.add(LEVEL_MARKER);
                continue;
            }

            ArrayList expressions = levelExpressions[expectedRelLevel];
            assert(expressions != null);

            NodeData nodeData = (NodeData) nodeDataItem;
            RexNode node = nodeData.node;

            assert (nodeData.relLevel == expectedRelLevel);

            List children = Collections.EMPTY_LIST;

            if ((node == null) || node instanceof RexInputRef) {
                if (expectedRelLevel < maxRelLevel) {
                    NodeData childNodeData =
                        ((NodeData) nodeData.children.get(0));

                    int index = childNodeData.position;

                    RelDataType type =
                        ((node == null) ? childNodeData.node.getType()
                         : node.getType());

                    RexInputRef inputRef = new RexInputRef(index, type);

                    expressions.add(inputRef);

                    children = nodeData.children;
                } else {
                    expressions.add(node.clone());
                }
            } else if (node instanceof RexCall
                       || node instanceof RexFieldAccess) {
                children = new ArrayList();

                RexNode clonedCall =
                    transformCallChildren(nodeData, children, maxRelLevel);

                expressions.add(clonedCall);
            } else {
                expressions.add(node.clone());
            }

            nodeDataQueue.addAll(children);
        }

        return levelExpressions;
    }


    /**
     * Iterates over the array of level expression lists and
     * converts each level expression list (e.g. each element of
     * the array) into a CalcRel.
     *
     * @return the top-level CalcRel of the converted expressions
     *         as a RelNode
     */
    private RelNode convertLevelExpressionsToCalcRels(
        ArrayList[] levelExpressions)
    {
        // Generate the actual CalcRel objects that represent the
        // decomposition of the original CalcRel.
        RelDataTypeFactory typeFactory = calc.getCluster().getTypeFactory();

        RelNode resultCalcRel = calc.child;
        for (int i = levelExpressions.length - 1; i >= 0; i--) {
            ArrayList expressions = levelExpressions[i];

            if (i > 0) {
                int numNodes = expressions.size();
                RexNode [] nodes = new RexNode[numNodes];
                RelDataType [] types = new RelDataType[numNodes];
                String [] names = new String[numNodes];
                for (int j = 0; j < numNodes; j++) {
                    nodes[j] = (RexNode) expressions.get(j);
                    types[j] = nodes[j].getType();
                    names[j] = "$" + j;
                }

                RelDataType rowType =
                    typeFactory.createStructType(types, names);

                resultCalcRel =
                    new CalcRel(
                        calc.getCluster(),
                        calc.getTraits(),
                        resultCalcRel,
                        rowType,
                        nodes,
                        null);
            } else {
                NodeData lastNodeData =
                    (NodeData) nodeDataList.get(nodeDataList.size() - 1);

                int numNodes = expressions.size();
                RexNode conditional = null;

                if (lastNodeData.isConditional) {
                    conditional = (RexNode) expressions.get(numNodes - 1);
                    numNodes--;
                }

                RexNode [] nodes = new RexNode[numNodes];
                for (int j = 0; j < numNodes; j++) {
                    nodes[j] = (RexNode) expressions.get(j);
                }

                resultCalcRel =
                    new CalcRel(
                        calc.getCluster(),
                        calc.getTraits(),
                        resultCalcRel,
                        calc.rowType,
                        nodes,
                        conditional);
            }
        }

        return resultCalcRel;
    }


    /**
     * Iterate over a RexCall's (or RexFieldAccess's) children,
     * copying RexInputRefs to the next level down (if necessary),
     * inserting RexInputRefs between a child RexDynamicParam and
     * the RexCall (if necessary), and recursively processing
     * child RexCalls and RexFieldAccesses at the same level.
     */
    private void processCallChildren(
        List children,
        NodeData nodeData,
        int expectedRelLevel,
        int maxRelLevel)
    {
        for (ListIterator i = nodeData.children.listIterator();
             i.hasNext();) {
            NodeData child = (NodeData) i.next();

            if (child.node instanceof RexInputRef) {
                if (child.relLevel < maxRelLevel) {
                    NodeData childNodeData =
                        new NodeData(
                            child.node, child.isConditional, child);
                    childNodeData.relLevel = child.relLevel + 1;

                    child.children = new ArrayList();
                    child.children.add(childNodeData);

                    children.add(childNodeData);
                }
            } else if (child.node instanceof RexDynamicParam) {
                if (child.relLevel > expectedRelLevel) {
                    NodeData inputRefData =
                        new NodeData(
                            null, nodeData.isConditional, nodeData);
                    inputRefData.children = new ArrayList();
                    inputRefData.children.add(child);
                    i.set(inputRefData);

                    children.add(child);
                }
            } else if (child.node instanceof RexCall
                       || child.node instanceof RexFieldAccess) {
                if (child.relLevel == expectedRelLevel) {
                    processCallChildren(
                        children, child, expectedRelLevel, maxRelLevel);
                } else {
                    children.add(child);
                }
            }
        }
    }


    /**
     * Traverses a RexCall's or RexFieldAccess's children and creates
     * new RexInputRef objects with the correct data.  Essentially
     * implements the hierarchy created in
     * {@link #processCallChildren(List, NodeData, int, int)}.
     *
     * @return the RexNode that should replace nodeData.node in the
     *         expression.
     */
    private RexNode transformCallChildren(
        NodeData nodeData,
        List children,
        int maxRelLevel)
    {
        assert (nodeData.node instanceof RexCall
                || nodeData.node instanceof RexFieldAccess);

        RexNode clonedCall = (RexNode) nodeData.node.clone();

        for (int i = 0; i < nodeData.children.size(); i++) {
            NodeData child = (NodeData) nodeData.children.get(i);

            if ((child.node == null)
                || child.node instanceof RexInputRef) {
                if (child.relLevel < maxRelLevel) {
                    assert (child.children.size() == 1);
                    NodeData grandChild = (NodeData) child.children.get(0);

                    int position = grandChild.position;

                    RexInputRef inputRef =
                        new RexInputRef(
                            position, grandChild.node.getType());

                    if (nodeData.node instanceof RexCall) {
                        ((RexCall) clonedCall).operands[i] = inputRef;
                    } else {
                        ((RexFieldAccess) clonedCall).expr = inputRef;
                    }

                    children.add(grandChild);
                }
            } else if (child.node instanceof RexCall
                       || child.node instanceof RexFieldAccess) {
                if (child.relLevel == nodeData.relLevel) {
                    RexNode clonedChildCall =
                        transformCallChildren(
                            child, children, maxRelLevel);

                    if (nodeData.node instanceof RexCall) {
                        ((RexCall) clonedCall).operands[i] =
                            clonedChildCall;
                    } else {
                        ((RexFieldAccess) clonedCall).expr =
                            clonedChildCall;
                    }
                } else {
                    RexInputRef inputRef =
                        new RexInputRef(child.position,
                                        child.node.getType());

                    if (nodeData.node instanceof RexCall) {
                        ((RexCall) clonedCall).operands[i] = inputRef;
                    } else {
                        ((RexFieldAccess) clonedCall).expr = inputRef;
                    }

                    children.add(child);
                }
            }
        }

        return clonedCall;
    }


    protected abstract boolean canImplementAs(RexCall call, RelType relType);

    protected abstract boolean canImplementAs(
        RexDynamicParam param, RelType relType);

    protected abstract boolean canImplementAs(
        RexFieldAccess field, RelType relType);

    /**
     * Creates a forest of trees in {@link #nodeDataList} based on
     * the project and conditional expressions of the given
     * CalcRel.
     */
    private void buildNodeDataTreeList()
    {
        nodeDataList = new ArrayList();

        ArrayList baseNodes = new ArrayList();
        if (calc.projectExprs != null && calc.projectExprs.length > 0) {
            for (int i = 0; i < calc.projectExprs.length; i++) {
                RexNode projectExpr = calc.projectExprs[i];
                baseNodes.add(projectExpr);

                nodeDataList.add(new NodeData(projectExpr, false, null));
            }
        }
        if (calc.conditionExpr != null) {
            baseNodes.add(calc.conditionExpr);
            nodeDataList.add(new NodeData(calc.conditionExpr, true, null));
        }

        buildNodeDataTreeList(nodeDataList, baseNodes);
    }


    private static void buildNodeDataTreeList(List nodeDataList,
                                              List nodeList)
    {
        assert (nodeDataList != null);
        assert (nodeList != null);
        assert (nodeDataList.size() == nodeList.size());

        Iterator r = nodeDataList.iterator();
        Iterator n = nodeList.iterator();
        while (r.hasNext()) {
            Object nodeDataItem = r.next();
            RexNode node = (RexNode) n.next();

            if (node instanceof RexCall) {
                NodeData nodeData = (NodeData) nodeDataItem;
                RexCall call = (RexCall) node;

                nodeData.children = new ArrayList(call.operands.length);

                for (int i = 0; i < call.operands.length; i++) {
                    RexNode op = call.operands[i];

                    nodeData.children.add(
                        new NodeData(
                            op, nodeData.isConditional, nodeData));
                }

                buildNodeDataTreeList(
                    nodeData.children,
                    Arrays.asList(call.operands));
            } else if (node instanceof RexFieldAccess) {
                NodeData nodeData = (NodeData) nodeDataItem;
                RexFieldAccess fieldAccess = (RexFieldAccess) node;

                nodeData.children = new ArrayList(1);

                nodeData.children.add(
                    new NodeData(fieldAccess.expr, nodeData.isConditional,
                                 nodeData));

                buildNodeDataTreeList(
                    nodeData.children,
                    Collections.singletonList(fieldAccess.expr));
            }
        }
    }

    /**
     * Traces the given array of level expression lists at the
     * finer level.
     *
     * @param levelExpressions an array of level expression lists to trace
     */
    private void traceLevelExpressions(ArrayList[] levelExpressions)
    {
        StringWriter traceMsg = new StringWriter();
        PrintWriter traceWriter = new PrintWriter(traceMsg);
        traceWriter.println("FarragoAutoCalcRule result expressions for:");
        traceWriter.println(calc.toString());

        int level = 0;
        for (int i = 0; i < levelExpressions.length; i++) {
            ArrayList expressions = levelExpressions[i];
            traceWriter.println("Rel Level " + level++);

            int index = 0;
            for (Iterator j = expressions.iterator(); j.hasNext();) {
                RexNode node = (RexNode) j.next();

                traceWriter.println("\t" + String.valueOf(index++) + ": "
                       + node.toString());
            }
            traceWriter.println();
        }
        String msg = traceMsg.toString();
        ruleTracer.finer(msg);
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class RelType
    {
        private final String name;

        public RelType(String name)
        {
            this.name = name;
        }

        public String toString()
        {
            return name;
        }
    }


    public class AssignLevelsVisitor
        implements RexVisitor
    {
        NodeData nodeData;
        int relLevel;

        void defaultVisit(RexNode node)
        {
            if (nodeData.parent != null) {
                nodeData.relLevel = nodeData.parent.relLevel;
            } else {
                nodeData.relLevel = 0;
            }
        }

        public void visitInputRef(RexInputRef inputRef)
        {
            defaultVisit(inputRef);
        }

        public void visitLiteral(RexLiteral literal)
        {
            defaultVisit(literal);
        }

        public void visitCall(RexCall call)
        {
            boolean isRelType1 = canImplementAs(call, relType1);
            boolean isRelType2 = canImplementAs(call, relType2);

            checkValidRelType(call, isRelType1, isRelType2);

            adjustLevel(nodeData, relLevel, isRelType1, isRelType2);
        }

        public void visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            defaultVisit(correlVariable);
        }

        public void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            boolean isRelType1 = canImplementAs(dynamicParam, relType1);
            boolean isRelType2 = canImplementAs(dynamicParam, relType2);

            checkValidRelType(dynamicParam, isRelType1, isRelType2);

            adjustLevel(nodeData, relLevel, isRelType1, isRelType2);
        }

        public void visitRangeRef(RexRangeRef rangeRef)
        {
            defaultVisit(rangeRef);
        }

        public void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            boolean isRelType1 = canImplementAs(fieldAccess, relType1);
            boolean isRelType2 = canImplementAs(fieldAccess, relType2);

            checkValidRelType(fieldAccess, isRelType1, isRelType2);

            adjustLevel(nodeData, relLevel, isRelType1, isRelType2);
        }

        private void checkValidRelType(
            RexNode node, boolean isRelType1, boolean isRelType2)
        {
            // REVIEW: SZ: 8/4/2004: Ideally, this test would
            // be performed much earlier and this rule would
            // not perform any work for an unimplementable
            // CalcRel.  Doing so requires traversing all the
            // RexNodes in the CalcRel and performing this
            // test for each RexCall.  The common case is that
            // the test passes and the rule continues, at
            // which point we end up here and perform the test
            // again for each RexCall (and others).  If we add the initial
            // test, we should probably cache the results (Map
            // of SqlOperator to rel type?), rather than
            // testing each RexCall repeatedly.
            if (!isRelType1 && !isRelType2) {
                throw Util.newInternal("Implementation of "
                    + node + " not found");
            }
        }
    }


    /**
     * NodeData represents information concerning the final location
     * of RexNodes in a series of <code>CalcRel</code>s.
     */
    private static class NodeData
    {
        /** Data regarding the children of the RexCall. */
        List children;

        /** Which CalcRel the RexCall will belong to (0-based). */
        int relLevel;

        /** This NodeData's parent -- null means top-level. */
        final NodeData parent;

        /** The RexNode this NodeData is associated with. */
        final RexNode node;

        /**
         * True if this expression belongs to the original CalcRel's
         * conditional expression tree.
         */
        final boolean isConditional;
        int position;

        NodeData(RexNode node, boolean isConditional, NodeData parent)
        {
            this.node = node;
            this.isConditional = isConditional;
            this.parent = parent;
        }
    }
}
