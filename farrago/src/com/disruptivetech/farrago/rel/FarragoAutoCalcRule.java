/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2002-2004 Disruptive Tech
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.*;

import net.sf.farrago.query.*;
import net.sf.farrago.trace.FarragoTrace;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.ArrayQueue;
import org.eigenbase.util.Util;


/**
 * FarragoAutoCalcRule is a rule for implementing {@link CalcRel} via
 * a combination of the Fennel Calculator ({@link FennelCalcRel}) and
 * the Java Calculator ({@link org.eigenbase.oj.rel.IterCalcRel}).
 *
 * <p>This rule does not attempt to transform the matching
 * {@link org.eigenbase.relopt.RelOptRuleCall} if the entire CalcRel
 * can be implemented entirely via one calculator or the other.  A
 * future optimization might be to use a costing mechanism to
 * determine where expressions that can be implemented by both
 * calculators should be executed.
 *
 * <p><b>Strategy:</b> Each CalcRel can be considered a forest (e.g. a
 * group of trees).  The forest is comprised of the RexNode trees
 * contained in the project and conditional expressions in the
 * CalcRel.  The rule's basic strategy is to stratify the forest into
 * levels, such that each level can be implemented entirely by a
 * single calc.  Having divided the forest into levels, the rule
 * creates new CalcRel instances which each contain all of the
 * RexNodes associated with a given level and then connects RexNodes
 * across the levels by adding RexInputRef instances as needed.  The
 * planner will then evaluate each CalcRel and should find that either
 * IterCalcRel or FennelCalcRel is able to implement each of the
 * CalcRels this rule creates.  The planner will then automatically
 * place the necessary plumbing between IterCalcRel and FennelCalcRel
 * instances to convert between the two types of calculator.  This
 * strategy depends the rules that generate IterCalcRel and
 * FennelCalcRel not performing their transformations if a portion of
 * an expression cannot be implemented in the corresponding
 * calculator.  It also depends on accurate implementability
 * information regarding RexCalls.
 *
 * <p><b>Potential improvements:</b> Currently, the rule does not
 * exploit redundancy between trees in the forest.  For example,
 * consider a table T with columns C1 and C2 and calculator functions
 * F1, F2 and J where F1 and F2 are Fennel-only and J is Java-only.
 * The query <pre>
 *     select F1(C1), F2(C1), J(C2) from T</pre>
 * begins life as <pre>
 *     CalcRel Project: "F1($0), F2($0), J($1)"</pre>
 * After applying FarragoAutoCalcRule we get <pre>
 *     CalcRel Project: "F1($0), F2($1), $2"
 *     CalcRel Project: "$0, $0, J($1)"</pre>
 * Notice that although the calls to F1 and F2 refer to the same base
 * column, the rule treats them separately.  A better result
 * would be <pre>
 *     CalcRel Project: "F1($0), F2($0), $1"
 *     CalcRel Project: "$0, J($1)"</pre>
 *
 * <p>Another improvement relates to handling conditional expressions.
 * The current implementation of the FarragoAutoCalc rule only treats
 * the conditional expression specially in the top-most
 * post-transformation CalcRel.  For example, consider a table T with
 * columns C1 and C2 and calculator functions F and J where F is
 * Fennel-only and J is Java-only.  The query <pre>
 *     select F(C1) from T where J(C2)</pre>
 * begins life as <pre>
 *     CalcRel Project: "F($0)"    Conditional: "J($1)"</pre>
 * After applying FarragoAutoCalcRule we get <pre>
 *     CalcRel Project: "F($0)"    Conditional: "$1"
 *     CalcRel Project: "$0 J($1)" Conditional:     </pre>
 * Notice that even though the conditional expression could be
 * evaluated and used for filtering in the lower CalcRel, it's not.
 * This means that all rows are sent to the Fennel calculator.  A
 * better result would be <pre>
 *     CalcRel Project: "F($0)"    Contidional:
 *     CalcRel Project: "$0"       Conditional: "J($1)"</pre>
 * In this case, rows that don't match the conditional expression
 * would not reach the Fennel calculator.
 */
public class FarragoAutoCalcRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger ruleTracer =
        FarragoTrace.getOptimizerRuleTracer();

    /**
     * The singleton instance.
     */
    public static final FarragoAutoCalcRule instance =
        new FarragoAutoCalcRule();

    /** Used for iterative over forests by level. */
    private static final Object LEVEL_MARKER = new Object();

    /** Represents a RexNode that can be implemented by either calculator. */
    private static final String REL_TYPE_EITHER = "REL_TYPE_EITHER";

    /**
     * Represents a RexNode that can only be implemented by the Java
     * calculator.
     */
    private static final String REL_TYPE_JAVA = "REL_TYPE_JAVA";

    /**
     * Represents a RexNode that can only be implemented by the Fennel
     * calculator.
     */
    private static final String REL_TYPE_FENNEL = "REL_TYPE_FENNEL";

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoAutoCalcRule object.
     */
    private FarragoAutoCalcRule()
    {
        super(new RelOptRuleOperand(
                CalcRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Called when this rule matches a rel.  First uses
     * {@link RexToCalcTranslator} to determine if the rel can be
     * implemented in the Fennel calc.  If so, this method returns
     * without performing any transformation.  Next uses
     * {@link JavaRelImplementor} to perform the same test for the
     * Java calc.  Also returns with no transformation if the Java
     * calc can implement the rel.  Finally, transforms the given
     * CalcRel into a stack of CalcRels that can each individually be
     * implemented in the Fennel or Java calcs.
     */
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        RelNode relInput = call.rels[1];

        // Test if we can translate the CalcRel to a fennel calc program
        RelNode fennelInput =
            convert(relInput, FennelPullRel.FENNEL_PULL_CONVENTION);

        final RexToCalcTranslator translator =
            new RexToCalcTranslator(calc.getCluster().rexBuilder,
                calc.projectExprs, calc.conditionExpr);

        if (fennelInput != null) {
            boolean canTranslate = true;
            for (int i = 0; i < calc.projectExprs.length; i++) {
                if (!translator.canTranslate(calc.projectExprs[i], true)) {
                    canTranslate = false;
                    break;
                }
            }
            if ((calc.conditionExpr != null)
                    && !translator.canTranslate(calc.conditionExpr, true)) {
                canTranslate = false;
            }

            if (canTranslate) {
                // yes: do nothing, let FennelCalcRule handle this CalcRel
                return;
            }
        }

        // Test if we can translate the CalcRel to a java calc program
        final RelNode convertedChild =
            convert(calc.child, CallingConvention.ITERATOR);

        final JavaRelImplementor relImplementor =
            calc.getCluster().getPlanner().getJavaRelImplementor(calc);

        if (convertedChild != null) {
            if (relImplementor.canTranslate(convertedChild,
                        calc.conditionExpr, calc.projectExprs)) {
                // yes: do nothing, let IterCalcRule handle this CalcRel
                return;
            }
        }

        Transform transform = new Transform(calc, relImplementor, translator);

        RelNode resultCalcRelTree = transform.execute();

        call.transformTo(resultCalcRelTree);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * NodeData represents information concerning the final location of
     * RexNodes in a series of <code>CalcRel</code>s.
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

        private NodeData(
            RexNode node,
            boolean isConditional,
            NodeData parent)
        {
            this.node = node;
            this.isConditional = isConditional;
            this.parent = parent;
        }
    }


    /**
     * Transform implements the actual transformation of a single
     * mixed-calculator CalcRel into a series of two or more
     * single-calculator CalcRels.
     */
    private static class Transform
    {
        /** The original CalcRel that is being transformed. */
        private final CalcRel calc;

        private final JavaRelImplementor relImplementor;

        private final RexToCalcTranslator translator;

        private ArrayList nodeDataList;

        private int maxRelLevel;
        private String relType;
        private boolean usedRelLevel;

        Transform(CalcRel calc,
                  JavaRelImplementor relImplementor,
                  RexToCalcTranslator translator)
        {
            this.calc = calc;
            this.relImplementor = relImplementor;
            this.translator = translator;

            this.maxRelLevel = -1;
            this.relType = REL_TYPE_EITHER;
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
         * either in the Java or Fennel calc.  Nodes can either be
         * pulled up to their parent node's level (if both are
         * implementable in the same calc) or pushed down to a lower
         * level.  The type of level 0 is determined by the first node
         * that forces a specific calc.  If no node in the first level
         * of nodes requires a specific calc, level 0 is arbitrarily
         * implemented in Java.
         */
        private void assignLevels()
        {
            // A queue used to perform a traversal of the forest,
            // level by level.
            ArrayQueue nodeDataQueue = new ArrayQueue();
            nodeDataQueue.addAll(nodeDataList);
            nodeDataQueue.add(LEVEL_MARKER);

            int relLevel = 0;

            while (true) {
                // remove first node
                Object nodeDataItem = nodeDataQueue.poll();
                
                if (nodeDataItem == LEVEL_MARKER) {
                    if (nodeDataQueue.isEmpty()) {
                        break;
                    }
                    
                    if (relType == REL_TYPE_EITHER) {
                        // Top-most level can be implemented as either
                        // Java or Fennel.  Arbitrarily pick one.
                        relType = REL_TYPE_JAVA;
                    }
                    
                    if (usedRelLevel) {
                        // Alternate rel types after the first level.
                        if (relType == REL_TYPE_JAVA) {
                            relType = REL_TYPE_FENNEL;
                        } else {
                            relType = REL_TYPE_JAVA;
                        }
                        relLevel++;
                        
                        usedRelLevel = false;
                    }
                    
                    nodeDataQueue.add(LEVEL_MARKER);
                    continue;
                }
                
                NodeData nodeData = (NodeData) nodeDataItem;
                RexNode node = nodeData.node;

                if (node instanceof RexCall) {
                    RexCall call = (RexCall)node;

                    boolean isJavaRel = canImplementInJava(call);
                    boolean isFennelRel = canImplementInFennel(call);

                    // REVIEW: SZ: 8/4/2004: Ideally, this test would
                    // be performed much earlier and this rule would
                    // not perform any work for an unimplementable
                    // CalcRel.  Doing so requires traversing all the
                    // RexNodes in the CalcRel and performing this
                    // test for each RexCall.  The common case is that
                    // the test passes and the rule continues, at
                    // which point we end up here and perform the test
                    // again for each RexCall.  If we add the initial
                    // test, we should probably cache the results (Map
                    // of SqlOperator to rel type?), rather than
                    // testing each RexCall repeatedly.
                    if (!isJavaRel && !isFennelRel) {
                        throw Util.newInternal("Implementation of "
                        	+ call.getOperator().name + " not found");
                    }

                    adjustLevel(nodeData, relLevel, isJavaRel, isFennelRel);
                } else if (node instanceof RexDynamicParam ||
                           node instanceof RexFieldAccess) {
                    // handle RexDynamicParam or RexFieldAccess --
                    // implementable in Java only
                    adjustLevel(nodeData, relLevel, true, false);
                } else {
                    if (nodeData.parent != null) {
                        nodeData.relLevel = nodeData.parent.relLevel;
                    } else {
                        nodeData.relLevel = 0;
                    }
                }

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
         * <p>One of <code>isJavaRel</code> or <code>isFennelRel</code>
         * must be true.
         *
         * @param nodeData data about a particular RexNode
         * @param relLevel the current rel level
         * @param isJavaRel if this node can be implemented in the Java Calc
         * @param isFennelRel if this node can be implemented in the
         *                    Fennel Calc
         */
        private void adjustLevel(
            NodeData nodeData,
            int relLevel,
            boolean isJavaRel,
            boolean isFennelRel)
        {
            if (relType == REL_TYPE_EITHER) {
                assert (relLevel == 0);
                
                if (!isJavaRel) {
                    relType = REL_TYPE_FENNEL;
                } else if (!isFennelRel) {
                    relType = REL_TYPE_JAVA;
                }
                
                nodeData.relLevel = relLevel;
                maxRelLevel = relLevel;
                usedRelLevel = true;
            } else if (((relType == REL_TYPE_JAVA) && !isJavaRel)
                       || ((relType == REL_TYPE_FENNEL) && !isFennelRel)) {
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
                if ((relLevel > 0) && isJavaRel && isFennelRel) {
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
            RelDataTypeFactory typeFactory =
                RelDataTypeFactoryImpl.threadInstance();

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
                        typeFactory.createProjectType(types, names);

                    resultCalcRel =
                        new CalcRel(
                            calc.getCluster(),
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
         * {@link #processCallChildren(
         *             List, FarragoAutoCalcRule.NodeData, int, int)}.
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
        

        private boolean canImplementInJava(RexCall call)
        {
            return relImplementor.canTranslate(calc, call, false);
        }
        

        private boolean canImplementInFennel(RexCall call)
        {
            return translator.canTranslate(call, false);
        }

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
    }
}
