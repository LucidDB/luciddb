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

package com.disruptivetech.farrago.rel;

import net.sf.farrago.query.*;
import net.sf.farrago.trace.FarragoTrace;

import com.disruptivetech.farrago.calc.RexToCalcTranslator;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rel.CalcRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.SaffronRel;

import net.sf.saffron.rex.RexCall;
import net.sf.saffron.rex.RexDynamicParam;
import net.sf.saffron.rex.RexFieldAccess;
import net.sf.saffron.rex.RexInputRef;
import net.sf.saffron.rex.RexNode;

import net.sf.saffron.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import java.util.logging.*;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * FarragoAutoCalcRule is a rule for implementing {@link CalcRel} via
 * a combination of the Fennel Calculator ({@link FennelCalcRel}) and
 * the Java Calculator ({@link net.sf.saffron.oj.rel.IterCalcRel}).
 * 
 * <p>This rule does not attempt to transform the matching
 * {@link net.sf.saffron.opt.VolcanoRuleCall} if the entire CalcRel
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
public class FarragoAutoCalcRule
    extends VolcanoRule
{
    private static final Logger ruleTracer =
    	FarragoTrace.getOptimizerRuleTracer();

    /**
     * The singleton instance.
     */
    public static final FarragoAutoCalcRule instance =
    	new FarragoAutoCalcRule();

    /**
     * Creates a new FarragoAutoCalcRule object.
     */
    private FarragoAutoCalcRule()
    {
        super(new RuleOperand(CalcRel.class,
                              new RuleOperand[] {
                                  new RuleOperand(SaffronRel.class, null)
                              }));
    }


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
    public void onMatch(VolcanoRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        SaffronRel relInput = call.rels[1];

        // Test if we can translate the CalcRel to a fennel calc program
        SaffronRel fennelInput = convert(relInput,
                                         FennelPullRel.FENNEL_PULL_CONVENTION);

        final RexToCalcTranslator translator = new RexToCalcTranslator(
            calc.getCluster().rexBuilder,
            calc._projectExprs,
            calc._conditionExpr);

        if (fennelInput != null) {
            boolean canTranslate = true;
            for(int i = 0; i < calc._projectExprs.length; i++) {
                if (!translator.canTranslate(calc._projectExprs[i], true)) {
                    canTranslate = false;
                    break;
                }
            }
            if (calc._conditionExpr != null && 
                !translator.canTranslate(calc._conditionExpr, true)) {
                canTranslate = false;
            }

            if (canTranslate) {
                // yes: do nothing, let FennelCalcRule handle this CalcRel
                return;
            }
        }

        // Test if we can translate the CalcRel to a java calc program
        final SaffronRel convertedChild =
            convert(calc.child, CallingConvention.ITERATOR);

        final JavaRelImplementor relImplementor =
            calc.getCluster().getPlanner().getJavaRelImplementor(calc);

        if (convertedChild != null) {
            if (relImplementor.canTranslate(convertedChild,
                                            calc._conditionExpr,
                                            calc._projectExprs)) {
                // yes: do nothing, let IterCalcRule handle this CalcRel
                return;
            }
        }

        transform(relImplementor, translator, call, calc);
    }


    /**
     * Traverse the forest of expressions in <code>calc</code> and
     * assign a depth (level) to each node.  Each depth corresponds to
     * a set of nodes that will be implemented either in the Java or
     * Fennel calc.  Nodes can either be pulled up to their parent
     * node's depth (if both are implementable in the same calc) or
     * pushed down to a lower depth.  The type of depth 0 is
     * determined by the first node that forces a specific calc.  If
     * no node in the first level of nodes requires a specific calc,
     * depth 0 is arbitrarily implemented in Java.
     */
    private void transform(JavaRelImplementor javaRelImplementor,
                           RexToCalcTranslator calcTranslator,
                           VolcanoRuleCall ruleCall,
                           CalcRel calc)
    {
        ArrayList relDataList = buildRelDataTree(calc);

        // A queue used to perform a traversal of the forest, level by
        // level.
        ArrayList relDataQueue = new ArrayList();
        relDataQueue.addAll(relDataList);
        relDataQueue.add(DEPTH_MARKER);

        int relDepth = 0;

        TransformData tdata = new TransformData();
        tdata.maxRelDepth = -1;
        tdata.relType = REL_TYPE_EITHER;
        tdata.usedRelDepth = false;

        while(!relDataQueue.isEmpty()) {
            // remove first node
            Object relDataItem = relDataQueue.remove(0);

            if (relDataItem == DEPTH_MARKER) {
                if (relDataQueue.isEmpty()) {
                    break;
                }

                if (tdata.relType == REL_TYPE_EITHER) {
                    // Top-most level can be implemented as either
                    // Java or Fennel.  Arbitrarily pick one.
                    tdata.relType = REL_TYPE_JAVA;
                }

                if (tdata.usedRelDepth) {
                    // Alternate rel types after the first level.
                    if (tdata.relType == REL_TYPE_JAVA) {
                        tdata.relType = REL_TYPE_FENNEL;
                    } else {
                        tdata.relType = REL_TYPE_JAVA;
                    }
                    relDepth++;

                    tdata.usedRelDepth = false;
                }

                relDataQueue.add(DEPTH_MARKER);
                continue;
            }


            RelData relData = (RelData)relDataItem;
            RexNode node = relData.node;

            if (node instanceof RexCall) {
                RexCall call = (RexCall)node;

                boolean isJavaRel = canImplementInJava(calc,
                                                       javaRelImplementor,
                                                       call);
                boolean isFennelRel = canImplementInFennel(calcTranslator,
                                                           call);

                // REVIEW: SZ: 8/4/2004: Ideally, this test would be
                // performed much earlier and this rule would not
                // perform any work for an unimplementable CalcRel.
                // Doing so requires traversing all the RexNodes in
                // the CalcRel and performing this test for each
                // RexCall.  The common case is that the test passes
                // and the rule continues, at which point we end up
                // here and perform the test again for each RexCall.
                // If we add the initial test, we should probably
                // cache the results (Map of SqlOperator to rel
                // type?), rather than testing each RexCall
                // repeatedly.
                if (!isJavaRel && !isFennelRel) {
                    // REVIEW: SZ: 8/4/2004: This probably wants to be
                    // more Farrago specific, rather than a
                    // SaffronError.  Create an "internal error" in
                    // FarragoResource.xml?
                    throw Util.newInternal("Implementation of "
                                           + call.getOperator().name
                                           + " not found");
                }

                adjustDepth(relData, relDepth, tdata, isJavaRel, isFennelRel);

                if (relData.children != null) {
                    relDataQueue.addAll(relData.children);
                }
            } else if (node instanceof RexDynamicParam) {
                // handle RexDynamicParam -- implementable in Java only
                adjustDepth(relData, relDepth, tdata, true, false);
            } else if (node instanceof RexFieldAccess) {
                // handle RexFieldAccess -- implementable in Java only
                adjustDepth(relData, relDepth, tdata, true, false);

                if (relData.children != null) {
                    relDataQueue.addAll(relData.children);
                }
            } else {
                if (relData.parent != null) {
                    relData.relDepth = relData.parent.relDepth;
                } else {
                    relData.relDepth = 0;
                }
            }
        }

        transform(ruleCall, calc, tdata.maxRelDepth, relDataList);
    }


    /**
     * Given that rel type alternates with increasing depth, two
     * depths have the same rel type if they are both odd or both
     * even.
     */
    private boolean sameRelType(int relDepthA, int relDepthB)
    {
        return (relDepthA & 0x1) == (relDepthB & 0x1);
    }

    /**
     * Internal method that handles adjusting the depth at which a
     * particular expression will be implemented.
     *
     * <p>One of <code>isJavaRel</code> or <code>isFennelRel</code>
     * must be true.
     *
     * @param relData data about a particular RexNode
     * @param relDepth the current rel depth
     * @param tdata modifiable data about the transformation in process
     * @param isJavaRel if this node can be implemented in the Java Calc
     * @param isFennelRel if this node can be implemented in the Fennel Calc
     */
    private void adjustDepth(RelData relData,
                             int relDepth,
                             TransformData tdata,
                             boolean isJavaRel,
                             boolean isFennelRel)
    {
        if (tdata.relType == REL_TYPE_EITHER) {
            assert(relDepth == 0);

            if (!isJavaRel) {
                tdata.relType = REL_TYPE_FENNEL;
            } else if (!isFennelRel) {
                tdata.relType = REL_TYPE_JAVA;
            }
            
            relData.relDepth = relDepth;
            tdata.maxRelDepth = relDepth;
            tdata.usedRelDepth = true;
        } else if ((tdata.relType == REL_TYPE_JAVA && !isJavaRel) ||
                   (tdata.relType == REL_TYPE_FENNEL && !isFennelRel)) {
            // Node is mismatched for the rel we'll be using.
            if (relData.parent != null &&
                relData.parent.relDepth < relDepth) {
                // Pull this node up to previous depth.
                relData.relDepth = relDepth - 1;
            } else {
                // Push this node down into the next rel depth.
                relData.relDepth = relDepth + 1;
                tdata.maxRelDepth = Math.max(tdata.maxRelDepth, relDepth + 1);
            }
        } else {
            if (relDepth > 0 && isJavaRel && isFennelRel) {
                // Pull this node up to the previous depth.
                relData.relDepth = relDepth - 1;
            } else if (relData.parent != null &&
                       sameRelType(relDepth, relData.parent.relDepth)) {
                // Pull this node up to the parent's depth
                // (regardless of how far up that is).
                relData.relDepth = relData.parent.relDepth;
            } else {
                relData.relDepth = relDepth;
                tdata.maxRelDepth = Math.max(tdata.maxRelDepth, relDepth);
                tdata.usedRelDepth = true;
            }
        }
    }


    /**
     * Traverse the forest of expressions, creating new RexInputRefs
     * to pass data between levels.  When RexInputRefs are encountered
     * that at other than the lowest level, we insert another
     * RexInputRef as a child at the next level down.  For RexCall
     * nodes, if the node's depth is lower than the current depth, we
     * insert a RexInputRef at the current level to refer to the
     * RexCall at the lower level.
     */
    private void transform(VolcanoRuleCall ruleCall,
                           CalcRel calc,
                           final int maxRelDepth,
                           List relDataList)
    {
        ArrayList relDataQueue = new ArrayList(relDataList);
        relDataQueue.add(DEPTH_MARKER);

        int position = 0;
        int expectedRelDepth = 0;

        // Assign positions and copy RexInputRefs down to the deepest
        // levels.  Also makes sure that RexDynamicParams and
        // RexFieldAccesses that cannot be implemented with their
        // parents have a RexInputRef inserted between the parent and
        // RexDynamicRef/RexFieldAccess (see processCallChildren).
        while(!relDataQueue.isEmpty()) {
            Object relDataItem = relDataQueue.remove(0);
            if (relDataItem == DEPTH_MARKER) {
                if (relDataQueue.isEmpty()) {
                    break;
                }

                position = 0;
                expectedRelDepth++;
                relDataQueue.add(DEPTH_MARKER);
                continue;
            }

            RelData relData = (RelData)relDataItem;
            RexNode node = relData.node;

            List children = Collections.EMPTY_LIST;

            if (node instanceof RexInputRef) {
                if (relData.relDepth < maxRelDepth) {
                    // Copy RexInputRefs down to the lowest level.
                    RelData childRelData = new RelData(node,
                                                       relData.isConditional,
                                                       relData);
                    childRelData.relDepth = relData.relDepth + 1;

                    relData.children = new ArrayList();
                    relData.children.add(childRelData);

                    children = relData.children;
                }
            } else if (node instanceof RexCall ||
                       node instanceof RexFieldAccess) {
                assert(relData.relDepth >= expectedRelDepth);

                if (relData.relDepth > expectedRelDepth) {
                    // insert an input reference
                    RelData inputRefData = new RelData(null,
                                                       relData.isConditional,
                                                       relData.parent);
                    inputRefData.children = new ArrayList();
                    inputRefData.children.add(relData);

                    if (relData.parent != null) {
                        int parentIndex =
                            relData.parent.children.indexOf(relData);
                        relData.parent.children.set(parentIndex, inputRefData);
                    } else {
                        int index = relDataList.indexOf(relData);
                        relDataList.set(index, inputRefData);
                    }

                    relData = inputRefData;

                    children = relData.children;
                } else {
                    // pull children up into this RelData, if necessary
                    children = new ArrayList();

                    processCallChildren(children,
                                        relData,
                                        expectedRelDepth,
                                        maxRelDepth);
                }
            }

            relData.position = position++;

            if (children != null) {
                relDataQueue.addAll(children);
            }
        }


        relDataQueue = new ArrayList(relDataList);
        relDataQueue.add(DEPTH_MARKER);

        expectedRelDepth = 0;

        ArrayList levelExpressions = new ArrayList(maxRelDepth + 1);
        for(int i = 0; i <= maxRelDepth; i++) {
            levelExpressions.add(null);
        }

        // REVIEW: push this step into its own function

        // Figure out what the actual RexNode trees for each level and
        // projection are.
        while(!relDataQueue.isEmpty()) {
            Object relDataItem = relDataQueue.remove(0);
            if (relDataItem == DEPTH_MARKER) {
                if (relDataQueue.isEmpty()) {
                    break;
                }

                expectedRelDepth++;
                relDataQueue.add(DEPTH_MARKER);
                continue;
            }

            ArrayList expressions =
                (ArrayList)levelExpressions.get(expectedRelDepth);
            if (expressions == null) {
                expressions = new ArrayList();
                levelExpressions.set(expectedRelDepth, expressions);
            }

            RelData relData = (RelData)relDataItem;
            RexNode node = relData.node;

            assert(relData.relDepth == expectedRelDepth);

            List children = Collections.EMPTY_LIST;

            if (node == null || node instanceof RexInputRef) {
                if (expectedRelDepth < maxRelDepth) {
                    RelData childRelData = ((RelData)relData.children.get(0));
                    int index = childRelData.position;

                    SaffronType type = (node == null
                                        ? childRelData.node.getType()
                                        : node.getType());

                    RexInputRef inputRef = new RexInputRef(index, type);

                    expressions.add(inputRef);

                    children = relData.children;
                } else {
                    expressions.add(node.clone());
                }
            } else if (node instanceof RexCall ||
                       node instanceof RexFieldAccess) {
                children = new ArrayList();

                RexNode clonedCall = transformCallChildren(relData,
                                                           children,
                                                           maxRelDepth);

                expressions.add(clonedCall);
            } else {
                expressions.add(node.clone());
            }

            relDataQueue.addAll(children);
        }

        // REVIEW: consider pushing this into it's own function
        if (ruleTracer.isLoggable(Level.FINER)) {
            StringWriter traceMsg = new StringWriter();
            PrintWriter traceWriter = new PrintWriter(traceMsg);
            traceWriter.println("FarragoAutoCalcRule result expressions for:");
            traceWriter.println(calc.toString());

            int depth = 0;
            for(Iterator i = levelExpressions.iterator(); i.hasNext(); ) {
                ArrayList expressions = (ArrayList)i.next();
                traceWriter.println("Rel Depth " + depth++);

                int index = 0;
                for(Iterator j = expressions.iterator(); j.hasNext(); ) {
                    RexNode node = (RexNode)j.next();

                    traceWriter.println("\t"
                                        + String.valueOf(index++)
                                        + ": "
                                        + node.toString());
                }
                traceWriter.println();
            }
            String msg = traceMsg.toString();
            ruleTracer.finer(msg);
        }


        // REVIEW: push this step into its own function

        // Generate the actual CalcRel objects that represent the
        // decomposition of the original CalcRel.
        SaffronTypeFactory typeFactory =
            SaffronTypeFactoryImpl.threadInstance();

        SaffronRel resultCalcRel = calc.child;
        for(int i = levelExpressions.size() - 1; i >= 0; i--) {
            ArrayList expressions = (ArrayList)levelExpressions.get(i);

            if (i > 0) {
                int numNodes = expressions.size();
                RexNode[] nodes = new RexNode[numNodes];
                SaffronType[] types = new SaffronType[numNodes];
                String[] names = new String[numNodes];
                for(int j = 0; j < numNodes; j++) {
                    nodes[j] = (RexNode)expressions.get(j);
                    types[j] = nodes[j].getType();
                    names[j] = "$" + j;
                }

                SaffronType rowType = typeFactory.createProjectType(types,
                                                                    names);

                resultCalcRel = new CalcRel(calc.getCluster(),
                                            resultCalcRel,
                                            rowType,
                                            nodes,
                                            null);
            } else {
                RelData lastRelData =
                    (RelData)relDataList.get(relDataList.size() - 1);

                int numNodes = expressions.size();
                RexNode conditional = null;

                if (lastRelData.isConditional) {
                    conditional = (RexNode)expressions.get(numNodes - 1);
                    numNodes--;
                }

                RexNode[] nodes = new RexNode[numNodes];
                for(int j = 0; j < numNodes; j++) {
                    nodes[j] = (RexNode)expressions.get(j);
                }

                resultCalcRel = new CalcRel(calc.getCluster(),
                                            resultCalcRel,
                                            calc.rowType,
                                            nodes,
                                            conditional);
            }
        }

        ruleCall.transformTo(resultCalcRel);
    }


    /**
     * Iterate over a RexCall's (or RexFieldAccess's) children,
     * copying RexInputRefs to the next level down (if necessary),
     * inserting RexInputRefs between a child RexDynamicParam and the
     * RexCall (if necessary), and recursively processing child
     * RexCalls and RexFieldAccesses at the same depth.
     */
    private void processCallChildren(List children,
                                     RelData relData,
                                     int expectedRelDepth,
                                     int maxRelDepth)
    {
        for(ListIterator i = relData.children.listIterator(); i.hasNext(); ) {
            RelData child = (RelData)i.next();

            if (child.node instanceof RexInputRef) {
                if (child.relDepth < maxRelDepth) {
                    RelData childRelData = new RelData(child.node,
                                                       child.isConditional,
                                                       child);
                    childRelData.relDepth = child.relDepth + 1;

                    child.children = new ArrayList();
                    child.children.add(childRelData);

                    children.add(childRelData);
                }
            } else if (child.node instanceof RexDynamicParam) {
                if (child.relDepth > expectedRelDepth) {
                    RelData inputRefData = new RelData(null,
                                                       relData.isConditional,
                                                       relData);
                    inputRefData.children = new ArrayList();
                    inputRefData.children.add(child);
                    i.set(inputRefData);

                    children.add(child);
                }
            } else if (child.node instanceof RexCall ||
                       child.node instanceof RexFieldAccess) {
                if (child.relDepth == expectedRelDepth) {
                    processCallChildren(children,
                                        child,
                                        expectedRelDepth,
                                        maxRelDepth);
                } else {
                    children.add(child);
                }
            }
        }
    }


    /**
     * Traverse a RexCall's or RexFieldAccess's children and create
     * new RexInputRef objects with the correct data.  Essentially
     * implements the hierarchy created in
     * {@link #processCallChildren(List, FarragoAutoCalcRule.RelData, int, int)}.
     *
     * @return the RexNode that should replace relData.node in the
     *         expression.
     */
    private RexNode transformCallChildren(RelData relData,
                                          List children,
                                          int maxRelDepth)
    {
        assert(relData.node instanceof RexCall || 
               relData.node instanceof RexFieldAccess);

        RexNode clonedCall = (RexNode)relData.node.clone();

        for(int i = 0; i < relData.children.size(); i++) {
            RelData child = (RelData)relData.children.get(i);

            if (child.node == null || child.node instanceof RexInputRef) {
                if (child.relDepth < maxRelDepth) {
                    assert(child.children.size() == 1);
                    RelData grandChild = ((RelData)child.children.get(0));

                    int position = grandChild.position;

                    RexInputRef inputRef =
                        new RexInputRef(position, grandChild.node.getType());

                    if (relData.node instanceof RexCall) {
                        ((RexCall)clonedCall).operands[i] = inputRef;
                    } else {
                        ((RexFieldAccess)clonedCall).expr = inputRef;
                    }

                    children.add(grandChild);
                }
            } else if (child.node instanceof RexCall ||
                       child.node instanceof RexFieldAccess) {
                if (child.relDepth == relData.relDepth) {
                    RexNode clonedChildCall = 
                        transformCallChildren(child, children, maxRelDepth);


                    if (relData.node instanceof RexCall) {
                        ((RexCall)clonedCall).operands[i] = clonedChildCall;
                    } else {
                        ((RexFieldAccess)clonedCall).expr = clonedChildCall;
                    }
                } else {
                    RexInputRef inputRef =
                        new RexInputRef(child.position, child.node.getType());

                    if (relData.node instanceof RexCall) {
                        ((RexCall)clonedCall).operands[i] = inputRef;
                    } else {
                        ((RexFieldAccess)clonedCall).expr = inputRef;
                    }

                    children.add(child);
                }
            }
        }

        return clonedCall;
    }

    private boolean canImplementInJava(CalcRel calc,
                                       JavaRelImplementor impl,
                                       RexCall call)
    {
        return impl.canTranslate(calc, call, false);
    }

    private boolean canImplementInFennel(RexToCalcTranslator translator,
                                         RexCall call)
    {
        return translator.canTranslate(call, false);
    }


    /**
     * Creates a forest of trees based on the project and conditional
     * expressions of the given CalcRel.
     *
     * @param calc the CalcRel to convert
     * @return an ArrayList containing the roots of the trees.
     */
    private ArrayList buildRelDataTree(CalcRel calc)
    {
        ArrayList baseNodes = new ArrayList();
        ArrayList baseRelData = new ArrayList();
        if (calc._projectExprs != null && calc._projectExprs.length > 0) {
            for(int i = 0; i < calc._projectExprs.length; i++) {
                RexNode projectExpr = calc._projectExprs[i];
                baseNodes.add(projectExpr);

                baseRelData.add(new RelData(projectExpr, false, null));
            }
        }
        if (calc._conditionExpr != null) {
            baseNodes.add(calc._conditionExpr);
            baseRelData.add(new RelData(calc._conditionExpr, true, null));
        }

        buildRelDataTree(baseRelData, baseNodes);

        return baseRelData;
    }


    private void buildRelDataTree(List relDataList, List nodeList)
    {
        assert(relDataList != null);
        assert(nodeList != null);
        assert(relDataList.size() == nodeList.size());

        Iterator r = relDataList.iterator();
        Iterator n = nodeList.iterator();
        while(r.hasNext()) {
            Object relDataItem = r.next();
            RexNode node = (RexNode)n.next();

            if (node instanceof RexCall) {
                RelData relData = (RelData)relDataItem;
                RexCall call = (RexCall)node;

                relData.children = new ArrayList(call.operands.length);

                for(int i = 0; i < call.operands.length; i++) {
                    RexNode op = call.operands[i];

                    relData.children.add(new RelData(op,
                                                     relData.isConditional,
                                                     relData));
                }

                buildRelDataTree(relData.children,
                                 Arrays.asList(call.operands));
            } else if (node instanceof RexFieldAccess) {
                RelData relData = (RelData)relDataItem;
                RexFieldAccess fieldAccess = (RexFieldAccess)node;

                relData.children = new ArrayList(1);

                relData.children.add(new RelData(fieldAccess.expr,
                                                 relData.isConditional,
                                                 relData));

                buildRelDataTree(relData.children,
                                 Collections.singletonList(fieldAccess.expr));
            }
        }
    }

    // REVIEW: SZ: 8/4/2004: In retrospect, RelData is a bad name for
    // this class.  Should probably be renamed to NodeData or
    // something.
    /**
     * RelData represents information concerning the final location of
     * RexNodes in a series of <code>CalcRel</code>s.
     */
    private static class RelData
    {
        private RelData(RexNode node,
                        boolean isConditional,
                        RelData parent)
        {
            this.node = node;
            this.isConditional = isConditional;
            this.parent = parent;
        }

        /** Data regarding the children of the RexCall. */
        List children;

        /** Which CalcRel the RexCall will belong to (0-based). */
        int relDepth;

        /** This RelData's parent -- null means top-level. */
        final RelData parent;

        /** The RexNode this RelData is associated with. */
        final RexNode node;

        /**
         * True if this expression belongs to the original CalcRel's
         * conditional expression tree.
         */
        final boolean isConditional;

        int position;
    }

    /**
     * Per-execution data passed between and modified by multiple
     * functions in FarragoAutoCalcRule.
     */
    private static class TransformData
    {
        int maxRelDepth;
        Integer relType;
        boolean usedRelDepth;
    }

    /** Used for iterative over forests by level. */
    private static final Object DEPTH_MARKER = new Object();

    /** Represents a RexNode that can be implemented by either calculator. */
    private static final Integer REL_TYPE_EITHER = new Integer(-1);

    /**
     * Represents a RexNode that can only be implemented by the Java
     * calculator.
     */
    private static final Integer REL_TYPE_JAVA = new Integer(1);

    /**
     * Represents a RexNode that can only be implemented by the Fennel
     * calculator.
     */
    private static final Integer REL_TYPE_FENNEL = new Integer(2);
}
