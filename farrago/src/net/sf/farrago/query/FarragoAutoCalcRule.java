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

package net.sf.farrago.query;

import net.sf.farrago.trace.FarragoTrace;

import net.sf.saffron.calc.RexToCalcTranslator;
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
import net.sf.saffron.rex.RexInputRef;
import net.sf.saffron.rex.RexNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import java.util.logging.*;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * FarragoAutoCalcRule is a rule for implementing {@link CalcRel} via
 * a combination of the Fennel Calculator ({@link FennelCalcRel}) and
 * the Java Calculator ({@link net.sf.saffron.oj.rel.IterCalcRel}).
 * n
 * <p>This rule does not attempt to transform matching the matching
 * {@link net.sf.saffron.opt.VolcanoRuleCall} if the entire CalcRel
 * can be implemented entirely via one calculator or the other.  A
 * future optimization might be to use a costing mechanism to
 * determine where expressions that can be implemented by both
 * calculators should be executed.
 */
class FarragoAutoCalcRule
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

    // implement VolcanoRule
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
     * assign a depth to each node.  Each depth corresponds to a set
     * of nodes that will be implemented either in the Java or Fennel
     * calc.  Nodes can either be pulled up to their parent node's
     * depth (if both are implementable in the same calc) or pushed
     * down to a lower depth.  The type of depth 0 is determined by
     * the first node that forces a specific calc.  If no node in the
     * first level of nodes requires a specific calc, depth 0 is
     * arbitrarily implemented in Java.
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

        int maxRelDepth = -1;
        int relDepth = 0;
        Integer relType = REL_TYPE_EITHER;
        boolean usedRelDepth = false;

        while(!relDataQueue.isEmpty()) {
            // remove first node
            Object relDataItem = relDataQueue.remove(0);

            if (relDataItem == DEPTH_MARKER) {
                if (relDataQueue.isEmpty()) {
                    break;
                }

                if (relType == REL_TYPE_EITHER) {
                    // Top-most level can be implemented as either
                    // Java or Fennel.  Arbitrarily pick one.
                    relType = REL_TYPE_JAVA;
                }

                if (usedRelDepth) {
                    // Alternate rel types after the first level.
                    if (relType == REL_TYPE_JAVA) {
                        relType = REL_TYPE_FENNEL;
                    } else {
                        relType = REL_TYPE_JAVA;
                    }
                    relDepth++;

                    usedRelDepth = false;
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

                assert(isJavaRel || isFennelRel);

                if (relType == REL_TYPE_EITHER) {
                    assert(relDepth == 0);

                    if (!isJavaRel) {
                        relType = REL_TYPE_FENNEL;
                    } else if (!isFennelRel) {
                        relType = REL_TYPE_JAVA;
                    }

                    relData.relDepth = relDepth;
                    maxRelDepth = relDepth;
                    usedRelDepth = true;
                } else if ((relType == REL_TYPE_JAVA && !isJavaRel) ||
                           (relType == REL_TYPE_FENNEL && !isFennelRel)) {
                    // Node is mismatched for the rel we'll be using.
                    if (relData.parent != null &&
                        relData.parent.relDepth < relDepth) {
                        // Pull this node up to previous depth.
                        relData.relDepth = relDepth - 1;
                    } else {
                        // Push this node down into the next rel depth.
                        relData.relDepth = relDepth + 1;
                        maxRelDepth = Math.max(maxRelDepth, relDepth + 1);
                    }
                } else {
                    if (relDepth > 0 && isJavaRel && isFennelRel) {
                        // Pull this node up to the previous depth.
                        relData.relDepth = relDepth - 1;
                    } else if (relData.parent != null &&
                               sameRelType(relDepth,
                                           relData.parent.relDepth)) {
                        // Pull this node up to the parent's depth
                        // (regardless of how far up that is).
                        relData.relDepth = relData.parent.relDepth;
                    } else {
                        relData.relDepth = relDepth;
                        maxRelDepth = Math.max(maxRelDepth, relDepth);
                        usedRelDepth = true;
                    }
                }

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

        transform(ruleCall, calc, maxRelDepth, relDataList);
    }


    private boolean sameRelType(int relDepthA, int relDepthB)
    {
        return (relDepthA & 0x1) == (relDepthB & 0x1);
    }

    private void transform(VolcanoRuleCall ruleCall,
                           CalcRel calc,
                           final int maxRelDepth,
                           List relDataList)
    {
        ArrayList relDataQueue = new ArrayList(relDataList);
        relDataQueue.add(DEPTH_MARKER);

        int position = 0;
        int expectedRelDepth = 0;

        // Assign positions and copy RexInputRefs down to the deepest levels.
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
                    RelData childRelData = new RelData(node,
                                                       relData.isConditional,
                                                       relData);
                    childRelData.relDepth = relData.relDepth + 1;

                    relData.children = new ArrayList();
                    relData.children.add(childRelData);

                    children = relData.children;
                }
            } else if (node instanceof RexCall) {
                assert(relData.relDepth >= expectedRelDepth);

                if (relData.relDepth > expectedRelDepth) {
                    // insert an input reference
                    RelData inputRefData = new RelData(null,
                                                       relData.isConditional,
                                                       relData);
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

                    processInputRefs(children,
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
            } else if (node instanceof RexCall) {
                children = new ArrayList();

                RexCall clonedCall = (RexCall)relData.node.clone();

                transformCallChildren(relData,
                                      clonedCall,
                                      children,
                                      maxRelDepth);

                expressions.add(clonedCall);
            } else {
                expressions.add(node.clone());
            }

            relDataQueue.addAll(children);
        }

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


    private void processInputRefs(List children,
                                  RelData relData,
                                  int expectedRelDepth,
                                  int maxRelDepth)
    {
        for(Iterator i = relData.children.iterator(); i.hasNext(); ) {
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
            } else if (child.node instanceof RexCall) {
                if (child.relDepth == expectedRelDepth) {
                    processInputRefs(children,
                                     child,
                                     expectedRelDepth,
                                     maxRelDepth);
                } else {
                    children.add(child);
                }
            }
        }
    }



    private void transformCallChildren(RelData relData,
                                       RexCall clonedCall,
                                       List children,
                                       int maxRelDepth)
    {
        for(int i = 0; i < relData.children.size(); i++) {
            RelData child = (RelData)relData.children.get(i);

            if (child.node instanceof RexInputRef) {
                if (child.relDepth < maxRelDepth) {
                    assert(child.children.size() == 1);
                    RelData grandChild = ((RelData)child.children.get(0));

                    int position = grandChild.position;

                    RexInputRef inputRef =
                        new RexInputRef(position, grandChild.node.getType());

                    clonedCall.operands[i] = inputRef;

                    children.add(grandChild);
                }
            } else if (child.node instanceof RexCall) {
                if (child.relDepth == relData.relDepth) {
                    RexCall clonedChildCall = (RexCall)child.node.clone();

                    clonedCall.operands[i] = clonedChildCall;

                    transformCallChildren(child,
                                          clonedChildCall,
                                          children,
                                          maxRelDepth);
                } else {
                    RexInputRef inputRef =
                        new RexInputRef(child.position, child.node.getType());
                    clonedCall.operands[i] = inputRef;

                    children.add(child);
                }
            }
        }
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
            }
        }
    }

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

    private static class Marker
    {
    }

    private static final Marker DEPTH_MARKER = new Marker();

    private static final Integer REL_TYPE_EITHER = new Integer(-1);
    private static final Integer REL_TYPE_JAVA = new Integer(1);
    private static final Integer REL_TYPE_FENNEL = new Integer(2);
}
