/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.relopt.hep;

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.util.*;

import org._3pq.jgrapht.*;
import org._3pq.jgrapht.graph.*;
import org._3pq.jgrapht.traverse.*;
import org._3pq.jgrapht.alg.*;

import java.util.*;
import java.util.logging.*;

/**
 * HepPlanner is a heuristic implementation of the {@link RelOptPlanner}
 * interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class HepPlanner extends AbstractRelOptPlanner
{
    private HepProgram mainProgram;
    
    private HepProgram currentProgram;
    
    private HepRelVertex root;

    private Map<String, HepRelVertex> mapDigestToVertex;

    private Set<RelOptRule> allRules;

    private List<RelOptRule> unknownRules;

    private int nTransformations;

    private int graphSizeLastGC;

    private int nTransformationsLastGC;

    /**
     * Query graph, with edges directed from parent to child.  This is a
     * single-rooted DAG, possibly with additional roots corresponding to
     * discarded plan fragments which remain to be garbage-collected.
     */
    private DirectedGraph<HepRelVertex, Edge<HepRelVertex>> graph;

    /**
     * Creates a new HepPlanner.
     *
     * @param program program controlling rule application
     */
    public HepPlanner(HepProgram program)
    {
        this.mainProgram = program;
        
        mapDigestToVertex = new HashMap<String, HepRelVertex>();
        graph = new DefaultDirectedGraph<HepRelVertex, Edge<HepRelVertex>>();
        allRules = new HashSet<RelOptRule>();
    }

    // implement RelOptPlanner
    public void setRoot(RelNode rel)
    {
        root = addRelToGraph(rel);
        dumpGraph();
    }

    // implement RelOptPlanner
    public RelNode getRoot()
    {
        return root;
    }

    // implement RelOptPlanner
    public boolean addRule(RelOptRule rule)
    {
        boolean added = allRules.add(rule);
        if (added) {
            mapRuleDescription(rule);
        }
        return added;
    }

    // implement RelOptPlanner
    public boolean removeRule(RelOptRule rule)
    {
        unmapRuleDescription(rule);
        return allRules.remove(rule);
    }

    // implement RelOptPlanner
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits)
    {
        // Just ignore traits.
        return rel;
    }
    
    // implement RelOptPlanner
    public RelNode findBestExp()
    {
        assert(root != null);

        executeProgram(mainProgram);

        // Get rid of everything except what's in the final plan.
        collectGarbage();
        
        return buildFinalPlan(root);
    }

    private void executeProgram(HepProgram program)
    {
        HepProgram savedProgram = currentProgram;
        currentProgram = program;
        currentProgram.initialize(program == mainProgram);
        for (HepInstruction instruction : currentProgram.instructions) {
            instruction.execute(this);
            int delta = nTransformations - nTransformationsLastGC;
            if (delta > graphSizeLastGC) {
                // The number of transformations performed since the last
                // garbage collection is greater than the number of vertices in
                // the graph at that time.  That means there should be a
                // reasonable amount of garbage to collect now.  We do
                // it this way to amortize garbage collection cost over
                // multiple instructions, while keeping the highwater
                // memory usage proportional to the graph size.
                collectGarbage();
            }
        }
        currentProgram = savedProgram;
    }

    void executeInstruction(
        HepInstruction.MatchLimit instruction)
    {
        currentProgram.matchLimit = instruction.limit;
    }
    
    void executeInstruction(
        HepInstruction.MatchOrder instruction)
    {
        currentProgram.matchOrder = instruction.order;
    }
    
    void executeInstruction(
        HepInstruction.RuleInstance instruction)
    {
        if (skippingGroup()) {
            return;
        }
        if (instruction.rule == null) {
            assert(instruction.ruleDescription != null);
            instruction.rule =
                getRuleByDescription(instruction.ruleDescription);
        }
        if (instruction.rule != null) {
            applyRules(Collections.singleton(instruction.rule));
        }
    }
    
    void executeInstruction(
        HepInstruction.RuleClass instruction)
    {
        if (skippingGroup()) {
            return;
        }
        if (instruction.ruleSet == null) {
            instruction.ruleSet = new HashSet<RelOptRule>();
            for (RelOptRule rule : allRules) {
                if (instruction.ruleClass.isInstance(rule)) {
                    instruction.ruleSet.add(rule);
                }
            }
        }
        applyRules(instruction.ruleSet);
    }

    private boolean skippingGroup()
    {
        if (currentProgram.group != null) {
            // Skip if we've already collected the ruleset.
            return !currentProgram.group.collecting;
        } else {
            // Not grouping.
            return false;
        }
    }
    
    void executeInstruction(
        HepInstruction.Subprogram instruction)
    {
        for (;;) {
            int nTransformationsBefore = nTransformations;
            executeProgram(instruction.subprogram);
            if (nTransformations == nTransformationsBefore) {
                // Nothing happened this time around.
                break;
            }
        }
    }
    
    void executeInstruction(
        HepInstruction.BeginGroup instruction)
    {
        assert(currentProgram.group == null);
        currentProgram.group = instruction.endGroup;
    }
    
    void executeInstruction(
        HepInstruction.EndGroup instruction)
    {
        assert(currentProgram.group == instruction);
        currentProgram.group = null;
        instruction.collecting = false;
        applyRules(instruction.ruleSet);
    }
    
    private void applyRules(
        Collection<RelOptRule> rules)
    {
        if (currentProgram.group != null) {
            assert(currentProgram.group.collecting);
            currentProgram.group.ruleSet.addAll(rules);
            return;
        }
        
        boolean fullRestartAfterTransformation =
            (currentProgram.matchOrder != HepMatchOrder.ARBITRARY);

        int nMatches = 0;
        
        boolean fixpoint;
        do {
            Iterator<HepRelVertex> iter = getGraphIterator(root);
            fixpoint = true;
            while (iter.hasNext()) {
                HepRelVertex vertex = iter.next();
                for (RelOptRule rule : rules) {
                    HepRelVertex newVertex = applyRule(rule, vertex);
                    if (newVertex != null) {
                        ++nMatches;
                        if (nMatches >= currentProgram.matchLimit) {
                            return;
                        }
                        if (fullRestartAfterTransformation) {
                            iter = getGraphIterator(root);
                        } else {
                            // To the extent possible, pick up where we left
                            // off; have to create a new iterator because old
                            // one was invalidated by transformation.
                            iter = getGraphIterator(newVertex);
                            // Remember to go around again since we're
                            // skipping some stuff.
                            fixpoint = false;
                        }
                        break;
                    }
                }
            }
        } while (!fixpoint);
    }

    private Iterator<HepRelVertex> getGraphIterator(HepRelVertex start)
    {
        if (currentProgram.matchOrder == HepMatchOrder.ARBITRARY) {
            return new DepthFirstIterator<
                HepRelVertex, Edge<HepRelVertex>, Long>(
                    graph, start);
        }

        assert(start == root);

        // Make sure there's no garbage, because topological sort
        // doesn't start from a specific root, and rules can't
        // deal with firing on garbage.
        collectGarbage();

        // TODO jvs 4-Apr-2006:  streamline JGraphT generics

        Iterator<HepRelVertex> iter = 
            new TopologicalOrderIterator<
            HepRelVertex, Edge<HepRelVertex>, Long>(graph);
        
        if (currentProgram.matchOrder == HepMatchOrder.TOP_DOWN) {
            return iter;
        }

        // TODO jvs 4-Apr-2006:  enhance TopologicalOrderIterator
        // to support reverse walk.  Also, consider whether we
        // care about the fact that this might cause us to fire
        // rules on discarded portions of the plan.
        assert(currentProgram.matchOrder == HepMatchOrder.BOTTOM_UP);
        ArrayList<HepRelVertex> list = new ArrayList<HepRelVertex>();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        Collections.reverse(list);
        return list.iterator();
    }

    private HepRelVertex applyRule(
        RelOptRule rule,
        HepRelVertex vertex)
    {
        RelTraitSet parentTraits = null;
        if (rule instanceof ConverterRule) {
            // Guaranteed converter rules require special casing to make sure
            // they only fire where actually needed, otherwise they tend to
            // fire to infinity and beyond.
            ConverterRule converterRule = (ConverterRule) rule;
            if (converterRule.isGuaranteed()) {
                if (!doesConverterApply(converterRule, vertex)) {
                    return null;
                }
                parentTraits = converterRule.getOutTraits();
            }
        }
        
        List<RelNode> bindings = new ArrayList<RelNode>();
        boolean match =
            matchOperands(
                rule.getOperand(),
                vertex.getCurrentRel(),
                bindings);

        if (!match) {
            return null;
        }
        
        HepRuleCall call = new HepRuleCall(
            this,
            rule.getOperand(),
            bindings.toArray(RelNode.emptyArray));
        fireRule(call);

        if (!call.getResults().isEmpty()) {
            return applyTransformationResults(
                vertex,
                call,
                parentTraits);
        }
        
        return null;
    }

    private boolean doesConverterApply(
        ConverterRule converterRule, HepRelVertex vertex)
    {
        RelTraitSet outTraits = converterRule.getOutTraits();
        List<HepRelVertex> parents = 
            GraphHelper.predecessorListOf(graph, vertex);
        for (HepRelVertex parent : parents) {
            RelNode parentRel = parent.getCurrentRel();
            if (parentRel instanceof ConverterRel) {
                // We don't support converter chains.
                continue;
            }
            if (parentRel.getTraits().matches(outTraits)) {
                // This parent wants the traits produced by the converter.
                return true;
            }
        }
        return false;
    }

    private boolean matchOperands(
        RelOptRuleOperand operand,
        RelNode rel,
        List<RelNode> bindings)
    {
        if (!operand.matches(rel)) {
            return false;
        }
        bindings.add(rel);
        Object [] childOperands = operand.getChildren();
        if (childOperands == null) {
            return true;
        }
        int n = childOperands.length;
        RelNode [] childRels = rel.getInputs();
        if (n != childRels.length) {
            return false;
        }
        for (int i = 0; i < n; ++i) {
            boolean match =
                matchOperands(
                    (RelOptRuleOperand) childOperands[i],
                    ((HepRelVertex) childRels[i]).getCurrentRel(),
                    bindings);
            if (!match) {
                return false;
            }
        }
        return true;
    }

    private HepRelVertex applyTransformationResults(
        HepRelVertex vertex, HepRuleCall call,
        RelTraitSet parentTraits)
    {
        // TODO jvs 5-Apr-2006:  Take the one that gives the best
        // global cost rather than the best local cost.  That requires
        // "tentative" graph edits.

        assert(!call.getResults().isEmpty());

        RelNode bestRel = null;

        if (call.getResults().size() == 1) {
            // No costing required; skip it to minimize the chance of hitting
            // rels without cost information.
            bestRel = call.getResults().get(0);
        } else {
            RelOptCost bestCost = null;
            for (RelNode rel : call.getResults()) {
                RelOptCost thisCost = getCost(rel);
                if ((bestRel == null) || thisCost.isLt(bestCost)) {
                    bestRel = rel;
                    bestCost = thisCost;
                }
            }
        }

        ++nTransformations;
        notifyTransformation(
            call,
            bestRel,
            true);

        // Before we add the result, make a copy of the list of vertex's
        // parents.  We'll need this later during contraction so that
        // we only update the existing parents, not the new parents
        // (otherwise loops can result).  Also take care of filtering
        // out parents by traits in case we're dealing with a converter rule.
        List<HepRelVertex> allParents =
            GraphHelper.predecessorListOf(graph, vertex);
        List<HepRelVertex> parents = new ArrayList<HepRelVertex>();
        for (HepRelVertex parent : allParents) {
            if (parentTraits != null) {
                RelNode parentRel = parent.getCurrentRel();
                if (!parentRel.getTraits().matches(parentTraits)) {
                    // This parent does not want the converted result.
                    continue;
                }
            }
            parents.add(parent);
        }

        HepRelVertex newVertex = addRelToGraph(bestRel);
        contractVertices(newVertex, vertex, parents);

        if (getListener() != null) {
            // Assume listener doesn't want to see garbage.
            collectGarbage();
        }

        notifyTransformation(
            call,
            bestRel,
            false);

        dumpGraph();
        
        return newVertex;
    }
    
    // implement RelOptPlanner
    public RelNode register(
        RelNode rel,
        RelNode equivRel)
    {
        // Ignore; this call is mostly to tell Volcano how to avoid
        // infinite loops.
        return rel;
    }

    // implement RelOptPlanner
    public RelNode ensureRegistered(RelNode rel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return true;
    }

    private HepRelVertex addRelToGraph(
        RelNode rel)
    {
        // Check if a transformation already produced a reference
        // to an existing vertex.
        if (rel instanceof HepRelVertex) {
            return (HepRelVertex) rel;
        }
        
        // Recursively add children, replacing this rel's inputs
        // with corresponding child vertices.
        RelNode [] inputs = rel.getInputs();
        for (int i = 0; i < inputs.length; ++i) {
            HepRelVertex childVertex = addRelToGraph(inputs[i]);
            rel.replaceInput(i, childVertex);
        }

        // Now, check if an equivalent vertex already exists in graph.
        String digest = rel.recomputeDigest();
        HepRelVertex equivVertex = mapDigestToVertex.get(digest);
        if (equivVertex != null) {
            // Use existing vertex.
            return equivVertex;
        }

        // No equivalence:  create a new vertex to represent this rel.
        HepRelVertex newVertex = new HepRelVertex(rel);
        graph.addVertex(newVertex);
        updateVertex(newVertex, rel);
        
        inputs = rel.getInputs();
        for (int i = 0; i < inputs.length; ++i) {
            graph.addEdge(newVertex, (HepRelVertex) inputs[i]);
        }
        
        return newVertex;
    }

    private void contractVertices(
        HepRelVertex preservedVertex,
        HepRelVertex discardedVertex,
        List<HepRelVertex> parents)
    {
        if (preservedVertex == discardedVertex) {
            // Nop.
            return;
        }
        
        RelNode rel = preservedVertex.getCurrentRel();
        updateVertex(preservedVertex, rel);

        // Update specified parents of discardedVertex.
        for (HepRelVertex parent : parents) {
            RelNode parentRel = parent.getCurrentRel();
            RelNode [] inputs = parentRel.getInputs();
            for (int i = 0; i < inputs.length; ++i) {
                RelNode child = inputs[i];
                if (child != discardedVertex) {
                    continue;
                }
                parentRel.replaceInput(i, preservedVertex);
            }
            graph.removeEdge(parent, discardedVertex);
            graph.addEdge(parent, preservedVertex);
            updateVertex(parent, parentRel);
        }

        // NOTE:  we don't actually do graph.removeVertex(discardedVertex),
        // because it might still be reachable from preservedVertex.
        // Leave that job for garbage collection.

        if (discardedVertex == root) {
            root = preservedVertex;
        }
    }

    private void updateVertex(HepRelVertex vertex, RelNode rel)
    {
        if (rel != vertex.getCurrentRel()) {
            // REVIEW jvs 5-Apr-2006:  We'll do this again later
            // during garbage collection.  Or we could get rid
            // of mark/sweep garbage collection and do it precisely
            // at this point by walking down to all rels which are only
            // reachable from here.
            notifyDiscard(vertex.getCurrentRel());
        }
        String oldDigest = vertex.getCurrentRel().toString();
        if (mapDigestToVertex.get(oldDigest) == vertex) {
            mapDigestToVertex.remove(oldDigest);
        }
        String newDigest = rel.recomputeDigest();
        if (mapDigestToVertex.get(newDigest) == null) {
            mapDigestToVertex.put(newDigest, vertex);
        } else {
            // REVIEW jvs 5-Apr-2006:  Could this lead us to
            // miss common subexpressions?  When called from
            // addRelToGraph, we'll check after this method returns,
            // but what about the other callers?
        }
        if (rel != vertex.getCurrentRel()) {
            vertex.replaceRel(rel);
        }
        notifyEquivalence(
            rel,
            vertex,
            false);
    }

    private RelNode buildFinalPlan(HepRelVertex vertex)
    {
        RelNode rel = vertex.getCurrentRel();
        
        notifyChosen(rel);

        // Recursively process children, replacing this rel's inputs
        // with corresponding child rels.
        RelNode [] inputs = rel.getInputs();
        for (int i = 0; i < inputs.length; ++i) {
            RelNode child = inputs[i];
            if (!(child instanceof HepRelVertex)) {
                // Already replaced.
                continue;
            }
            child = buildFinalPlan((HepRelVertex) child);
            rel.replaceInput(i, child);
        }
        
        return rel;
    }

    private void collectGarbage()
    {
        if (nTransformations == nTransformationsLastGC) {
            // No modifications have taken place since the last gc,
            // so there can't be any garbage.
            return;
        }
        nTransformationsLastGC = nTransformations;
        
        // Yer basic mark-and-sweep.
        Set<HepRelVertex> rootSet = new HashSet<HepRelVertex>();
        Iterator<HepRelVertex> iter = new DepthFirstIterator<
            HepRelVertex, Edge<HepRelVertex>, Long>(
                graph, root);
        while (iter.hasNext()) {
            rootSet.add(iter.next());
        }

        if (rootSet.size() == graph.vertexSet().size()) {
            // Everything is reachable:  no garbage to collect.
            return;
        }
        Set<HepRelVertex> sweepSet = new HashSet<HepRelVertex>();
        for (HepRelVertex vertex : graph.vertexSet()) {
            if (!rootSet.contains(vertex)) {
                sweepSet.add(vertex);
                RelNode rel = vertex.getCurrentRel();
                notifyDiscard(rel);
                String digest = rel.toString();
                if (mapDigestToVertex.get(digest) == rel) {
                    mapDigestToVertex.remove(digest);
                }
            }
        }
        graph.removeAllVertices(sweepSet);
        graphSizeLastGC = graph.vertexSet().size();
    }

    private void assertNoCycles()
    {
        // Verify that the graph is acyclic.
        CycleDetector<HepRelVertex, Edge<HepRelVertex>> cycleDetector
            = new CycleDetector<HepRelVertex, Edge<HepRelVertex>>(graph);
        Set<HepRelVertex> cyclicVertices = cycleDetector.findCycles();
        if (cyclicVertices.isEmpty()) {
            return;
        }

        throw Util.newInternal(
            "Query graph cycle detected in HepPlanner:  "
            + cyclicVertices);
    }

    private void dumpGraph()
    {
        if (!tracer.isLoggable(Level.FINER)) {
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(Util.lineSeparator);
        for (HepRelVertex vertex : graph.vertexSet()) {
            sb.append(vertex);
            sb.append(" = ");
            sb.append(vertex.getCurrentRel());
            sb.append(Util.lineSeparator);
        }
        sb.append("Root = ");
        sb.append(root);
        sb.append(Util.lineSeparator);
        sb.append("Graph = ");
        sb.append(graph);
        tracer.finer(sb.toString());
        
        assertNoCycles();
    }
    
    // implement RelOptPlanner
    public void registerMetadataProviders(ChainedRelMetadataProvider chain)
    {
        chain.addProvider(
            new HepRelMetadataProvider());
    }
}

// End HepPlanner.java
