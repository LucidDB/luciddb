/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
package com.disruptivetech.farrago.volcano;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.Util;


/**
 * A <code>RelSubset</code> is set of expressions in a set which have the same
 * calling convention.  An expression may be in more than one sub-set of a
 * set; the same expression is used.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 16 December, 2001
 */
public class RelSubset extends AbstractRelNode
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields -------------------------------------------------------

    /** List of the relational expressions for which this subset is an input. */
    ArrayList parents;
    ArrayList rels;
    CallingConvention convention;

    /** cost of best known plan (it may have improved since) */
    RelOptCost bestCost;
    RelSet set;

    /** best known plan */
    RelNode best;

    /** whether findBestPlan is being called */
    boolean active;

    //~ Constructors ----------------------------------------------------------

    RelSubset(
        RelOptCluster cluster,
        RelSet set,
        CallingConvention convention)
    {
        super(cluster);
        this.set = set;
        this.convention = convention;
        this.rels = new ArrayList();
        this.parents = new ArrayList();
        this.bestCost = VolcanoCost.INFINITY;
        this.digest = computeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return convention;
    }

    /**
     * There are no children, as such.  We throw an exception because you
     * probably don't want to be walking over trees which contain
     * <code>RelSet</code>s.
     */
    public RelNode [] getInputs()
    {
        throw new UnsupportedOperationException();
    }

    public Set getVariablesSet()
    {
        return set.variablesPropagated;
    }

    public Set getVariablesUsed()
    {
        return set.variablesUsed;
    }

    /**
     * An <code>RelSet</code> is its own clone.
     */
    public Object clone()
    {
        return this;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeZeroCost();
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explainSubset(id + ": RelSubset(" + convention + ")",
            (RelNode) rels.get(0));
    }

    protected String computeDigest()
    {
        return "Subset#" + set.id + "." + convention;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return set.rel.getRowType();
    }

    ArrayList getParentRels()
    {
        return parents;
    }

    Set getParentSubsets()
    {
        HashSet set = new HashSet();
        for (Iterator iterator = parents.iterator(); iterator.hasNext();) {
            RelNode rel = (RelNode) iterator.next();
            final RelSubset subset =
                ((VolcanoPlanner) cluster.planner).getSubset(rel);
            set.add(subset);
        }
        return set;
    }

    RelSet getSet()
    {
        return set;
    }

    /**
     * Add expression <code>rel</code> to this subset.
     */
    void add(RelNode rel)
    {
        if (rels.contains(rel)) {
            return;
        }

        VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().planner;
        if (planner.listener != null) {
            RelOptListener.RelEquivalenceEvent event =
                new RelOptListener.RelEquivalenceEvent(
                    planner,
                    rel,
                    this,
                    true);
            planner.listener.relEquivalenceFound(event);
        }
        
        rels.add(rel);
        set.addInternal(rel);
        Set variablesSet = RelOptUtil.getVariablesSet(rel);
        Set variablesStopped = rel.getVariablesStopped();
        Set variablesPropagated = Util.minus(variablesSet, variablesStopped);
        assert (set.variablesPropagated.containsAll(variablesPropagated));
        Set variablesUsed = RelOptUtil.getVariablesUsed(rel);
        assert (set.variablesUsed.containsAll(variablesUsed));
        propagateCostImprovements((VolcanoPlanner) (rel.getCluster().planner),
            rel);
    }

    /**
     * Recursively build a tree consisting of the cheapest plan at each node.
     */
    RelNode buildCheapestPlan(VolcanoPlanner planner)
    {
        CheapestPlanReplacer replacer = new CheapestPlanReplacer(planner);
        RelNode cheapest = RelOptUtil.go(replacer, this);
        
        if (planner.listener != null) {
            RelOptListener.RelChosenEvent event =
                new RelOptListener.RelChosenEvent(
                    planner,
                    null);
            planner.listener.relChosen(event);
        }
        
        return cheapest;
    }

    /**
     * Checks whether a relexp has made its subset cheaper, and if it so,
     * recursively checks whether that subset's parents have gotten cheaper.
     */
    void propagateCostImprovements(
        VolcanoPlanner planner,
        RelNode rel)
    {
        final RelOptCost cost = planner.getCost(rel);
        if (cost.isLt(bestCost)) {
            tracer.finer("Subset cost improved: subset [" + this
                + "] cost was " + bestCost + " now " + cost);
            bestCost = cost;
            best = rel;

            // Lower cost means lower importance. Other nodes will change
            // too, but we'll get to them later.
            planner.ruleQueue.recompute(this);
            for (int i = 0; i < parents.size(); i++) {
                RelNode parent = (RelNode) parents.get(i);
                final RelSubset parentSubset = planner.getSubset(parent);
                parentSubset.propagateCostImprovements(planner, parent);
            }
            planner.checkForSatisfiedConverters(set, rel);
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Visitor which walks over a tree of {@link RelSet}s, replacing each node
     * with the cheapest implementation of the expression.
     */
    class CheapestPlanReplacer extends RelVisitor
    {
        VolcanoPlanner planner;

        CheapestPlanReplacer(VolcanoPlanner planner)
        {
            super();
            this.planner = planner;
        }

        public void visit(
            RelNode p,
            int ordinal,
            RelNode parent)
        {
            if (p instanceof RelSubset) {
                RelSubset subset = (RelSubset) p;
                RelNode cheapest = subset.best;
                if (cheapest == null) {
                    final String expr = subset.toString();
                    if (tracer.isLoggable(Level.FINE)) {
                        // Dump the planner's expression pool so we can figure
                        // out why we reached impasse.
                        StringWriter sw = new StringWriter();
                        final PrintWriter pw = new PrintWriter(sw);
                        pw.println("Node [" + expr
                            + "] could not be implemented; planner state:");
                        planner.dump(pw);
                        pw.flush();
                        tracer.fine(sw.toString());
                    }
                    Error e =
                        Util.newInternal("node could not be implemented: "
                            + expr);
                    tracer.throwing(
                        getClass().getName(),
                        "visit",
                        e);
                    throw e;
                }
                parent.replaceInput(ordinal, cheapest);
                p = cheapest;
            }
            
            if (ordinal != -1) {
                if (planner.listener != null) {
                    RelOptListener.RelChosenEvent event =
                        new RelOptListener.RelChosenEvent(
                            planner,
                            p);
                    planner.listener.relChosen(event);
                }
            }
            
            p.childrenAccept(this);
        }
    }
}


// End RelSubset.java
