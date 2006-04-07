/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2006 Disruptive Tech
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
package com.disruptivetech.farrago.volcano;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.rel.metadata.*;
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
    final List parents;

    /** The relational expressions in this subset. */
    final List rels;

    /** cost of best known plan (it may have improved since) */
    RelOptCost bestCost;

    /** The set this subset belongs to. */
    final RelSet set;

    /** best known plan */
    RelNode best;

    /** whether findBestPlan is being called */
    boolean active;

    /** Timestamp for metadata validity */
    long timestamp;

    //~ Constructors ----------------------------------------------------------

    RelSubset(
        RelOptCluster cluster,
        RelSet set,
        RelTraitSet traits)
    {
        super(cluster, traits);
        this.set = set;
        this.rels = new ArrayList();
        this.parents = new ArrayList();
        this.bestCost = VolcanoCost.INFINITY;
        recomputeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    // REVIEW jvs 15-Mar-2005: I disabled this exception because there is
    // actually code which walks over trees containing RelSubsets (in
    // RelOptUtil), and that code was special casing traversal of RelSubset,
    // which isn't right.
    /**
     * There are no children, as such.  We throw an exception because you
     * probably don't want to be walking over trees which contain
     * <code>RelSubset</code>s.
     */
    /*
    public RelNode [] getInputs()
    {
        throw new UnsupportedOperationException();
    }
    */

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

    public double getRows()
    {
        if (best == null) {
            return VolcanoCost.INFINITY.getRows();
        } else {
            return RelMetadataQuery.getRowCount(best);
        }
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        StringBuffer s = new StringBuffer();
        s.append(id).append(": RelSubset(");
        for(int i = 0; i < traits.size(); i++) {
            if (i > 0) {
                s.append(", ");
            }
            s.append(traits.getTrait(i));
        }
        s.append(')');

        pw.explainSubset(s.toString(),
            (RelNode) rels.get(0));
    }

    protected String computeDigest()
    {
        StringBuffer digest = new StringBuffer("Subset#");
        digest.append(set.id);
        for(int i = 0; i < traits.size(); i++) {
            digest.append('.').append(traits.getTrait(i));
        }
        return digest.toString();
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return set.rel.getRowType();
    }

    // implement RelNode
    public boolean isDistinct()
    {
        return set.rel.isDistinct();
    }

    Set getParentSubsets()
    {
        HashSet set = new HashSet();
        for (Iterator iterator = parents.iterator(); iterator.hasNext();) {
            RelNode rel = (RelNode) iterator.next();
            final RelSubset subset =
                ((VolcanoPlanner) getCluster().getPlanner()).getSubset(rel);
            set.add(subset);
        }
        return set;
    }

    RelSet getSet()
    {
        return set;
    }

    /**
     * Adds expression <code>rel</code> to this subset.
     */
    void add(RelNode rel)
    {
        if (rels.contains(rel)) {
            return;
        }

        VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().getPlanner();
        if (planner.listener != null) {
            RelOptListener.RelEquivalenceEvent event =
                new RelOptListener.RelEquivalenceEvent(
                    planner,
                    rel,
                    this,
                    true);
            planner.listener.relEquivalenceFound(event);
        }

        // If this isn't the first rel in the set, it must have compatible
        // row type.
        assert set.rel == null ||
            RelOptUtil.equal(
                "rowtype of new rel", rel.getRowType(),
                "rowtype of set", getRowType(), true);
        rels.add(rel);
        set.addInternal(rel);
        Set variablesSet = RelOptUtil.getVariablesSet(rel);
        Set variablesStopped = rel.getVariablesStopped();
        if (false) {
            Set variablesPropagated = Util.minus(variablesSet, variablesStopped);
            assert set.variablesPropagated.containsAll(variablesPropagated);
            Set variablesUsed = RelOptUtil.getVariablesUsed(rel);
            assert set.variablesUsed.containsAll(variablesUsed);
        }
        propagateCostImprovements(
            (VolcanoPlanner) (rel.getCluster().getPlanner()),
            rel);
    }

    /**
     * Recursively builds a tree consisting of the cheapest plan at each node.
     */
    RelNode buildCheapestPlan(VolcanoPlanner planner)
    {
        CheapestPlanReplacer replacer = new CheapestPlanReplacer(planner);
        final RelNode cheapest = replacer.go(this);

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
        ++timestamp;
        
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

    public void collectVariablesUsed(Set variableSet)
    {
        variableSet.addAll(getVariablesUsed());
    }

    public void collectVariablesSet(Set variableSet)
    {
        variableSet.addAll(getVariablesSet());
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
                    if (tracer.isLoggable(Level.WARNING)) {
                        // Dump the planner's expression pool so we can figure
                        // out why we reached impasse.
                        StringWriter sw = new StringWriter();
                        final PrintWriter pw = new PrintWriter(sw);
                        pw.println(
                            "Node [" + subset.getDescription() +
                            "] could not be implemented; planner state:");
                        planner.dump(pw);
                        pw.flush();
                        tracer.warning(sw.toString());
                    }
                    Error e =
                        Util.newInternal(
                            "node could not be implemented: " +
                            subset.getDigest());
                    tracer.throwing(
                        getClass().getName(),
                        "visit",
                        e);
                    throw e;
                }
                if (parent == null) {
                    replaceRoot(cheapest);
                } else {
                    parent.replaceInput(ordinal, cheapest);
                }
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
