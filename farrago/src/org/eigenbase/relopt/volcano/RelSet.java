/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package org.eigenbase.relopt.volcano;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.trace.*;


/**
 * A <code>RelSet</code> is an equivalence-set of expressions; that is, a set of
 * expressions which have identical semantics. We are generally interested in
 * using the expression which has the lowest cost.
 *
 * <p>All of the expressions in an <code>RelSet</code> have the same calling
 * convention.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since 16 December, 2001
 */
class RelSet
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields --------------------------------------------------------

    final List<RelNode> rels = new ArrayList<RelNode>();
    final List<RelSubset> subsets = new ArrayList<RelSubset>();

    /**
     * List of {@link AbstractConverter} objects which have not yet been
     * satisfied.
     */
    List<AbstractConverter> abstractConverters =
        new ArrayList<AbstractConverter>();

    /**
     * Set to the superseding set when this is found to be equivalent to another
     * set.
     */
    RelSet equivalentSet;
    RelNode rel;

    /**
     * Names of variables which are set by relational expressions in this set
     * and available for use by parent and child expressions.
     */
    Set<String> variablesPropagated;

    /**
     * Names of variables which are used by relational expressions in this set.
     */
    Set<String> variablesUsed;
    int id;

    /**
     * Reentrancy flag.
     */
    boolean inMetadataQuery;

    //~ Constructors -----------------------------------------------------------

    RelSet()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns all of the {@link RelNode}s which reference {@link RelNode}s in
     * this set.
     */
    public List<RelNode> getParentRels()
    {
        List<RelNode> parents = new ArrayList<RelNode>();
        for (RelSubset subset : subsets) {
            parents.addAll(subset.parents);
        }
        return parents;
    }

    /**
     * @return all of the {@link RelNode}s contained by any subset of this set
     * (does not include the subset objects themselves)
     */
    public List<RelNode> getRelsFromAllSubsets()
    {
        return rels;
    }

    public RelSubset getSubset(RelTraitSet traits)
    {
        for (RelSubset subset : subsets) {
            if (subset.getTraits().equals(traits)) {
                return subset;
            }
        }
        return null;
    }

    /**
     * Adds a relational expression to a set, with its results available under a
     * particular calling convention. An expression may be in the set several
     * times with different calling conventions (and hence different costs).
     */
    public RelSubset add(RelNode rel)
    {
        assert equivalentSet == null : "adding to a dead set";
        RelSubset subset =
            getOrCreateSubset(
                rel.getCluster(),
                rel.getTraits());
        subset.add(rel);
        return subset;
    }

    RelSubset getOrCreateSubset(
        RelOptCluster cluster,
        RelTraitSet traits)
    {
        RelSubset subset = getSubset(traits);
        if (subset == null) {
            subset = new RelSubset(cluster, this, traits);
            subsets.add(subset);

            VolcanoPlanner planner = (VolcanoPlanner) cluster.getPlanner();
            if (planner.listener != null) {
                postEquivalenceEvent(planner, subset);
            }
        }
        return subset;
    }

    private void postEquivalenceEvent(VolcanoPlanner planner, RelNode rel)
    {
        RelOptListener.RelEquivalenceEvent event =
            new RelOptListener.RelEquivalenceEvent(
                planner,
                rel,
                "equivalence class " + id,
                false);
        planner.listener.relEquivalenceFound(event);
    }

    /**
     * Adds an expression <code>rel</code> to this set, without creating a
     * {@link org.eigenbase.relopt.volcano.RelSubset}. (Called only from
     * {@link org.eigenbase.relopt.volcano.RelSubset#add}.
     */
    void addInternal(RelNode rel)
    {
        if (!rels.contains(rel)) {
            rels.add(rel);

            VolcanoPlanner planner =
                (VolcanoPlanner) rel.getCluster().getPlanner();
            if (planner.listener != null) {
                postEquivalenceEvent(planner, rel);
            }
        }
        if (this.rel == null) {
            this.rel = rel;
        } else {
            assert (rel.getCorrelVariable() == null);
            String correl = this.rel.getCorrelVariable();
            if (correl != null) {
                rel.setCorrelVariable(correl);
            }

            // Row types must be the same, except for field names.
            RelOptUtil.verifyTypeEquivalence(
                this.rel,
                rel,
                this);
        }
    }

    /**
     * Merges <code>otherSet</code> into this RelSet.
     *
     * <p>One generally calls this method after discovering that two relational
     * expressions are equivalent, and hence the <code>RelSet</code>s they
     * belong to are equivalent also.
     *
     * <p>After this method completes, <code>otherSet</code> is obsolete, its
     * {@link #equivalentSet} member points to this RelSet, and this RelSet is
     * still alive.
     *
     * @param planner Planner
     * @param otherSet RelSet which is equivalent to this one
     */
    void mergeWith(
        VolcanoPlanner planner,
        RelSet otherSet)
    {
        assert (this != otherSet);
        assert (this.equivalentSet == null);
        assert (otherSet.equivalentSet == null);
        tracer.finer("Merge set#" + otherSet.id + " into set#" + id);
        otherSet.equivalentSet = this;

        // remove from table
        boolean existed = planner.allSets.remove(otherSet);
        assert (existed) : "merging with a dead otherSet";

        // merge subsets
        for (RelSubset otherSubset : otherSet.subsets) {
            RelSubset subset =
                getOrCreateSubset(
                    otherSubset.getCluster(),
                    otherSubset.getTraits());
            if (otherSubset.bestCost.isLt(subset.bestCost)) {
                subset.bestCost = otherSubset.bestCost;
                subset.best = otherSubset.best;
            }
            for (RelNode otherRel : otherSubset.rels) {
                planner.reregister(this, otherRel);
            }
        }

        // Has another set merged with this?
        assert equivalentSet == null;

        // Update all rels which have a child in the other set, to reflect the
        // fact that the child has been renamed.
        for (RelNode parentRel : otherSet.getParentRels()) {
            planner.rename(parentRel);
        }

        // Renaming may have caused this set to merge with another. If so,
        // this set is now obsolete. There's no need to update the children
        // of this set - indeed, it could be dangerous.
        if (equivalentSet != null) {
            return;
        }

        // Make sure the cost changes as a result of merging are propagated.
        for (RelSubset relSubset : subsets) {
            for (RelSubset parentSubset : relSubset.getParentSubsets()) {
                for (RelNode parentRel : parentSubset.rels) {
                    parentSubset.propagateCostImprovements(
                        planner,
                        parentRel);
                }
            }
        }

        assert equivalentSet == null;

        // Each of the relations in the old set now has new parents, so
        // potentially new rules can fire. Check for rule matches, just as if
        // it were newly registered.  (This may cause rules which have fired
        // once to fire again.)
        for (RelNode rel : rels) {
            assert planner.getSet(rel) == this;
            planner.fireRules(rel, true);
        }
    }
}

// End RelSet.java
