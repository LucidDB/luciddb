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
package com.disruptivetech.farrago.volcano;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.Util;


/**
 * A <code>RelSet</code> is an equivalence-set of expressions; that is, a set
 * of expressions which have identical semantics.  We are generally
 * interested in using the expression which has the lowest cost.
 *
 * <p>
 * All of the expressions in an <code>RelSet</code> have the same calling
 * convention.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 16 December, 2001
 */
class RelSet
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields -------------------------------------------------------

    final ArrayList rels = new ArrayList();
    final ArrayList subsets = new ArrayList();

    /**
     * List of {@link AbstractConverter} objects which have not yet been
     * satisfied.
     */
    ArrayList abstractConverters = new ArrayList();

    /**
     * Set to the superseding set when this is found to be equivalent to
     * another set.
     */
    RelSet equivalentSet;
    RelNode rel;

    /**
     * Names of variables which are set by relational expressions in this set
     * and available for use by parent and child expressions.
     */
    Set variablesPropagated;

    /**
     * Names of variables which are used by relational expressions in this
     * set.
     */
    Set variablesUsed;
    int id;

    //~ Constructors ----------------------------------------------------------

    RelSet()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns all of the {@link RelNode}s which reference {@link
     * RelNode}s in this set.
     */
    public ArrayList getParentRels()
    {
        ArrayList parents = new ArrayList();
        for (int i = 0; i < subsets.size(); i++) {
            RelSubset subset = (RelSubset) subsets.get(i);
            parents.addAll(subset.parents);
        }
        return parents;
    }

    /**
     * @return all of the {@link RelNode}s contained by any subset
     * of this set (does not include the subset objects themselves)
     */
    public ArrayList getRelsFromAllSubsets()
    {
        ArrayList rels = new ArrayList();
        for (int i = 0; i < subsets.size(); i++) {
            RelSubset subset = (RelSubset) subsets.get(i);
            rels.addAll(subset.rels);
        }
        return rels;
    }

    public RelSubset getSubset(RelTraitSet traits)
    {
        for (int i = 0; i < subsets.size(); i++) {
            RelSubset subset = (RelSubset) subsets.get(i);
            if (subset.getTraits().equals(traits)) {
                return subset;
            }
        }
        return null;
    }

    /**
     * Adds a relational expression to a set, with its results available under
     * a particular calling convention.  An expression may be in the set
     * several times with different calling conventions (and hence different
     * costs).
     */
    public RelSubset add(RelNode rel)
    {
        assert (equivalentSet == null) : "adding to a dead set";
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
     * {@link com.disruptivetech.farrago.volcano.RelSubset}.  (Called only from {@link
     * com.disruptivetech.farrago.volcano.RelSubset#add}.
     */
    void addInternal(RelNode rel)
    {
        if (!rels.contains(rel)) {
            rels.add(rel);

            VolcanoPlanner planner = (VolcanoPlanner)
                rel.getCluster().getPlanner();
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
            if (!RelOptUtil.areRowTypesEqual(
                this.rel.getRowType(), rel.getRowType(), false)) {
                failType(rel);
            }
        }
    }

    /**
     * Merges <code>otherSet</code> into this one. You generally call this
     * method after you discover that two relational expressions are
     * equivalent, and hence their sets are equivalent also. After you have
     * called this method, <code>otherSet</code> is obsolete, this otherSet
     * is still alive.
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
        for (int i = 0; i < otherSet.subsets.size(); i++) {
            RelSubset otherSubset = (RelSubset) otherSet.subsets.get(i);
            RelSubset subset =
                getOrCreateSubset(
                    otherSubset.getCluster(),
                    otherSubset.getTraits());
            if (otherSubset.bestCost.isLt(subset.bestCost)) {
                subset.bestCost = otherSubset.bestCost;
                subset.best = otherSubset.best;
            }
            for (int j = 0; j < otherSubset.rels.size(); j++) {
                planner.reregister(this, (RelNode) otherSubset.rels.get(j));
            }
        }

        // Update all rels which have a child in the other set, to reflect the
        // fact that the child has been renamed.
        for (Iterator parentRels = otherSet.getParentRels().iterator();
                parentRels.hasNext();) {
            planner.rename((RelNode) parentRels.next());
        }

        // Make sure the cost changes as a result of merging are propagated.
        for (Iterator relSubsets = subsets.iterator();
                relSubsets.hasNext(); ) {
            RelSubset relSubset = (RelSubset)relSubsets.next();
            for (Iterator parentSubsets =
                        relSubset.getParentSubsets().iterator();
                    parentSubsets.hasNext(); ) {
                RelSubset parentSubset = (RelSubset)parentSubsets.next();

                for (Iterator parentRels = parentSubset.rels.iterator();
                        parentRels.hasNext(); ) {
                    parentSubset.propagateCostImprovements(
                        planner, (RelNode) parentRels.next());
                }
            }
        }

        // Each of the relations in the old set now has new parents, so
        // potentially new rules can fire. Check for rule matches, just as if
        // it were newly registered.  (This may cause rules which have fired
        // once to fire again.)
        for (int i = 0; i < rels.size(); i++) {
            RelNode rel = (RelNode) rels.get(i);
            assert planner.getSet(rel) == this;
            planner.fireRules(rel, true);
        }
    }

    private void failType(RelNode rel)
    {
        final RelDataType rowType = this.rel.getRowType();
        final RelDataType relRowType = rel.getRowType();
        String s =
            "Cannot add expression of different type to set: "
            + Util.lineSeparator + "set type is "
            + rowType.getFullTypeString()
            + Util.lineSeparator + "expression type is "
            + relRowType.getFullTypeString()
            + Util.lineSeparator + "set is " + toString() + Util.lineSeparator
            + "expression is " + rel.toString();
        throw Util.newInternal(s);
    }
}


// End RelSet.java
