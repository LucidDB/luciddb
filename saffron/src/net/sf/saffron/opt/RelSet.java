/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.opt;

import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;
import net.sf.saffron.core.SaffronType;

import openjava.tools.DebugOut;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;


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
    SaffronRel rel;

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
     * Returns all of the {@link SaffronRel}s which reference {@link
     * SaffronRel}s in this set.
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

    public RelSubset getSubset(CallingConvention convention)
    {
        for (int i = 0; i < subsets.size(); i++) {
            RelSubset subset = (RelSubset) subsets.get(i);
            if (subset.convention == convention) {
                return subset;
            }
        }
        return null;
    }

    /**
     * Add a relational expression to a set, with its results available under
     * a particular calling convention.  An expression may be in the set
     * several times with different calling conventions (and hence different
     * costs).
     */
    public RelSubset add(SaffronRel rel)
    {
        assert (equivalentSet == null) : "adding to a dead set";
        RelSubset subset =
            getOrCreateSubset(rel.getCluster(),rel.getConvention());
        subset.add(rel);
        return subset;
    }

    RelSubset getOrCreateSubset(
        VolcanoCluster cluster,
        CallingConvention convention)
    {
        RelSubset subset = getSubset(convention);
        if (subset == null) {
            subset = new RelSubset(cluster,this,convention);
            subsets.add(subset);
        }
        return subset;
    }

    /**
     * Adds an expression <code>rel</code> to this set, without creating a
     * {@link net.sf.saffron.opt.RelSubset}.  (Called only from {@link
     * net.sf.saffron.opt.RelSubset#add}.
     */
    void addInternal(SaffronRel rel)
    {
        if (!rels.contains(rel)) {
            rels.add(rel);
        }
        if (this.rel == null) {
            this.rel = rel;
        } else {
            assert(rel.getCorrelVariable() == null);
            String correl = this.rel.getCorrelVariable();
            if (correl != null) {
                rel.setCorrelVariable(correl);
            }
            if (this.rel.getRowType() != rel.getRowType()) {
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
    void mergeWith(VolcanoPlanner planner,RelSet otherSet)
    {
        assert(this != otherSet);
        assert(this.equivalentSet == null);
        assert(otherSet.equivalentSet == null);
        DebugOut.println("Merge set#" + otherSet.id + " into set#" + id);
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
                    otherSubset.getConvention());
            if (otherSubset.bestCost.isLt(subset.bestCost)) {
                subset.bestCost = otherSubset.bestCost;
                subset.best = otherSubset.best;
            }
            for (int j = 0; j < otherSubset.rels.size(); j++) {
                planner.reregister(this,(SaffronRel) otherSubset.rels.get(j));
            }
        }
        for (
            Iterator parentRels = otherSet.getParentRels().iterator();
                parentRels.hasNext();) {
            planner.rename((SaffronRel) parentRels.next());
        }
    }

    private void failType(SaffronRel rel)
    {
        final SaffronType rowType = this.rel.getRowType();
        final SaffronType relRowType = rel.getRowType();
        String s =
            "Cannot add expression of different type to set: "
            + Util.lineSeparator + "set type is " + rowType
            + Util.lineSeparator + "expression type is " + relRowType
            + Util.lineSeparator + "set is " + toString() + Util.lineSeparator
            + "expression is " + rel.toString();
        throw Util.newInternal(s);
    }
}


// End RelSet.java
