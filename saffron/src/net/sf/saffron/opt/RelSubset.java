/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import net.sf.saffron.core.*;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.rel.RelVisitor;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;

import openjava.tools.DebugOut;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


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
public class RelSubset extends SaffronRel
{
    //~ Instance fields -------------------------------------------------------

    /** List of the relational expressions for which this subset is an input. */
    ArrayList parents;
    ArrayList rels;
    CallingConvention convention;

    /** cost of best known plan (it may have improved since) */
    PlanCost bestCost;
    RelSet set;

    /** best known plan */
    SaffronRel best;

    /** whether findBestPlan is being called */
    boolean active;

    //~ Constructors ----------------------------------------------------------

    RelSubset(VolcanoCluster cluster,RelSet set,CallingConvention convention)
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

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return convention;
    }

    /**
     * There are no children, as such.  We throw an exception because you
     * probably don't want to be walking over trees which contain
     * <code>RelSet</code>s.
     */
    public SaffronRel [] getInputs()
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

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        return planner.makeZeroCost();
    }

    // implement SaffronRel
    public void explain(PlanWriter pw)
    {
        pw.explainSubset(
            id + ": RelSubset(" + convention + ")",
            (SaffronRel) rels.get(0));
    }

    protected String computeDigest()
    {
        return "Subset#" + set.id + "." + convention;
    }

    // implement SaffronRel
    protected SaffronType deriveRowType()
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
            SaffronRel rel = (SaffronRel) iterator.next();
            final RelSubset subset = cluster.planner.getSubset(rel);
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
    void add(SaffronRel rel)
    {
        if (rels.contains(rel)) {
            return;
        }
        rels.add(rel);
        set.addInternal(rel);
        Set variablesSet = OptUtil.getVariablesSet(rel);
        Set variablesStopped = rel.getVariablesStopped();
        Set variablesPropagated = Util.minus(variablesSet,variablesStopped);
        assert(
            set.variablesPropagated.containsAll(variablesPropagated));
        Set variablesUsed = OptUtil.getVariablesUsed(rel);
        assert(set.variablesUsed.containsAll(variablesUsed));
        propagateCostImprovements(rel.getCluster().planner,rel);
    }

    /**
     * Recursively build a tree consisting of the cheapest plan at each node.
     */
    SaffronRel buildCheapestPlan(SaffronPlanner planner)
    {
        CheapestPlanReplacer replacer = new CheapestPlanReplacer(planner);
        SaffronRel cheapest = OptUtil.go(replacer,this);
        return cheapest;
    }

    /**
     * Checks whether a relexp has made its subset cheaper, and if it so,
     * recursively checks whether that subset's parents have gotten cheaper.
     */
    private void propagateCostImprovements(
        VolcanoPlanner planner,
        SaffronRel rel)
    {
        final PlanCost cost = planner.getCost(rel);
        if (cost.isLt(bestCost)) {
            if (DebugOut.getDebugLevel() > 2) {
                DebugOut.println(
                    "Subset cost improved: subset [" + this + "] cost was "
                    + bestCost + " now " + cost);
            }
            bestCost = cost;
            best = rel;

            // Lower cost means lower importance. Other nodes will change
            // too, but we'll get to them later.
            planner.ruleQueue.recompute(this);
            for (int i = 0; i < parents.size(); i++) {
                SaffronRel parent = (SaffronRel) parents.get(i);
                final RelSubset parentSubset = planner.getSubset(parent);
                parentSubset.propagateCostImprovements(planner,parent);
            }
            planner.checkForSatisfiedConverters(set,rel);
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Visitor which walks over a tree of {@link RelSet}s, replacing each node
     * with the cheapest implementation of the expression.
     */
    class CheapestPlanReplacer extends RelVisitor
    {
        SaffronPlanner planner;

        CheapestPlanReplacer(SaffronPlanner planner)
        {
            super();
            this.planner = planner;
        }

        public void visit(SaffronRel p,int ordinal,SaffronRel parent)
        {
            if (p instanceof RelSubset) {
                RelSubset subset = (RelSubset) p;
                SaffronRel cheapest = subset.best;
                if (cheapest == null) {
                    throw Util.newInternal(
                        "node could not be implemented: " + subset.toString());
                }
                parent.replaceInput(ordinal,cheapest);
                p = cheapest;
            }
            p.childrenAccept(this);
        }
    }
}


// End RelSubset.java
