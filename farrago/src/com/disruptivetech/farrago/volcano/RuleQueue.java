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
package com.disruptivetech.farrago.volcano;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.BinaryHeap;


/**
 * Priority queue of relexps whose rules have not been called, and
 * rule-matches which have not yet been acted upon.
 */
class RuleQueue
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields -------------------------------------------------------

    /** Maps {@link RelSubset} to {@link Double}. */
    HashMap subsetImportances = new HashMap();
    private final ArrayList matchList = new ArrayList();
    private final Comparator ruleMatchImportanceComparator =
        new RuleMatchImportanceComparator();
    private final HashSet matchNames = new HashSet();
    private final VolcanoPlanner planner;

    /** Compares relexps according to their cached 'importance'. */
    private Comparator relImportanceComparator = new RelImportanceComparator();
    private BinaryHeap relQueue =
        new BinaryHeap(true, relImportanceComparator);
    private HashSet rels = new HashSet();

    //~ Constructors ----------------------------------------------------------

    RuleQueue(VolcanoPlanner planner)
    {
        this.planner = planner;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Computes the importance a set (which is that of its most important
     * subset).
     */
    public double getImportance(RelSet set)
    {
        double importance = 0;
        for (int i = 0; i < set.subsets.size(); i++) {
            RelSubset subset = (RelSubset) set.subsets.get(i);
            importance = Math.max(
                    importance,
                    getImportance(subset));
        }
        return importance;
    }

    public boolean contains(RelNode rel)
    {
        return this.rels.contains(rel);
    }

    /**
     * Returns whether there is a rule match in the queue.
     */
    public boolean hasNextMatch()
    {
        return !matchList.isEmpty();
    }

    public void recompute(RelSubset subset)
    {
        final Double previousImportance =
            (Double) subsetImportances.get(subset);
        if (previousImportance == null) {
            // Subset has not been registered yet. Don't worry about it.
            return;
        }
        double importance = computeImportance(subset);
        if (previousImportance.doubleValue() == importance) {
            return;
        }
        relQueue.remove(subset);
        subsetImportances.put(
            subset,
            new Double(importance));
        relQueue.insert(subset);
    }

    boolean isEmpty()
    {
        //          return rels.isEmpty();
        return relQueue.isEmpty();
    }

    /**
     * Returns the importance of an equivalence class of relational
     * expressions. Subset importances are held in a lookup table, and
     * importance changes gradually propagate through that table.
     *
     * <p>
     * If a subset in the same set but with a different calling convention is
     * deemed to be important, then this subset has at least half of its
     * importance. (This rule is designed to encourage conversions to take
     * place.)
     * </p>
     */
    double getImportance(RelSubset rel)
    {
        assert rel != null;
        double importance = 0;
        final RelSet set = planner.getSet(rel);
        assert set != null;
        assert set.subsets != null;
        for (int i = 0; i < set.subsets.size(); i++) {
            RelSubset subset2 = (RelSubset) set.subsets.get(i);
            final Double d = (Double) subsetImportances.get(subset2);
            if (d == null) {
                continue;
            }
            double subsetImportance = d.doubleValue();
            if (subset2 != rel) {
                subsetImportance /= 2;
            }
            if (subsetImportance > importance) {
                importance = subsetImportance;
            }
        }
        return importance;
    }

    /**
     * Registers that a relational expression's rules have not been fired.
     */
    void add(RelNode rel)
    {
        assert (rels.add(rel)) : "RuleQueue already contained rel";
        final RelSubset subset = planner.getSubset(rel);
        assert (subset != null);
        add(subset);
    }

    /**
     * Registers a subset, if it has not already been registered.
     */
    void add(RelSubset subset)
    {
        if (subsetImportances.get(subset) == null) {
            final double importance = computeImportance(subset);
            final Double previousImportance =
                (Double) subsetImportances.put(
                    subset,
                    new Double(importance));
            assert (previousImportance == null);
            relQueue.insert(subset);
        }
    }

    /**
     * Adds a rule match.
     */
    void addMatch(VolcanoRuleMatch match)
    {
        final String matchName = match.toString();
        if (!matchNames.add(match)) {
            // Identical match has already been added.
            return;
        }
        tracer.finest("Rule-match queued: " + matchName);
        matchList.add(match);
    }

    /**
     * Computes the <dfn>importance</dfn> of a node. Importance is defined as
     * follows:
     *
     * <ul>
     * <li>
     * the root {@link RelSubset} has an importance of 1
     * </li>
     * <li>
     * the importance of any other subset is the sum of its importance to its
     * parents
     * </li>
     * <li>
     * The importance of children is pro-rated according to the cost of the
     * children. Consider a node which has a cost of 3, and children with
     * costs of 2 and 5. The total cost is 10. If the node has an importance
     * of .5, then the children will have importance of .1 and .25. The
     * retains .15 importance points, to reflect the fact that work needs to
     * be done on the node's algorithm.
     * </li>
     * </ul>
     *
     * The formula for the importance I of node n is:
     * <blockquote>
     * I<sub>n</sub> = Sum<sub>parents p of n</sub>{I<sub>p</sub> . W<sub>n,
     * p</sub>}
     * </blockquote>
     * where W<sub>n, p</sub>, the weight of n within its parent p, is
     * <blockquote>
     * W<sub>n, p</sub> = Cost<sub>n</sub> / (SelfCost<sub>p</sub> +
     * Cost<sub>n<sub>0</sub></sub> + ... + Cost<sub>n<sub>k</sub></sub>)
     * </blockquote>
     */
    double computeImportance(RelSubset subset)
    {
        double importance;
        if (subset == planner.root) {
            // The root always has importance = 1
            importance = 1.0;
        } else {
            // The importance of a subset is the max of its importance to its
            // parents
            importance = 0.0;
            final Set parentSubsets = subset.getParentSubsets();
            for (Iterator parents = parentSubsets.iterator();
                    parents.hasNext();) {
                RelSubset parent = (RelSubset) parents.next();
                final double childImportance =
                    computeImportanceOfChild(subset, parent);
                importance = Math.max(importance, childImportance);
            }
        }
        tracer.finest("Importance of [" + subset + "] is " + importance);
        return importance;
    }

    private void dump()
    {
        if (tracer.isLoggable(Level.FINER)) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            dump(pw);
            pw.flush();
            tracer.finer(sw.toString());
        }
    }

    private void dump(PrintWriter pw)
    {
        planner.dump(pw);
        pw.print("Importances: {");
        final RelSubset [] subsets =
            (RelSubset []) subsetImportances.keySet().toArray(
                new RelSubset[0]);
        Arrays.sort(subsets, relImportanceComparator);
        for (int i = 0; i < subsets.length; i++) {
            RelSubset subset = subsets[i];
            pw.print(" " + subset.toString() + "="
                + subsetImportances.get(subset));
        }
        pw.println("}");
    }

    RelNode findCheapestMember(RelSubset childSubset)
    {
        RelOptCost cheapestCost = null;
        RelNode cheapestRel = null;
        for (Iterator rels = childSubset.rels.iterator(); rels.hasNext();) {
            RelNode rel = (RelNode) rels.next();
            RelOptCost cost = planner.getCost(rel);
            if ((cheapestCost == null) || cost.isLt(cheapestCost)) {
                if (this.rels.contains(rel)) {
                    cheapestCost = cost;
                    cheapestRel = rel;
                }
            }
        }
        return cheapestRel;
    }

    /**
     * Returns the relational expression whose cost is highest
     *
     * @pre !isEmpty()
     * @post return != null
     */
    RelNode pop()
    {
        while (true) {
            dump();
            RelSubset subset = (RelSubset) relQueue.pop();
            if (!relQueue.isEmpty()) {
                RelSubset nextSubset = (RelSubset) relQueue.peek();
                double importance = computeImportance(subset);
                double nextImportance = computeImportance(nextSubset);
                if (nextImportance > importance) {
                    // The queue was out of order. Try it again.
                    subsetImportances.put(
                        subset,
                        new Double(importance));
                    subsetImportances.put(
                        nextSubset,
                        new Double(nextImportance));
                    relQueue.pop();
                    relQueue.insert(nextSubset);
                    relQueue.insert(subset);
                    continue;
                }
            }
            final RelNode cheapestMember = findCheapestMember(subset);
            if (cheapestMember != null) {
                relQueue.insert(subset); // put it back.. there may be more
                assert (rels.remove(cheapestMember)) : "candidate rel must be on rule queue";
                return cheapestMember;
            }

            // don't put subset back on the queue
            if (relQueue.isEmpty()) {
                return null;
            }
        }
    }

    /**
     * Removes the rule match with the highest importance, and returns it.
     *
     * @pre hasNextMatch()
     */
    VolcanoRuleMatch popMatch()
    {
        dump();
        assert (hasNextMatch());
        final Object [] matches = matchList.toArray();
        for (int i = 0; i < matches.length; i++) {
            assert matches[i] != null : i;
        }
        Arrays.sort(matches, ruleMatchImportanceComparator);
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Sorted rule queue:");
            for (int i = 0; i < matches.length; i++) {
                VolcanoRuleMatch match = (VolcanoRuleMatch) matches[i];
                final double importance = match.computeImportance();
                tracer.finest(match + " importance " + importance);
            }
        }
        final VolcanoRuleMatch match = (VolcanoRuleMatch) matches[0];
        matchList.remove(match);
        return match;
    }

    boolean remove(RelNode rel)
    {
        final boolean existed = rels.remove(rel);
        if (existed) {
            // Remove any matches which involve the obsolete relational expr.
            for (int i = 0; i < matchList.size(); i++) {
                VolcanoRuleMatch match = (VolcanoRuleMatch) matchList.get(i);
                if (matchContains(match, rel)) {
                    matchList.remove(i);
                    --i;
                }
            }
        }
        return existed;
    }

    private static boolean matchContains(
        VolcanoRuleMatch match,
        RelNode rel)
    {
        for (int j = 0; j < match.rels.length; j++) {
            if (match.rels[j] == rel) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the importance of a child to a parent. This is defined by the
     * importance of the parent, pro-rated by the cost of the child. For
     * example, if the parent has importance = 0.8 and cost 100, then a child
     * with cost 50 will have importance 0.4, and a child with cost 25 will
     * have importance 0.2.
     */
    private double computeImportanceOfChild(
        RelSubset child,
        RelSubset parent)
    {
        final double parentImportance = getImportance(parent);
        final double childCost = toDouble(planner.getCost(child));
        final double parentCost = toDouble(planner.getCost(parent));
        double alpha = (childCost / parentCost);
        if (alpha >= 1.0) {
            // child is always less important than parent
            alpha = 0.99;
        }
        final double importance = parentImportance * alpha;
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Importance of [" + child + "] to its parent ["
                + parent + "] is " + importance + " (parent importance="
                + parentImportance + ", child cost=" + childCost
                + ", parent cost=" + parentCost + ")");
        }
        return importance;
    }

    /**
     * Converts a cost to a scalar quantity.
     */
    private double toDouble(RelOptCost cost)
    {
        if (cost.isInfinite()) {
            // REVIEW:  shouldn't this be bigger?
            return 1e+3;
        } else {
            return cost.getCpu() + cost.getRows() + cost.getIo();
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Compares {@link RelNode} objects according to their cached
     * 'importance'.
     */
    private class RelImportanceComparator implements Comparator
    {
        public int compare(
            Object o1,
            Object o2)
        {
            RelSubset rel1 = (RelSubset) o1;
            RelSubset rel2 = (RelSubset) o2;
            double imp1 = getImportance(rel1);
            double imp2 = getImportance(rel2);
            return (imp1 < imp2) ? 1 : ((imp1 > imp2) ? (-1) : 0);
        }
    }

    /**
     * Compares {@link VolcanoRuleMatch} objects according to their
     * importance.
     */
    private class RuleMatchImportanceComparator implements Comparator
    {
        public int compare(
            Object o1,
            Object o2)
        {
            VolcanoRuleMatch match1 = (VolcanoRuleMatch) o1;
            VolcanoRuleMatch match2 = (VolcanoRuleMatch) o2;
            double imp1 = match1.computeImportance();
            double imp2 = match2.computeImportance();
            return (imp1 < imp2) ? 1 : ((imp1 > imp2) ? (-1) : 0);
        }
    }
}


// End RuleQueue.java
