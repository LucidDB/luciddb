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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * A match of a rule to a particular set of target relational expressions,
 * frozen in time.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 14, 2003
 */
class VolcanoRuleMatch
    extends VolcanoRuleCall
{
    //~ Instance fields --------------------------------------------------------

    private final RelSet targetSet;
    private RelSubset targetSubset;
    private String digest;
    private final VolcanoPlanner volcanoPlanner;
    private double cachedImportance = Double.NaN;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>VolcanoRuleMatch</code>.
     *
     * @param operand0 Primary operand
     * @param rels List of targets; copied by the constructor, so the client can
     * modify it later
     *
     * @pre rels[i] != null
     */
    VolcanoRuleMatch(
        VolcanoPlanner volcanoPlanner,
        RelOptRuleOperand operand0,
        RelNode [] rels)
    {
        super(volcanoPlanner, operand0, (RelNode []) rels.clone());
        this.volcanoPlanner = volcanoPlanner;
        for (int i = 0; i < rels.length; i++) {
            assert (rels[i] != null);
        }

        // Try to deduce which subset the result will belong to. Assume --
        // for now -- that the set is the same as the root relexp.
        targetSet = volcanoPlanner.getSet(rels[0]);
        assert targetSet != null : rels[0].toString() + " isn't in a set";
        digest = computeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    public String toString()
    {
        return digest;
    }

    /**
     * Clears the cached importance value of this rule match. The importance
     * will be re-calculated next time {@link #getImportance()} is called.
     */
    void clearCachedImportance()
    {
        cachedImportance = Double.NaN;
    }

    /**
     * Returns the importance of this rule.
     *
     * <p>Calls {@link #computeImportance()} the first time, thereafter uses a
     * cached value until {@link #clearCachedImportance()} is called.
     *
     * @return importance of this rule; a value between 0 and 1
     */
    double getImportance()
    {
        if (Double.isNaN(cachedImportance)) {
            cachedImportance = computeImportance();
        }

        return cachedImportance;
    }

    /**
     * Computes the importance of this rule match.
     *
     * @return importance of this rule match
     */
    double computeImportance()
    {
        assert rels[0] != null;
        RelSubset subset = volcanoPlanner.getSubset(rels[0]);
        double importance = 0;
        if (subset != null) {
            importance = volcanoPlanner.ruleQueue.getImportance(subset);
        }
        final RelSubset targetSubset = guessSubset();
        if ((targetSubset != null) && (targetSubset != subset)) {
            // If this rule will generate a member of an equivalence class
            // which is more important, use that importance.
            final double targetImportance =
                volcanoPlanner.ruleQueue.getImportance(targetSubset);
            if (targetImportance > importance) {
                importance = targetImportance;

                // If the equivalence class is cheaper than the target, bump up
                // the importance of the rule. A converter is an easy way to
                // make the plan cheaper, so we'd hate to miss this opportunity.
                //
                //
                // REVIEW: jhyde, 2007/12/21: This rule seems to make sense, but
                // is disabled until it has been proven.
                if ((subset != null)
                    && subset.bestCost.isLt(targetSubset.bestCost)
                    && false)
                {
                    importance *=
                        targetSubset.bestCost.divideBy(subset.bestCost);
                    importance = Math.min(importance, 0.99);
                }
            }
        }

        return importance;
    }

    /**
     * Computes a string describing this rule match. Two rule matches are
     * equivalent if and only if their digests are the same.
     *
     * @return description of this rule match
     */
    private String computeDigest()
    {
        StringBuilder buf =
            new StringBuilder("rule [" + getRule() + "] rels [");
        for (int i = 0; i < rels.length; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(rels[i].toString());
        }
        buf.append("]");
        return buf.toString();
    }

    /**
     * Recomputes the digest of this VolcanoRuleMatch. It is necessary when sets
     * have merged since the match was created.
     */
    public void recomputeDigest()
    {
        digest = computeDigest();
    }

    /**
     * Returns a guess as to which subset (that is equivalence class of
     * relational expressions combined with a set of physical traits) the result
     * of this rule will belong to.
     *
     * @return expected subset, or null if we cannot guess
     */
    private RelSubset guessSubset()
    {
        if (targetSubset != null) {
            return targetSubset;
        }
        final RelTrait targetTrait = getRule().getOutTrait();
        if ((targetSet != null) && (targetTrait != null)) {
            final RelTraitSet targetTraitSet = rels[0].getTraits().clone();
            targetTraitSet.setTrait(targetTrait.getTraitDef(), targetTrait);

            // Find the subset in the target set which matches the expected
            // set of traits. It may not exist yet.
            targetSubset = targetSet.getSubset(targetTraitSet);
            return targetSubset;
        }

        // The target subset doesn't exist yet.
        return null;
    }
}

// End VolcanoRuleMatch.java
