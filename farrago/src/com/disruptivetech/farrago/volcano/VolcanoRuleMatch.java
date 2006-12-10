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
        assert targetSet != null;
        digest = computeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    public String toString()
    {
        return digest;
    }

    void clearCachedImportance()
    {
        cachedImportance = Double.NaN;
    }
    
    double getImportance()
    {
        if (Double.isNaN(cachedImportance)) {
            cachedImportance = computeImportance();
        }
        
        return cachedImportance;
    }
    
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
            importance = Math.max(targetImportance, importance);
        }

        return importance;
    }

    private String computeDigest()
    {
        StringBuilder buf = new StringBuilder("rule [" + getRule() + "] rels [");
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
     * Recomputes the digest of this VolcanoRuleMatch. It is necessary when
     * sets have merged since the match was created.
     */
    public void recomputeDigest()
    {
        digest = computeDigest();
    }

    private RelSubset guessSubset()
    {
        if (targetSubset != null) {
            return targetSubset;
        }
        final RelTraitSet targetTraits = getRule().getOutTraits();
        if ((targetSet != null) && (targetTraits.size() > 0)) {
            targetSubset = targetSet.getSubset(targetTraits);
            if (targetSubset != null) {
                return targetSubset;
            }
        }

        // The target subset doesn't exist yet.
        return null;
    }
}

// End VolcanoRuleMatch.java
