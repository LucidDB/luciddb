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

/**
 * A match of a rule to a particular set of target relational expressions,
 * frozen in time.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Jun 14, 2003
 */
class VolcanoRuleMatch extends VolcanoRuleCall
{
    //~ Instance fields -------------------------------------------------------

    final RelSet targetSet;
    RelSubset targetSubset;
    private final String digest;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>VolcanoRuleMatch</code>.
     *
     * @param operand0 Primary operand
     * @param rels List of targets; copied by the constructor, so the client
     *        can modify it later
     *
     * @pre rels[i] != null
     */
    VolcanoRuleMatch(RuleOperand operand0,SaffronRel [] rels)
    {
        super(operand0,(SaffronRel []) rels.clone());
        for (int i = 0; i < rels.length; i++) {
            assert(rels[i] != null);
        }

        // Try to deduce which subset the result will belong to. Assume --
        // for now -- that the set is the same as the root relexp.
        targetSet = rule.planner.getSet(rels[0]);
        assert targetSet != null;
        digest = computeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    public String toString()
    {
        return digest;
    }

    double computeImportance()
    {
        final VolcanoPlanner planner = rule.planner;
        assert rels[0] != null;
        RelSubset subset = planner.getSubset(rels[0]);
        double importance = 0;
        if (subset != null) {
            importance = planner.ruleQueue.getImportance(subset);
        }
        final RelSubset targetSubset = guessSubset();
        if ((targetSubset != null) && (targetSubset != subset)) {
            // If this rule will generate a member of an equivalence class
            // which is more important, use that importance.
            final double targetImportance =
                planner.ruleQueue.getImportance(targetSubset);
            importance = Math.max(targetImportance,importance);
        }
        return importance;
    }

    private String computeDigest()
    {
        StringBuffer buf = new StringBuffer("rule [" + rule + "] rels [");
        for (int i = 0; i < rels.length; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(rels[i].toString());
        }
        buf.append("]");
        return buf.toString();
    }

    private RelSubset guessSubset()
    {
        if (targetSubset != null) {
            return targetSubset;
        }
        final CallingConvention targetConvention = rule.getOutConvention();
        if ((targetSet != null) && (targetConvention != null)) {
            targetSubset = targetSet.getSubset(targetConvention);
            if (targetSubset != null) {
                return targetSubset;
            }
        }

        // The target subset doesn't exist yet.
        return null;
    }
}


// End VolcanoRuleMatch.java
