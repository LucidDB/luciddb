/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.query;

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rel.RelFieldCollation;
import net.sf.saffron.rel.SaffronRel;

import java.util.Arrays;
import java.util.List;

/**
 * FennelRemoveRedundantSortRule removes instances of SortRel which are
 * already satisfied by the physical ordering produced by an underlying
 * FennelPullRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRemoveRedundantSortRule extends VolcanoRule
{
    public FennelRemoveRedundantSortRule()
    {
        super(
            new RuleOperand(
                FennelSortRel.class,
                new RuleOperand [] {
                    new RuleOperand(FennelPullRel.class,null) }));
    }

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        FennelSortRel sortRel = (FennelSortRel) call.rels[0];
        FennelRel inputRel = (FennelRel) call.rels[1];

        if (!isSortRedundant(sortRel,inputRel)) {
            return;
        }

        if (inputRel instanceof FennelSortRel) {
            SaffronRel newRel =
                convert(planner,inputRel,FennelPullRel.FENNEL_PULL_CONVENTION);
            if (newRel == null) {
                return;
            }
            call.transformTo(newRel);
        } else {
            // REVIEW: don't blindly eliminate sort without know what aspects
            // of the input we're relying on?
        }
    }

    public static boolean isSortRedundant(
        FennelSortRel sortRel,FennelRel inputRel)
    {
        if (sortRel.discardDuplicates) {
            // TODO:  once we can obtain the key for a SaffronRel, check
            // that
            return false;
        }

        RelFieldCollation [] inputCollationArray =
            inputRel.getCollations();
        RelFieldCollation [] outputCollationArray = sortRel.getCollations();
        if (outputCollationArray.length > inputCollationArray.length) {
            // no way input more specific order can be satisfied by less
            // specific
            return false;
        }

        List inputCollationList = Arrays.asList(inputCollationArray);
        List outputCollationList = Arrays.asList(outputCollationArray);
        if (outputCollationArray.length < inputCollationArray.length) {
            // truncate for prefix comparison
            inputCollationList = inputCollationList.subList(
                0,
                outputCollationArray.length);
        }
        return inputCollationList.equals(outputCollationList);
    }
}

// End FennelRemoveRedundantSortRule.java
