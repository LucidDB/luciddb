/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.query;

import java.util.Arrays;
import java.util.List;

import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;


/**
 * FennelRemoveRedundantSortRule removes instances of SortRel which are
 * already satisfied by the physical ordering produced by an underlying
 * FennelPullRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRemoveRedundantSortRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public FennelRemoveRedundantSortRule()
    {
        super(new RelOptRuleOperand(
                FennelSortRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FennelPullRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FennelSortRel sortRel = (FennelSortRel) call.rels[0];
        FennelRel inputRel = (FennelRel) call.rels[1];

        if (!isSortRedundant(sortRel, inputRel)) {
            return;
        }

        if (inputRel instanceof FennelSortRel) {
            RelNode newRel =
                convert(inputRel, FennelPullRel.FENNEL_PULL_CONVENTION);
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
        FennelSortRel sortRel,
        FennelRel inputRel)
    {
        if (sortRel.discardDuplicates) {
            // TODO:  once we can obtain the key for a RelNode, check
            // that
            return false;
        }

        RelFieldCollation [] inputCollationArray = inputRel.getCollations();
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
            inputCollationList =
                inputCollationList.subList(0, outputCollationArray.length);
        }
        return inputCollationList.equals(outputCollationList);
    }
}


// End FennelRemoveRedundantSortRule.java
