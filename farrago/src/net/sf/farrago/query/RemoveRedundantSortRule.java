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

import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;

import java.util.*;

/**
 * RemoveRedundantSortRule removes instances of SortRel which are already
 * satisfied by the physical ordering produced by an underlying FennelRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class RemoveRedundantSortRule extends VolcanoRule
{
    public RemoveRedundantSortRule()
    {
        super(
            new RuleOperand(
                FennelSortRel.class,
                new RuleOperand [] {
                    new RuleOperand(FennelRel.class,null) }));
    }

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_CALLING_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        FennelSortRel sortRel = (FennelSortRel) call.rels[0];
        FennelRel inputRel = (FennelRel) call.rels[1];

        if (!isSortRedundant(sortRel,inputRel)) {
            return;
        }

        if (inputRel instanceof FennelIndexScanRel) {
            // make sure scan order is preserved, since now we're relying
            // on it
            FennelIndexScanRel scanRel = (FennelIndexScanRel) inputRel;
            FennelIndexScanRel sortedScanRel = new FennelIndexScanRel(
                scanRel.getCluster(),
                scanRel.fennelTable,
                scanRel.index,
                scanRel.getConnection(),
                scanRel.projectedColumns,
                true);
            call.transformTo(sortedScanRel);
        } else if (inputRel instanceof FennelSortRel) {
            SaffronRel newRel =
                convert(planner,(SaffronRel) inputRel,
                        FennelRel.FENNEL_CALLING_CONVENTION);
            if (newRel == null) {
                return;
            }
            call.transformTo(newRel);
        } else {
            // REVIEW: don't blindly eliminate sort without know what aspects
            // of the input we're relying on?
        }
    }

    public boolean isSortRedundant(FennelSortRel sortRel,FennelRel inputRel)
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

// End RemoveRedundantSortRule.java
