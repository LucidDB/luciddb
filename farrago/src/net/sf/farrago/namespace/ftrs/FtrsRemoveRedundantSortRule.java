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

package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.util.*;
import net.sf.farrago.query.*;

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * FtrsRemoveRedundantSortRule removes instances of SortRel which are already
 * satisfied by the physical ordering produced by an underlying
 * FtrsIndexScanRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsRemoveRedundantSortRule extends RelOptRule
{
    public FtrsRemoveRedundantSortRule()
    {
        super(
            new RelOptRuleOperand(
                FennelSortRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FtrsIndexScanRel.class,null) }));
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FennelSortRel sortRel = (FennelSortRel) call.rels[0];
        FtrsIndexScanRel scanRel = (FtrsIndexScanRel) call.rels[1];

        if (!FennelRemoveRedundantSortRule.isSortRedundant(sortRel,scanRel)) {
            return;
        }

        // make sure scan order is preserved, since now we're relying
        // on it
        FtrsIndexScanRel sortedScanRel = new FtrsIndexScanRel(
            scanRel.getCluster(),
            scanRel.ftrsTable,
            scanRel.index,
            scanRel.getConnection(),
            scanRel.projectedColumns,
            true);
        call.transformTo(sortedScanRel);
    }
}

// End FtrsRemoveRedundantSortRule.java
