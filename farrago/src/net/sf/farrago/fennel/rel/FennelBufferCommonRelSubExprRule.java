/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.fennel.rel;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * FennelBufferCommonRelSubExprRule is a rule that places a Fennel buffering
 * node on top of  a common relational subexpression, provided it makes sense
 * to do so from a cost perspective.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelBufferCommonRelSubExprRule
    extends CommonRelSubExprRule
{
    public static final FennelBufferCommonRelSubExprRule instance =
        new FennelBufferCommonRelSubExprRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * @deprecated use {@link #instance} instead
     */
    public FennelBufferCommonRelSubExprRule()
    {
        super(
            new RelOptRuleOperand(
                RelNode.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        RelNode rel = call.rels[0];
        if (rel instanceof FennelMultiUseBufferRel) {
            return;
        }

        // Compare the cost of not buffering vs buffering.
        //
        // Non Buffering Cost = getCumulativeCost(rel) * (# common subexprs)
        // Buffering Cost = getCumulativeCost(rel) +
        //    N * (cost of writing buffer) +
        //    (# common subexprs) * (cost of reading buffer)
        // Cost of reading and writing buffer = getNonCumulativeCost(rel)
        // N represents the overhead of caching, and is arbitrarily set to the
        // value 2.
        //
        // REVIEW zfong 3/19/09 - Should we avoid using buffering if the size
        // of the buffered result exceeds some threshold?
        FennelMultiUseBufferRel bufRel =
            new FennelMultiUseBufferRel(rel.getCluster(), rel, false);
        RelOptCost bufferCost = RelMetadataQuery.getNonCumulativeCost(bufRel);

        int nCommonSubExprs = call.getParents().size();
        RelOptCost commonSubExprCost = RelMetadataQuery.getCumulativeCost(rel);

        RelOptCost bufferPlanCost = bufferCost.multiplyBy(nCommonSubExprs + 2);
        RelOptCost nonBufferPlanCost =
            commonSubExprCost.multiplyBy(nCommonSubExprs - 1);
        if (nonBufferPlanCost.isLt(bufferPlanCost)) {
            return;
        }

        call.transformTo(bufRel);
    }
}

// End FennelBufferCommonRelSubExprRule.java
