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
import org.eigenbase.relopt.*;


/**
 * FennelSortRule is a rule for implementing SortRel via a Fennel sort.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelSortRule
    extends RelOptRule
{
    public static final FennelSortRule instance = new FennelSortRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * @deprecated use {@link #instance} instead
     */
    public FennelSortRule()
    {
        super(
            new RelOptRuleOperand(
                SortRel.class,
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
        SortRel sortRel = (SortRel) call.rels[0];
        RelNode relInput = sortRel.getChild();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                sortRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        boolean discardDuplicates = false;
        FennelSortRel fennelSortRel =
            new FennelSortRel(
                sortRel.getCluster(),
                fennelInput,
                sortRel.getCollations(),
                discardDuplicates);
        call.transformTo(fennelSortRel);
    }
}

// End FennelSortRule.java
