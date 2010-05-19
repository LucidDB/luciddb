/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;


/**
 * Rule to convert a {@link UnionRel} to {@link FennelRel#FENNEL_EXEC_CONVENTION
 * Fennel calling convention}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelUnionRule
    extends ConverterRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FennelUnionRule instance = new FennelUnionRule();

    //~ Constructors -----------------------------------------------------------

    public FennelUnionRule()
    {
        super(
            UnionRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelUnionRule");
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode convert(RelNode rel)
    {
        final UnionRel unionRel = (UnionRel) rel;
        if (!unionRel.isHomogeneous()) {
            // Fennel's MergeExecStream only operates on homogeneous inputs;
            // we'll try again once {@link CoerceInputsRule} has taken
            // care of that.
            return null;
        }
        if (unionRel.isDistinct()) {
            // can only convert non-distinct Union; we'll try again once {@link
            // UnionToDistinctRule} and {@link FennelDistinctSortRule} have
            // taken care of that.
            return null;
        }
        RelNode [] newInputs = new RelNode[unionRel.getInputs().length];
        for (int i = 0; i < newInputs.length; i++) {
            newInputs[i] =
                mergeTraitsAndConvert(
                    unionRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    unionRel.getInput(i));
            if (newInputs[i] == null) {
                return null; // cannot convert this input
            }
        }
        return new FennelMergeRel(
            unionRel.getCluster(),
            newInputs);
    }
}

// End FennelUnionRule.java
