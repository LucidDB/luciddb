/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FtrsIndexBuilderRule is a rule for converting an abstract {@link
 * net.sf.farrago.query.FarragoIndexBuilderRel} into a corresponding {@link
 * net.sf.farrago.namespace.ftrs.FtrsIndexBuilderRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FtrsIndexBuilderRule
    extends RelOptRule
{
    public static final FtrsIndexBuilderRule instance =
        new FtrsIndexBuilderRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FtrsIndexBuilderRule.
     */
    private FtrsIndexBuilderRule()
    {
        super(
            new RelOptRuleOperand(
                FarragoIndexBuilderRel.class,
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
        FarragoIndexBuilderRel builderRel =
            (FarragoIndexBuilderRel) call.rels[0];

        if (!(builderRel.getTable() instanceof FtrsTable)) {
            return;
        }

        RelNode inputRel = builderRel.getChild();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                call.rels[0].getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                inputRel);
        if (fennelInput == null) {
            return;
        }

        FtrsIndexBuilderRel ftrsRel =
            new FtrsIndexBuilderRel(
                builderRel.getCluster(),
                fennelInput,
                builderRel.getIndex());

        call.transformTo(ftrsRel);
    }
}

// End FtrsIndexBuilderRule.java
