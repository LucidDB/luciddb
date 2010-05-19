/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 Dynamo BI Corporation
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
 * FennelBernoulliSamplingRule converts a {@link SamplingRel} into a {@link
 * FennelBernoulliSamplingRel}, regardless of whether the original SamplingRel
 * specified Bernoulli or system sampling. By default Farrago doesn't not
 * support system sampling.
 *
 * @author Stephan Zuercher
 */
public class FennelBernoulliSamplingRule
    extends RelOptRule
{
    public static final FennelBernoulliSamplingRule instance =
        new FennelBernoulliSamplingRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelBernoulliSamplingRule.
     */
    private FennelBernoulliSamplingRule()
    {
        super(new RelOptRuleOperand(SamplingRel.class, ANY));
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
        SamplingRel origRel = (SamplingRel) call.rels[0];

        RelOptSamplingParameters origParams = origRel.getSamplingParameters();

        RelOptSamplingParameters params =
            new RelOptSamplingParameters(
                true,
                origParams.getSamplingPercentage(),
                origParams.isRepeatable(),
                origParams.getRepeatableSeed());

        FennelBernoulliSamplingRel samplingRel =
            new FennelBernoulliSamplingRel(
                origRel.getCluster(),
                origRel.getChild(),
                params);

        call.transformTo(samplingRel);
    }
}

// End FennelBernoulliSamplingRule.java
