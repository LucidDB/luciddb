/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;

/**
 * FennelBernoulliSamplingRel implements Bernoulli-style table sampling using
 * a generic Fennel XO.
 *
 * @author Stephan Zuercher
 */
public class FennelBernoulliSamplingRel
    extends FennelSingleRel
{
    private final RelOptSamplingParameters samplingParams;

    public FennelBernoulliSamplingRel(
        RelOptCluster cluster, 
        RelNode child, 
        RelOptSamplingParameters samplingParams)
    {
        super(cluster, child);
        
        this.samplingParams = samplingParams;
        
        assert(samplingParams.isBernoulli());
    }

    public RelNode clone()
    {
        FennelBernoulliSamplingRel clone = 
            new FennelBernoulliSamplingRel(
                getCluster(),
                getChild().clone(),
                samplingParams);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "rate", "repeatableSeed" },
            new Object[] { 
                samplingParams.getSamplingPercentage(),
                samplingParams.isRepeatable()
                    ? samplingParams.getRepeatableSeed()
                    : "-"
            });
    }

    // implement RelNode
    public double getRows()
    {
        double rows = RelMetadataQuery.getRowCount(getChild());
        
        return rows * (double)samplingParams.getSamplingPercentage();
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemBernoulliSamplingStreamDef streamDef = 
            repos.newFemBernoulliSamplingStreamDef();
        
        FemExecutionStreamDef childInput =
            implementor.visitFennelChild((FennelRel) getChild(), 0);
        
        implementor.addDataFlowFromProducerToConsumer(
            childInput,
            streamDef);
        
        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getRexBuilder().getTypeFactory(),
                getRowType()));
        streamDef.setSamplingRate(samplingParams.getSamplingPercentage());
        streamDef.setRepeatable(samplingParams.isRepeatable());
        streamDef.setRepeatableSeed(samplingParams.getRepeatableSeed());
        
        return streamDef;
    }

}
