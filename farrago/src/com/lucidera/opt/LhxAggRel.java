/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;
import org.eigenbase.stat.RelStatColumnStatistics;
import org.eigenbase.stat.RelStatSource;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.query.*;

/**
 * LhxAggRel represents hash aggregation.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxAggRel extends AggregateRelBase implements FennelRel
{
    public LhxAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(
            cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child, groupCount, aggCalls);
    }

    public Object clone()
    {
        LhxAggRel clone = new LhxAggRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            groupCount,
            aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO jvs 5-Oct-2005: if group keys are already sorted,
        // non-hash agg will preserve them; full-table agg is
        // trivially sorted (only one row of output)
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(this, 0, getChild());
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemLhxAggStreamDef aggStream = repos.newFemLhxAggStreamDef();
        aggStream.setGroupingPrefixSize(groupCount);
        for (int i = 0; i < aggCalls.length; ++i) {
            Call call = aggCalls[i];
            assert(!call.isDistinct());
            // allow 0 for COUNT(*)
            assert(call.args.length <= 1);
            AggFunction func = lookupAggFunction(call);
            FemAggInvocation aggInvocation = repos.newFemAggInvocation();
            aggInvocation.setFunction(func);
            if (call.args.length == 1) {
                aggInvocation.setInputAttributeIndex(call.args[0]);
            } else {
                // COUNT(*) ignores input
                aggInvocation.setInputAttributeIndex(-1);
            }
            aggStream.getAggInvocation().add(aggInvocation);
        }
        
        RelNode child = getChild();
        Double numRows = RelMetadataQuery.getRowCount(child);
        if (numRows == null) {
            numRows = 10000.0;
        }
        aggStream.setNumRows(numRows.intValue());
        
        RelStatSource statSource = RelMetadataQuery.getStatistics(child);

        // Derive cardinality of RHS join keys.
        Double cndKeys = 1.0;
        double correlationFactor = 0.7;
        RelStatColumnStatistics colStat;
        Double cndCol;
        
        for (int i = 0; i < getGroupCount(); i ++) {
            cndCol = null;
            if (statSource != null) {
                colStat = statSource.getColumnStatistics(i, null);
                if (colStat != null) {
                    cndCol = colStat.getCardinality();
                }
            }

            if (cndCol == null) {
                // default to 100 distinct values for a column
                cndCol = 100.0;
            }
            
            cndKeys *= cndCol;
            
            // for each additional key, apply the correlationFactor.
            if (i > 0) {
                cndKeys *= correlationFactor;
            }
            
            // cndKeys can be at most equal to number of rows from the build
            // side.
            if (cndKeys > numRows) {
                cndKeys = numRows;
                break;
            }
        }
        aggStream.setCndGroupByKeys(cndKeys.intValue());

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()), 
            aggStream);
        
        return aggStream;
    }

    public static AggFunction lookupAggFunction(
        AggregateRel.Call call)
    {
        return AggFunctionEnum.forName(
            "AGG_FUNC_" + call.getAggregation().getName());
    }
}

// End LhxAggRel.java
