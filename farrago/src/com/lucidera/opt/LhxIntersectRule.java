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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import net.sf.farrago.query.FennelRel;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;

/**
 * LhxIntersectRule is a rule for transforming {@link IntersectRel} to
 * {@link LhxJoinRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxIntersectRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new LhxIntersectRule object.
     */
    public LhxIntersectRule()
    {
        super(new RelOptRuleOperand(
                IntersectRel.class,
                null));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        IntersectRel intersectRel = (IntersectRel) call.rels[0];
        
        // TODO: intersect all
        assert (intersectRel.isDistinct());
        
        RelNode leftRel = intersectRel.getInputs()[0];
        List<String> newJoinOutputNames =
        	RelOptUtil.getFieldNameList(leftRel.getRowType());
        
        // make up the condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
    
        for (int i = 0; i < leftRel.getRowType().getFieldCount(); i ++) {
            leftKeys.add(i);
            rightKeys.add(i);
        }
        
        for (int inputNo = 1; inputNo < intersectRel.getInputs().length;
             inputNo ++) {        
            // perform pair-wise intersect
            RelNode rightRel = intersectRel.getInputs()[inputNo];
            
            // TODO: casting
            assert (leftRel.getRowType() == rightRel.getRowType());
                
            RelNode fennelLeft =
                mergeTraitsAndConvert(
                    intersectRel.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                    leftRel);
        
            if (fennelLeft == null) {
                return;
            }

            RelNode fennelRight =
                mergeTraitsAndConvert(
                    intersectRel.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                    rightRel);
        
            if (fennelRight == null) {
                return;
            }
            
            Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);
            if (numBuildRows == null) {
                numBuildRows = 10000.0;
            }

            // Derive cardinality of build side(RHS) join keys.
            Double cndBuildKey;
            BitSet joinKeyMap = new BitSet();
            
            for (int i = 0; i < rightKeys.size(); i ++) {
                joinKeyMap.set(rightKeys.get(i));
            }
            
            cndBuildKey = RelMetadataQuery.getPopulationSize(
                fennelRight, joinKeyMap);
            
            if ((cndBuildKey == null) || (cndBuildKey > numBuildRows)) {
                cndBuildKey = numBuildRows;
            }
            
            boolean isSetop = true;
            
            leftRel =
                new LhxJoinRel(
                    intersectRel.getCluster(),
                    fennelLeft,
                    fennelRight,
                    LhxJoinRelType.LEFTSEMI,
                    isSetop,
                    leftKeys,
                    rightKeys,
                    newJoinOutputNames,                    
                    numBuildRows.intValue(),
                    cndBuildKey.intValue());
            
        }
        call.transformTo(leftRel);
    }    
}

// End LhxIntersectRule.java
