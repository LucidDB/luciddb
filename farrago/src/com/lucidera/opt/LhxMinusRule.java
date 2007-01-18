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

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * LhxMinusRule is a rule for transforming {@link MinusRel} to {@link
 * LhxJoinRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxMinusRule
    extends RelOptRule
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LhxIntersectRule object.
     */
    public LhxMinusRule()
    {
        super(new RelOptRuleOperand(
                MinusRel.class,
                null));
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
        MinusRel minusRel = (MinusRel) call.rels[0];

        // TODO: minus all
        assert (minusRel.isDistinct());

        // implement minus as a "right anti" join;
        // switch the input sides here.
        RelNode leftRel = minusRel.getInputs()[0];
        List<String> newJoinOutputNames = new ArrayList<String>();
        newJoinOutputNames.addAll(
            RelOptUtil.getFieldNameList(leftRel.getRowType()));

        // make up the condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();

        // an empty array means no filtering of null values
        // i.e. a null value is considered to match another null value
        List<Integer> filterNulls = new ArrayList<Integer>();

        for (int i = 0; i < leftRel.getRowType().getFieldCount(); i++) {
            leftKeys.add(i);
            rightKeys.add(i);
        }

        for (int inputNo = 1; inputNo < minusRel.getInputs().length;
            inputNo++) {
            // perform pair-wise minus
            RelNode rightRel = minusRel.getInputs()[inputNo];

            // TODO: casting
            assert (leftRel.getRowType() == rightRel.getRowType());

            RelNode fennelLeft =
                mergeTraitsAndConvert(
                    minusRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    leftRel);

            if (fennelLeft == null) {
                return;
            }

            RelNode fennelRight =
                mergeTraitsAndConvert(
                    minusRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    rightRel);

            if (fennelRight == null) {
                return;
            }

            Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);

            // implement minus as a "right anti" join;
            // derive cardinality of builde side(LHS) join keys.
            Double cndBuildKey;
            BitSet joinKeyMap = new BitSet();

            for (int i = 0; i < leftKeys.size(); i++) {
                joinKeyMap.set(leftKeys.get(i));
            }

            cndBuildKey =
                RelMetadataQuery.getPopulationSize(
                    fennelLeft,
                    joinKeyMap);

            if (cndBuildKey == null) {
                cndBuildKey = -1.0;
            }

            // implement minus as a "right anti" join;
            // switch the input sides here.
            boolean isSetop = true;
            leftRel =
                new LhxJoinRel(
                    minusRel.getCluster(),
                    fennelRight,
                    fennelLeft,
                    LhxJoinRelType.RIGHTANTI,
                    isSetop,
                    rightKeys,
                    leftKeys,
                    filterNulls,
                    newJoinOutputNames,
                    numBuildRows.longValue(),
                    cndBuildKey.longValue());
        }
        call.transformTo(leftRel);
    }
}

// End LhxMinusRule.java
