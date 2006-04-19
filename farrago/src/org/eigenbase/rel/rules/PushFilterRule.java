/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

/**
 * PushFilterRule implements the rule for pushing filters above and within
 * a join node into the join node and/or its children nodes.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushFilterRule extends RelOptRule
{
    private JoinRel joinRel;
    private List<RexNode> aboveFilters;
    private List<RexNode> joinFilters;
    private List<RexNode> leftFilters;
    private List<RexNode> rightFilters;
    private BitSet leftBitmap;
    private BitSet rightBitmap;
    private int nFieldsLeft;
    private int nTotalFields;
    private boolean filterPushed; 
    private RexBuilder rexBuilder;
    private RelDataTypeField[] joinFields;
    
    //  ~ Constructors --------------------------------------------------------

    public PushFilterRule()
    {
        super(new RelOptRuleOperand(
            FilterRel.class,
            new RelOptRuleOperand [] {
                new RelOptRuleOperand(JoinRel.class, null)
            }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        // no need to push filters for joins that have been converted back
        // from MultiJoinRels since filters have already been pushed to
        // the appropriate relnodes
        joinRel = (JoinRel) call.rels[1];
        if (joinRel.isMultiJoinDone()) {
            return;
        }
        
        aboveFilters = new ArrayList<RexNode>();
        FilterRel filterRel = (FilterRel) call.rels[0];
        RelOptUtil.decompCF(filterRel.getCondition(), aboveFilters);
        joinFilters = new ArrayList<RexNode>();
        RelOptUtil.decompCF(joinRel.getCondition(), joinFilters);
        leftFilters = new ArrayList<RexNode>();
        rightFilters = new ArrayList<RexNode>();
        
        nFieldsLeft = joinRel.getLeft().getRowType().getFields().length;
        joinFields = joinRel.getRowType().getFields();
        nTotalFields = joinFields.length;
        leftBitmap = new BitSet(nFieldsLeft);
        rightBitmap = new BitSet(nTotalFields - nFieldsLeft);
        rexBuilder = joinRel.getCluster().getRexBuilder();
        
        // set the reference bitmaps for the left and right children
        RelOptUtil.setRexInputBitmap(leftBitmap, 0, nFieldsLeft);
        RelOptUtil.setRexInputBitmap(rightBitmap, nFieldsLeft, nTotalFields);
        
        // TODO - add logic to derive additional filters.  E.g., from
        // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
        // derive table filters:
        // (t1.a = 1 OR t1.b = 3)
        // (t2.a = 2 OR t2.b = 4)
        
        // determine where the filters should be moved, if anywhere
        filterPushed = false;
        classifyFilters(
            aboveFilters, true, 
            !joinRel.getJoinType().generatesNullsOnLeft(),
            !joinRel.getJoinType().generatesNullsOnRight());
        classifyFilters(
            joinFilters, false,
            !joinRel.getJoinType().generatesNullsOnRight(),
            !joinRel.getJoinType().generatesNullsOnLeft());
        
        if (!filterPushed) {
            return;
        }
        
        // create FilterRels on top of the children if any filters were
        // pushed to them
        RelNode leftRel = createFilterOnRel(joinRel.getLeft(), leftFilters);
        RelNode rightRel = createFilterOnRel(joinRel.getRight(), rightFilters);
        
        // create the new join node referencing the new children and 
        // containing its new join filters (if there are any)
        RexNode joinFilter;
        if (joinFilters.size() == 0) {
            joinFilter = rexBuilder.makeLiteral(true);
        } else {
            joinFilter = RexUtil.andRexNodeList(rexBuilder, joinFilters);
        }
        RelNode newJoinRel = new JoinRel(
            joinRel.getCluster(), leftRel, rightRel, joinFilter,
            joinRel.getJoinType(), Collections.emptySet(),
            joinRel.isSemiJoinDone(), joinRel.isMultiJoinDone());
        
        // create a FilterRel on top of the join if needed
        RelNode newRel = createFilterOnRel(newJoinRel, aboveFilters);
        
        call.transformTo(newRel);
    }
    
    /**
     * Classifies filters according to where they should be processed.
     * They either stay where they are, are pushed to the join (if they
     * originated from above the join), or are pushed to one of the children.
     * 
     * @param filters filters to be classified
     * @param pushJoin true if filters originated from above the join
     * node
     * @param pushLeft true if filters can be pushed to the left
     * @param pushRight true if filters can be pushed to the right
     */
    private void classifyFilters(
        List<RexNode> filters, 
        boolean pushJoin,
        boolean pushLeft,
        boolean pushRight)
    {
        ListIterator filterIter = filters.listIterator();
        while (filterIter.hasNext()) {
            RexNode filter = (RexNode) filterIter.next();
            
            BitSet filterBitmap = new BitSet(nTotalFields);
            RelOptUtil.findRexInputRefs(filter, filterBitmap);
            
            // REVIEW - are there any expressions that need special handling
            // and therefore cannot be pushed?
            
            // filters can be pushed to the left child if the left child
            // does not generate NULLs and the only columns referenced in
            // the filter originate from the left child
            if (pushLeft && RelOptUtil.contains(leftBitmap, filterBitmap)) {
                filterPushed = true;
                // ignore filters that always evaluate to true
                if (!filter.isAlwaysTrue()) {
                    leftFilters.add(filter);
                }
                filterIter.remove();
                
            // filters can be pushed to the right child if the right child
            // does not generate NULLs and the only columns referenced in
            // the filter originate from the right child
            } else if (pushRight && RelOptUtil.contains(rightBitmap, filterBitmap)) {
                filterPushed = true;
                if (!filter.isAlwaysTrue()) {
                    // adjust the field references in the filter to reflect
                    // that fields in the right now shift over to the left
                    int[] adjustments = new int[nTotalFields];
                    for (int i = 0; i < nFieldsLeft; i++) {
                        adjustments[i] = 0;
                    }
                    for (int i = nFieldsLeft; i < nTotalFields; i++) {
                        adjustments[i] = -nFieldsLeft;
                    }
                    rightFilters.add(
                        RelOptUtil.convertRexInputRefs(
                            rexBuilder, filter, joinFields, adjustments));
                }
                filterIter.remove();
            
            // if the filter can't be pushed to either child and the join
            // is an inner join, push them to the join if they originated
            // from above the join
            } else if (joinRel.getJoinType() == JoinRelType.INNER &&
                pushJoin)
            {
                filterPushed = true;
                joinFilters.add(filter);
                filterIter.remove();
            }
            
            // else, leave the filter where it is
        }
    }

    /**
     * If the filter list passed in is non-empty, creates a FilterRel on
     * top of the existing RelNode; otherwise, just returns the RelNode
     * 
     * @param rel the RelNode that the filter will be put on top of
     * @param filters list of filters
     * @return new RelNode or existing one if no filters
     */
    private RelNode createFilterOnRel(RelNode rel, List<RexNode> filters)
    {
        RelNode newRel;
        
        if (filters.size() == 0) {
            newRel = rel;
        } else {
            RexNode andFilters = RexUtil.andRexNodeList(rexBuilder, filters);
            newRel = CalcRel.createFilter(rel, andFilters);
        }
        return newRel;
    }
}

// End PushFilterRule.java
