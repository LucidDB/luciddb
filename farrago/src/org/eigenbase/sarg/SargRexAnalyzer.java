/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.sarg;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;

import java.util.*;

/**
 * SargRexAnalyzer attempts to translate a rex predicate into
 * a {@link SargBinding}.  It assumes that the predicate expression
 * is already well-formed.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargRexAnalyzer
{
    private final SargFactory factory;

    private final Map<SqlOperator, CallConvertlet> convertletMap;

    private boolean failed;

    private RexInputRef boundInputRef;

    private RexNode coordinate;

    private boolean variableSeen;

    private boolean reverse;

    private List<SargExpr> exprStack;

    private List<RexNode> rexCFList;

    private List<RexNode> rexPostFilterList;

    private List<SargBinding> sargBindingList;

    private Map<SargExpr, RexNode> sarg2RexMap;

    SargRexAnalyzer(
        SargFactory factory)
    {
        this.factory = factory;

        convertletMap = new HashMap<SqlOperator, CallConvertlet>();

        registerConvertlet(
            SqlStdOperatorTable.equalsOperator,
            new ComparisonConvertlet(
                null, SargStrictness.CLOSED));

        registerConvertlet(
            SqlStdOperatorTable.lessThanOperator,
            new ComparisonConvertlet(
                SargBoundType.UPPER, SargStrictness.OPEN));

        registerConvertlet(
            SqlStdOperatorTable.lessThanOrEqualOperator,
            new ComparisonConvertlet(
                SargBoundType.UPPER, SargStrictness.CLOSED));

        registerConvertlet(
            SqlStdOperatorTable.greaterThanOperator,
            new ComparisonConvertlet(
                SargBoundType.LOWER, SargStrictness.OPEN));

        registerConvertlet(
            SqlStdOperatorTable.greaterThanOrEqualOperator,
            new ComparisonConvertlet(
                SargBoundType.LOWER, SargStrictness.CLOSED));

        registerConvertlet(
            SqlStdOperatorTable.andOperator,
            new BooleanConvertlet(
                SargSetOperator.INTERSECTION));

        registerConvertlet(
            SqlStdOperatorTable.orOperator,
            new BooleanConvertlet(
                SargSetOperator.UNION));

        registerConvertlet(
            SqlStdOperatorTable.notOperator,
            new BooleanConvertlet(
                SargSetOperator.COMPLEMENT));

        // TODO: isNull, isTrue, isFalse, isUnknown, likeOperator, inOperator,
        // inOperator and (via complement) notEquals, isNotNull,
        // notLikeOperator
        
        // TODO:  non-literal constants (e.g. CURRENT_USER)
    }

    private void registerConvertlet(
        SqlOperator op, CallConvertlet convertlet)
    {
        convertletMap.put(op, convertlet);
    }

    /**
     * Decompose a rex predicate into list of RexNodes that are AND'ed together
     *
     * @param rexPredicate predicate to be analyzed
     */
    public void decompCF(RexNode rexPredicate)
    {
        if (rexPredicate.isA(RexKind.And)) {
            final RexNode[] operands = ((RexCall) rexPredicate).getOperands();
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                decompCF(operand);
            }
        } else {
            rexCFList.add(rexPredicate);            
        }
    }

    /**
     * Reconstruct a rex predicate from a list of SargExprs which will be 
     * AND'ed together.
     */
    public void recompCF()
    {
        SargBinding currBinding, nextBinding;
        RexInputRef currRef, nextRef;
        SargExpr    currSargExpr, nextSargExpr;
        RexNode     currAndNode;
        boolean     recomp;
        ListIterator iter;
        
        for (int i = 0; i < sargBindingList.size(); i++) {
            currBinding  = sargBindingList.get(i);
            currRef      = currBinding.getInputRef();
            currSargExpr = currBinding.getExpr();
            currAndNode = sarg2RexMap.get(currSargExpr);

            // don't need this anymore
            // will be have new mapping put back if currSargExpr remain
            // unchanged.
            sarg2RexMap.remove(currSargExpr);

            recomp = false;

            // search the rest of the list to find SargExpr on the same col.
            iter = sargBindingList.listIterator(i + 1);

            while (iter.hasNext()) {
                nextBinding  = (SargBinding) iter.next();
                nextRef      = nextBinding.getInputRef();
                nextSargExpr = nextBinding.getExpr();

                if (nextRef.getIndex() == currRef.getIndex()) {
                    // build new SargExpr
                    SargSetExpr expr =
                        factory.newSetExpr(boundInputRef.getType(), 
                            SargSetOperator.INTERSECTION);
                    expr.addChild(currSargExpr);
                    expr.addChild(nextSargExpr);

                    // build new RexNode
                    currAndNode =
                        factory.getRexBuilder().makeCall(
                            SqlStdOperatorTable.andOperator,
                            currAndNode,
                            sarg2RexMap.get(nextSargExpr));

                    currSargExpr = expr;

                    sarg2RexMap.remove(nextSargExpr);
                    iter.remove();

                    recomp = true;
                }
            }

            if (recomp) {
                SargBinding newBinding =  new SargBinding(currSargExpr, currRef);
                sargBindingList.remove(i);
                sargBindingList.add(i, newBinding);
            }

            sarg2RexMap.put(currSargExpr, currAndNode);
        }   
    }

    /**
     * Analyzes a rex predicate.
     *
     * @param rexPredicate predicate to be analyzed
     *
     * @return a list of SargBindings contained in the input rex predicate 
     */
    public List<SargBinding> analyzeAll(RexNode rexPredicate)
    {
        rexCFList = new ArrayList<RexNode>();
        sargBindingList = new ArrayList<SargBinding>();
        sarg2RexMap = new HashMap<SargExpr, RexNode>();
        rexPostFilterList = new ArrayList<RexNode>();

        SargBinding sargBinding;

        decompCF(rexPredicate);

        for (RexNode rexPred : rexCFList) {
        	sargBinding = analyze(rexPred);
            if (sargBinding != null) {
                sargBindingList.add(sargBinding);
                sarg2RexMap.put(sargBinding.getExpr(), rexPred);
            } else {
                rexPostFilterList.add(rexPred);
            }
        }
        
        recompCF();

        return sargBindingList;
    }


    /**
     * Reconstruct a rex predicate from the non-sargable filter predicates
     * which are AND'ed together.
     * 
     * @return the rex predicate reconstructed from the non-sargable predicates.
     */
    public RexNode getPostFilterRexNode()
    {
        if (rexPostFilterList.isEmpty()) {
            return null;
        }

        RexNode newAndNode = rexPostFilterList.get(0);
 
        for (int i = 1; i < rexPostFilterList.size(); i++) {
            newAndNode =
                factory.getRexBuilder().makeCall(
                    SqlStdOperatorTable.andOperator,
                    newAndNode,
                    rexPostFilterList.get(i));
        }

        return newAndNode;
    }

    /**
     * Reconstruct a rex predicate from a list of SargBindings which are 
     * AND'ed together.
     * 
     * @param residualSargList list of SargBindings to be converted.
     * 
     * @return the rex predicate reconstructed from the list of SargBindings.
     */
    public RexNode getResidualSargRexNode(List<SargBinding> residualSargList)
    {
        if (residualSargList.isEmpty()) {
            return null;
        }

        RexNode newAndNode = sarg2RexMap.get(residualSargList.get(0).getExpr());

        for (int i = 1; i < residualSargList.size(); i++) {
            RexNode nextNode= sarg2RexMap.get(residualSargList.get(i).getExpr());
            newAndNode =
                factory.getRexBuilder().makeCall(
                    SqlStdOperatorTable.andOperator,
                    newAndNode, nextNode);
        }
        return newAndNode;
    }
    
    /**
     * Analyzes a rex predicate.
     *
     * @param rexPredicate predicate to be analyzed
     *
     * @return corresponding bound sarg expression, or null if analysis failed
     */
    public SargBinding analyze(RexNode rexPredicate)
    {
        RexVisitor visitor = new NodeVisitor();
        
        // Initialize analysis state.
        exprStack = new ArrayList<SargExpr>();
        failed = false;
        boundInputRef = null;
        clearLeaf();

        // Walk the predicate.
        rexPredicate.accept(visitor);

        if (boundInputRef == null) {
            // No variable references at all, so not sargable.
            failed = true;
        }

        if (exprStack.isEmpty()) {
            failed = true;
        }

        if (failed) {
            return null;
        }

        // well-formedness assumption
        assert(exprStack.size() == 1);
        
        SargExpr expr = exprStack.get(0);
        return new SargBinding(expr, boundInputRef);
    }

    private void clearLeaf()
    {
        coordinate = null;
        variableSeen = false;
        reverse = false;
    }

    private abstract class CallConvertlet
    {
        public abstract void convert(RexCall call);
    }

    private class ComparisonConvertlet extends CallConvertlet
    {
        private final SargBoundType boundType;
        
        private final SargStrictness strictness;
        
        ComparisonConvertlet(
            SargBoundType boundType,
            SargStrictness strictness)
        {
            this.boundType = boundType;
            this.strictness = strictness;
        }
        
        // implement CallConvertlet
        public void convert(RexCall call)
        {
            if (!variableSeen || (coordinate == null)) {
                failed = true;
            }

            if (failed) {
                return;
            }
            
            SargIntervalExpr expr =
                factory.newIntervalExpr(boundInputRef.getType());

            if (boundType == null) {
                expr.setPoint(coordinate);
            } else {
                SargBoundType actualBound = boundType;
                if (reverse) {
                    if (actualBound == SargBoundType.LOWER) {
                        actualBound = SargBoundType.UPPER;
                    } else {
                        actualBound = SargBoundType.LOWER;
                    }
                }
                if (actualBound == SargBoundType.LOWER) {
                    expr.setLower(coordinate, strictness);
                } else {
                    expr.setUpper(coordinate, strictness);
                }
            }
            exprStack.add(expr);
            
            clearLeaf();
        }
    }

    private class BooleanConvertlet extends CallConvertlet
    {
        private final SargSetOperator setOp;

        BooleanConvertlet(SargSetOperator setOp)
        {
            this.setOp = setOp;
        }

        // implement CallConvertlet
        public void convert(RexCall call)
        {
            if (variableSeen || (coordinate != null)) {
                failed = true;
            }

            if (failed) {
                return;
            }

            int nOperands = call.getOperands().length;
            assert(exprStack.size() >= nOperands);

            SargSetExpr expr =
                factory.newSetExpr(boundInputRef.getType(), setOp);

            // Pop the correct number of operands off the stack
            // and transfer them to the new set expression.
            ListIterator<SargExpr> iter = exprStack.listIterator(
                exprStack.size() - nOperands);
            while (iter.hasNext()) {
                expr.addChild(iter.next());
                iter.remove();
            }

            exprStack.add(expr);
        }
    }

    private class NodeVisitor extends RexVisitorImpl
    {
        NodeVisitor()
        {
            // go deep
            super(true);
        }

        public void visitInputRef(RexInputRef inputRef)
        {
            variableSeen = true;
            if (boundInputRef == null) {
                boundInputRef = inputRef;
                return;
            }
            if (inputRef.getIndex() != boundInputRef.getIndex()) {
                // sargs can only be over a single variable
                failed = true;
                return;
            }
        }

        public void visitLiteral(RexLiteral literal)
        {
            visitCoordinate(literal);
        }

        public void visitOver(RexOver over)
        {
            failed = true;
        }

        public void visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            failed = true;
        }

        public void visitCall(RexCall call)
        {
            CallConvertlet convertlet = convertletMap.get(call.getOperator());
            if (convertlet == null) {
                failed = true;
                return;
            }
            
            // visit operands first
            super.visitCall(call);

            convertlet.convert(call);
        }

        public void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            visitCoordinate(dynamicParam);
        }

        private void visitCoordinate(RexNode node)
        {
            if (!variableSeen) {
                // We may be looking at an expression like (1 < x).
                reverse = true;
            }
            if (coordinate != null) {
                // e.g. constants on both sides of comparison
                failed = true;
                return;
            }
            coordinate = node;
        }

        public void visitRangeRef(RexRangeRef rangeRef)
        {
            failed = true;
        }

        public void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            failed = true;
        }
    }
}

// End SargRexAnalyzer.java
