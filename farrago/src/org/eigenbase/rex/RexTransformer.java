/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.rex;

import java.util.HashSet;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;


/**
 * Takes a rextree and transforms it into another in one sense equivalent rextree.
 * Nodes in tree will be modified and hence tree will not remain unchanged
 * @pre The RexTree needs to have gone through the {@link org.eigenbase.sql.SqlValidator} prior to using this class
 *
 * @author wael
 * @since Mar 8, 2004
 * @version $Id$
 **/
public class RexTransformer
{
    //~ Instance fields -------------------------------------------------------

    private RexNode root;
    private final RexBuilder rexBuilder;
    private final RelDataType boolType;
    private int isParentsCount;
    private final SqlStdOperatorTable opTab = SqlOperatorTable.std();
    private final HashSet transformableOperators = new HashSet();

    //~ Constructors ----------------------------------------------------------

    public RexTransformer(
        RexNode root,
        RexBuilder rexBuilder)
    {
        this.root = root;
        this.rexBuilder = rexBuilder;
        boolType =
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.Boolean);
        isParentsCount = 0;

        transformableOperators.add(opTab.andOperator);

        /** NOTE the OR operator is NOT missing.
         * see {@link org.eigenbase.test.RexTransformerTest */
        transformableOperators.add(opTab.equalsOperator);
        transformableOperators.add(opTab.notEqualsOperator);
        transformableOperators.add(opTab.greaterThanOperator);
        transformableOperators.add(opTab.greaterThanOrEqualOperator);
        transformableOperators.add(opTab.lessThanOperator);
        transformableOperators.add(opTab.lessThanOrEqualOperator);
    }

    //~ Methods ---------------------------------------------------------------

    private boolean isBoolean(RexNode node)
    {
        RelDataType type = node.getType();
        return boolType.isSameType(type);
    }

    private boolean isNullable(RexNode node)
    {
        return node.getType().isNullable();
    }

    private boolean isTransformable(RexNode node)
    {
        if (0 == isParentsCount) {
            return false;
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            return !transformableOperators.contains(call.op)
                && isNullable(node);
        }
        return isNullable(node);
    }

    public RexNode tranformNullSemantics()
    {
        root = tranformNullSemantics(root);
        return root;
    }

    private RexNode tranformNullSemantics(RexNode node)
    {
        assert (isParentsCount >= 0) : "Cannot be negative";
        if (!isBoolean(node)) {
            return node;
        }

        Boolean directlyUnderIs = null;
        if (node.isA(RexKind.IsTrue)) {
            directlyUnderIs = Boolean.TRUE;
            isParentsCount++;
        } else if (node.isA(RexKind.IsFalse)) {
            directlyUnderIs = Boolean.FALSE;
            isParentsCount++;
        }

        //special case when we have a Literal, Parameter or Identifer
        //directly as an operand to IS TRUE or IS FALSE
        if (null != directlyUnderIs) {
            RexCall call = (RexCall) node;
            assert isParentsCount > 0 : "Stack should not be empty";
            assert 1 == call.operands.length;
            RexNode operand = call.operands[0];
            if ((operand instanceof RexLiteral
                    || operand instanceof RexInputRef
                    || operand instanceof RexDynamicParam)) {
                if (isNullable(node)) {
                    RexNode notNullNode =
                        rexBuilder.makeCall(opTab.isNotNullOperator,
                            operand);
                    RexNode boolNode =
                        rexBuilder.makeLiteral(
                            directlyUnderIs.booleanValue());
                    RexNode eqNode =
                        rexBuilder.makeCall(opTab.equalsOperator, operand,
                            boolNode);
                    RexNode andBoolNode =
                        rexBuilder.makeCall(opTab.andOperator,
                            notNullNode, eqNode);

                    return andBoolNode;
                } else {
                    RexNode boolNode =
                        rexBuilder.makeLiteral(
                            directlyUnderIs.booleanValue());
                    RexNode andBoolNode =
                        rexBuilder.makeCall(opTab.equalsOperator, node,
                            boolNode);
                    return andBoolNode;
                }
            }

            //else continue as normal
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            //transform children (if any) before transforming node itself
            for (int i = 0; i < call.operands.length; i++) {
                RexNode operand = call.operands[i];
                call.operands[i] = tranformNullSemantics(operand);
            }

            if (null != directlyUnderIs) {
                isParentsCount--;
                directlyUnderIs = null;
                return call.operands[0];
            }

            if (transformableOperators.contains(call.op)) {
                assert (2 == call.operands.length);
                RexNode isNotNullOne = null;
                RexNode isNotNullTwo = null;

                if (isTransformable(call.operands[0])) {
                    isNotNullOne =
                        rexBuilder.makeCall(opTab.isNotNullOperator,
                            call.operands[0]);
                }

                if (isTransformable(call.operands[1])) {
                    isNotNullTwo =
                        rexBuilder.makeCall(opTab.isNotNullOperator,
                            call.operands[1]);
                }

                RexNode intoFinalAnd = null;
                if ((null != isNotNullOne) && (null != isNotNullTwo)) {
                    intoFinalAnd =
                        rexBuilder.makeCall(opTab.andOperator,
                            isNotNullOne, isNotNullTwo);
                } else if (null != isNotNullOne) {
                    intoFinalAnd = isNotNullOne;
                } else if (null != isNotNullTwo) {
                    intoFinalAnd = isNotNullTwo;
                }

                if (null != intoFinalAnd) {
                    RexNode andNullAndCheckNode =
                        rexBuilder.makeCall(opTab.andOperator,
                            intoFinalAnd, call);
                    return andNullAndCheckNode;
                }

                //if come here no need to do anything
            }
        }

        return node;
    }
}


// End RexTransformer.java
