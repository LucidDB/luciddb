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

    private RexNode m_root;
    private RexBuilder m_rexBuilder;
    private RelDataType m_boolType;
    private int m_nbrOfIsParents;
    final SqlStdOperatorTable m_opTab = SqlOperatorTable.std();
    private final HashSet m_transformableOperators = new HashSet();

    //~ Constructors ----------------------------------------------------------

    public RexTransformer(
        RexNode root,
        RexBuilder rexBuilder)
    {
        m_root = root;
        m_rexBuilder = rexBuilder;
        m_boolType =
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.Boolean);
        m_nbrOfIsParents = 0;

        m_transformableOperators.add(m_opTab.andOperator);

        /** NOTE the OR operator is NOT missing.
         * see {@link org.eigenbase.test.RexTransformerTest */
        m_transformableOperators.add(m_opTab.equalsOperator);
        m_transformableOperators.add(m_opTab.notEqualsOperator);
        m_transformableOperators.add(m_opTab.greaterThanOperator);
        m_transformableOperators.add(m_opTab.greaterThanOrEqualOperator);
        m_transformableOperators.add(m_opTab.lessThanOperator);
        m_transformableOperators.add(m_opTab.lessThanOrEqualOperator);
    }

    //~ Methods ---------------------------------------------------------------

    private boolean isBoolean(RexNode node)
    {
        RelDataType type = node.getType();
        return m_boolType.isSameType(type);
    }

    private boolean isNullable(RexNode node)
    {
        return node.getType().isNullable();
    }

    private boolean isTransformable(RexNode node)
    {
        if (0 == m_nbrOfIsParents) {
            return false;
        }

        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            return !m_transformableOperators.contains(call.op)
                && isNullable(node);
        }
        return isNullable(node);
    }

    public RexNode tranformNullSemantics()
    {
        m_root = tranformNullSemantics(m_root);
        return m_root;
    }

    private RexNode tranformNullSemantics(RexNode node)
    {
        assert (m_nbrOfIsParents >= 0) : "Cannot be negative";
        if (!isBoolean(node)) {
            return node;
        }

        Boolean directlyUnderIs = null;
        if (node.isA(RexKind.IsTrue)) {
            directlyUnderIs = Boolean.TRUE;
            m_nbrOfIsParents++;
        } else if (node.isA(RexKind.IsFalse)) {
            directlyUnderIs = Boolean.FALSE;
            m_nbrOfIsParents++;
        }

        //special case when we have a Literal, Parameter or Identifer
        //directly as an operand to IS TRUE or IS FALSE
        if (null != directlyUnderIs) {
            RexCall call = (RexCall) node;
            assert m_nbrOfIsParents > 0 : "Stack should not be empty";
            assert 1 == call.operands.length;
            RexNode operand = call.operands[0];
            if ((operand instanceof RexLiteral
                    || operand instanceof RexInputRef
                    || operand instanceof RexDynamicParam)) {
                if (isNullable(node)) {
                    RexNode notNullNode =
                        m_rexBuilder.makeCall(m_opTab.isNotNullOperator,
                            operand);
                    RexNode boolNode =
                        m_rexBuilder.makeLiteral(
                            directlyUnderIs.booleanValue());
                    RexNode eqNode =
                        m_rexBuilder.makeCall(m_opTab.equalsOperator, operand,
                            boolNode);
                    RexNode andBoolNode =
                        m_rexBuilder.makeCall(m_opTab.andOperator,
                            notNullNode, eqNode);

                    return andBoolNode;
                } else {
                    RexNode boolNode =
                        m_rexBuilder.makeLiteral(
                            directlyUnderIs.booleanValue());
                    RexNode andBoolNode =
                        m_rexBuilder.makeCall(m_opTab.equalsOperator, node,
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
                m_nbrOfIsParents--;
                directlyUnderIs = null;
                return call.operands[0];
            }

            if (m_transformableOperators.contains(call.op)) {
                assert (2 == call.operands.length);
                RexNode isNotNullOne = null;
                RexNode isNotNullTwo = null;

                if (isTransformable(call.operands[0])) {
                    isNotNullOne =
                        m_rexBuilder.makeCall(m_opTab.isNotNullOperator,
                            call.operands[0]);
                }

                if (isTransformable(call.operands[1])) {
                    isNotNullTwo =
                        m_rexBuilder.makeCall(m_opTab.isNotNullOperator,
                            call.operands[1]);
                }

                RexNode intoFinalAnd = null;
                if ((null != isNotNullOne) && (null != isNotNullTwo)) {
                    intoFinalAnd =
                        m_rexBuilder.makeCall(m_opTab.andOperator,
                            isNotNullOne, isNotNullTwo);
                } else if (null != isNotNullOne) {
                    intoFinalAnd = isNotNullOne;
                } else if (null != isNotNullTwo) {
                    intoFinalAnd = isNotNullTwo;
                }

                if (null != intoFinalAnd) {
                    RexNode andNullAndCheckNode =
                        m_rexBuilder.makeCall(m_opTab.andOperator,
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
