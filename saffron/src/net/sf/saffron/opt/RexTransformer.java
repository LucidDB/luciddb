/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.saffron.opt;

import net.sf.saffron.rex.*;
import net.sf.saffron.sql.SqlValidator;
import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.core.SaffronType;

import java.util.HashSet;


/**
 * Takes a rextree and transforms it into another in one sense equivalent rextree.
 * Nodes in tree will be modified and hence tree will not remain unchanged
 * @pre The RexTree needs to have gone through the {@link net.sf.saffron.sql.SqlValidator} prior to using this class
 *
 * @author wael
 * @since Mar 8, 2004
 * @version $Id$
 **/
public class RexTransformer
{
    private RexNode m_root;
    private RexBuilder m_rexBuilder;
    private SaffronType m_boolType;
    final SqlOperatorTable m_opTab = SqlOperatorTable.instance();
    private final HashSet m_transformableOperators = new HashSet();


    public RexTransformer(RexNode root, RexBuilder rexBuilder)
    {
        m_root = root;
        m_rexBuilder = rexBuilder;
        m_boolType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.Boolean);

        m_transformableOperators.add(m_opTab.andOperator);
        m_transformableOperators.add(m_opTab.equalsOperator);
        m_transformableOperators.add(m_opTab.notEqualsOperator);
        m_transformableOperators.add(m_opTab.greaterThanOperator);
        m_transformableOperators.add(m_opTab.greaterThanOrEqualOperator);
        m_transformableOperators.add(m_opTab.lessThanOperator);
        m_transformableOperators.add(m_opTab.lessThanOrEqualOperator);
    }

    private boolean isBoolean(RexNode node)
    {
        SaffronType type=node.getType();
        return m_boolType.isSameTypeFamily(type);
    }

    private boolean isNullable(RexNode node)
    {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            return literal.getValue()==null;
        }

        return node.getType().isNullable();
    }

    private boolean isTransformable(RexNode node)
    {
        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            return !m_transformableOperators.contains(call.op) && isNullable(node);
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
        if (!isBoolean(node)) {
            return node;
        }

        if (node instanceof RexCall)
        {
            RexCall call = (RexCall) node;
            //transform children (if any) before transforming node itself
            for (int i = 0; i < call.operands.length; i++) {
                RexNode operand = call.operands[i];
                call.operands[i] = tranformNullSemantics(operand);
            }

            if (m_transformableOperators.contains(call.op))
            {
                assert(2==call.operands.length);
                if (isTransformable(call.operands[0]) && isTransformable(call.operands[1]))
                {
                    RexNode isNotNullOneNode = m_rexBuilder.makeCall(SqlOperatorTable.instance().isNotNullOperator, call.operands[0]);
                    RexNode isNotNullTwoNode = m_rexBuilder.makeCall(SqlOperatorTable.instance().isNotNullOperator, call.operands[1]);
                    RexNode andNullCheckNode = m_rexBuilder.makeCall(SqlOperatorTable.instance().andOperator, isNotNullOneNode, isNotNullTwoNode);
                    RexNode andNullAndCheckNode = m_rexBuilder.makeCall(SqlOperatorTable.instance().andOperator, andNullCheckNode, call);

                    return andNullAndCheckNode;
                }
                else if (isTransformable(call.operands[0]) || isTransformable(call.operands[1]))
                {
                    int index;
                    if (isTransformable(call.operands[0])) {
                        index = 0;
                    } else {
                        index = 1;
                    }

                    RexNode isNotNullNode = m_rexBuilder.makeCall(SqlOperatorTable.instance().isNotNullOperator, call.operands[index]);
                    RexNode andNullCallNode = m_rexBuilder.makeCall(SqlOperatorTable.instance().andOperator, isNotNullNode, call);

                    return andNullCallNode;
                }
                //else there is no need to do anything
            }
        }

        return node;

    }
}

// End RexTransformer.java