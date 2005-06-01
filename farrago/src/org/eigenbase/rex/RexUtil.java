/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;


/**
 * Utility methods concerning row-expressions.
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 **/
public class RexUtil
{
    //~ Static fields/initializers --------------------------------------------

    public static final RexNode [] emptyExpressionArray = new RexNode[0];

    //~ Methods ---------------------------------------------------------------

    public static double getSelectivity(RexNode exp)
    {
        return 0.5;
    }

    /**
     * Returns a copy of a row-expression.
     */
    public static RexNode clone(RexNode exp)
    {
        return (RexNode) exp.clone();
    }

    /**
     * Returns a copy of an array of row-expressions.
     */
    public static RexNode [] clone(RexNode [] exps)
    {
        if (null == exps) {
            return null;
        }
        RexNode [] exps2 = new RexNode[exps.length];
        for (int i = 0; i < exps.length; i++) {
            exps2[i] = clone(exps[i]);
        }
        return exps2;
    }

    /**
     * Generates a cast from one row type to another
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     *
     * @param lhsRowType target row type
     *
     * @param rhsRowType source row type; fields must be 1-to-1 with
     * lhsRowType, in same order
     *
     * @return cast expressions
     */
    public static RexNode [] generateCastExpressions(
        RexBuilder rexBuilder,
        RelDataType lhsRowType,
        RelDataType rhsRowType)
    {
        int n = rhsRowType.getFieldList().size();
        assert (n == lhsRowType.getFieldList().size());
        RexNode [] rhsExps = new RexNode[n];
        for (int i = 0; i < n; ++i) {
            rhsExps[i] =
                rexBuilder.makeInputRef(
                    rhsRowType.getFields()[i].getType(),
                    i);
        }
        return generateCastExpressions(rexBuilder, lhsRowType, rhsExps);
    }

    /**
     * Generates a cast for a row type.
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     *
     * @param lhsRowType target row type
     *
     * @param rhsExps expressions to be cast
     *
     * @return cast expressions
     */
    public static RexNode [] generateCastExpressions(
        RexBuilder rexBuilder,
        RelDataType lhsRowType,
        RexNode [] rhsExps)
    {
        RelDataTypeField [] lhsFields = lhsRowType.getFields();
        final int fieldCount = lhsFields.length;
        RexNode [] castExps = new RexNode[fieldCount];
        assert fieldCount == rhsExps.length;
        for (int i = 0; i < fieldCount; ++i) {
            RelDataTypeField lhsField = lhsFields[i];
            RelDataType lhsType = lhsField.getType();
            RelDataType rhsType = rhsExps[i].getType();
            if (lhsType.equals(rhsType)) {
                castExps[i] = rhsExps[i];
            } else {
                castExps[i] = rexBuilder.makeCast(lhsType, rhsExps[i]);
            }
        }
        return castExps;
    }


    /**
     * Returns whether a node represents the NULL value.
     *
     * <p>Examples:<ul>
     * <li>For {@link org.eigenbase.rex.RexLiteral} Unknown, returns false.
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if
     *     <code>allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
     *     returns false.
     * </ul>
     */
    public static boolean isNullLiteral(
        RexNode node,
        boolean allowCast)
    {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            if (literal.getTypeName() == SqlTypeName.Null) {
                assert (null == literal.getValue());
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as NULL.
                return false;
            }
        }
        if (allowCast) {
            if (node.isA(RexKind.Cast)) {
                RexCall call = (RexCall) node;
                if (isNullLiteral(call.operands[0], false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node represents the NULL value or a series of nested
     * CAST(NULL as <TYPE>) calls
     * <br>
     * For Example:<br>
     * isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1))) returns true
     */
    public static boolean isNull(RexNode node)
    {
        /* Checks to see if the RexNode is null */
        return RexLiteral.isNullLiteral(node)
            || ((node.getKind() == RexKind.Cast)
            && isNull(((RexCall) node).operands[0]));
    }

    /**
     * Returns wheter a given node contains a RexCall with a specified operator
     * @param operator to look for
     * @param node a RexNode tree
     */
    public static RexCall findOperatorCall(final SqlOperator operator,
                                           RexNode node)
    {
        try {
            RexShuttle shuttle = new RexShuttle() {
                public RexNode visit(RexCall call)
                {
                    if (call.getOperator().equals(operator)) {
                        throw new Util.FoundOne(call);
                    }
                    return super.visit(call);
                }
            };
            shuttle.visit(node);
            return null;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return (RexCall) e.getNode();
        }
    }

    /**
     * Creates an array of {@link RexInputRef}, one for each field of a given
     * rowtype.
     */
    public static RexInputRef[] toInputRefs(RelDataType rowType)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        final RexInputRef[] rexNodes = new RexInputRef[fields.length];
        for (int i = 0; i < rexNodes.length; i++) {
            rexNodes[i] = new RexInputRef(i, fields[i].getType());
        }
        return rexNodes;
    }

    /**
     * Converts an array of {@link RexNode} to an array of {@link Integer}.
     * Every node must be a {@link RexInputRef}.
     */
    public static Integer[] toOrdinalArray(RexNode[] rexNodes)
    {
        Integer[] orderKeys = new Integer[rexNodes.length];
        for (int i = 0; i < orderKeys.length; i++) {
            RexInputRef inputRef = (RexInputRef) rexNodes[i];
            orderKeys[i] = new Integer(inputRef.getIndex());
        }
        return orderKeys;
    }
}


// End RexUtil.java
