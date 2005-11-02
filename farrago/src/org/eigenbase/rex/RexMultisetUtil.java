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

import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.SqlOperator;

import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;

/**
 * Utility class for various multiset related methods
 *
 * @author Wael Chatila
 * @since Apr 4, 2005
 * @version $Id$
 */
public class RexMultisetUtil
{
    /** A set defining all implementable multiset calls */
    private static final Set multisetOperators = new HashSet();
    public static final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

    static {
        multisetOperators.add(SqlStdOperatorTable.cardinalityFunc);
        multisetOperators.add(SqlStdOperatorTable.castFunc);
        multisetOperators.add(SqlStdOperatorTable.elementFunc);
        multisetOperators.add(SqlStdOperatorTable.multisetExceptAllOperator);
        multisetOperators.add(SqlStdOperatorTable.multisetExceptOperator);
        multisetOperators.add(SqlStdOperatorTable.multisetIntersectAllOperator);
        multisetOperators.add(SqlStdOperatorTable.multisetIntersectOperator);
        multisetOperators.add(SqlStdOperatorTable.multisetUnionAllOperator);
        multisetOperators.add(SqlStdOperatorTable.multisetUnionOperator);
        multisetOperators.add(SqlStdOperatorTable.isASetOperator);
        multisetOperators.add(SqlStdOperatorTable.memberOfOperator);
        multisetOperators.add(SqlStdOperatorTable.submultisetOfOperator);
    }

    /**
     * Returns true if a node contains a mixing between multiset and
     * non-multiset calls.
     */
    public static boolean containsMixing(RexNode node)
    {
        RexCallMultisetOperatorCounter countShuttle = new RexCallMultisetOperatorCounter();
        node.accept(countShuttle);
        if (countShuttle.totalCount == countShuttle.multisetCount) {
            return false;
        }

        if (0 == countShuttle.multisetCount) {
            return false;
        }

        return true;
    }

    /**
     * Returns true if node contains a multiset operator, otherwise false.
     * Use it with deep=false when checking if a RexCall is a multiset call.
     *
     * @param node Expression
     * @param deep If true, returns whether expression contains a multiset.
     *   If false, returns whether expression <em>is</em> a multiset.
     */
    public static boolean containsMultiset(final RexNode node, boolean deep)
    {
        return null != findFirstMultiset(node, deep);
    }

    /**
     * Returns whether a list of expressions, or an optional single expression,
     * contain a multiset.
     */
    public static boolean containsMultiset(RexNode[] exprs, RexNode expr)
    {
        final boolean deep = true;
        for (int i = 0; i < exprs.length; i++) {
            if (containsMultiset(exprs[i], deep)) {
                return true;
            }
        }
        if (expr != null &&
            containsMultiset(expr, deep)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true if call is call to <code>CAST</code> and the to/from cast
     * types are of multiset types
     */
    public static boolean isMultisetCast(RexCall call) {
        if (!call.getOperator().equals(opTab.castFunc)) {
            return false;
        }
        return call.getType().getSqlTypeName() == SqlTypeName.Multiset;
    }

    /**
     * Returns a reference to the first found multiset call or null if none was
     * found
     */
    public static RexCall findFirstMultiset(final RexNode node, boolean deep)
    {
        if (node instanceof RexFieldAccess) {
            return findFirstMultiset(
                ((RexFieldAccess) node).getReferenceExpr(), deep);
        }

        if (!(node instanceof RexCall)) {
            return null;
        }
        final RexCall call = (RexCall) node;
        assert(null != call);
        RexCall firstOne = null;
        Iterator it = multisetOperators.iterator();
        while (it.hasNext()) {
            SqlOperator op = (SqlOperator) it.next();
            firstOne = RexUtil.findOperatorCall(op, call);
            if (null != firstOne) {
                if (firstOne.getOperator().equals(opTab.castFunc) &&
                    !isMultisetCast(firstOne)) {
                    firstOne = null;
                    continue;
                }
                break;
            }
        }

        if (!deep && (firstOne != call)) {
            return null;
        }
        return firstOne;
    }

    /**
     * A RexShuttle that traverse all RexNode and counts total number of RexCalls
     * traversed and number of multiset calls traversed.<p>
     * totalCount >= multisetCount always holds true.
     */
    private static class RexCallMultisetOperatorCounter extends RexShuttle {
        int totalCount = 0;
        int multisetCount = 0;

        public RexNode visitCall(RexCall call)
        {
            totalCount++;
            doSuper:
            if (multisetOperators.contains(call.getOperator())) {
                if (call.getOperator().equals(opTab.castFunc)
                    && !isMultisetCast(call))
                {
                    break doSuper;
                }
                multisetCount++;
            }
            return super.visitCall(call);
        }
    }
}

// End RexMultisetUtil.java
