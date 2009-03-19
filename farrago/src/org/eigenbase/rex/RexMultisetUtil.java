/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * Utility class for various methods related to multisets.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Apr 4, 2005
 */
public class RexMultisetUtil
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * A set defining all implementable multiset calls
     */
    private static final Set multisetOperators = createMultisetOperatorSet();
    public static final SqlStdOperatorTable opTab =
        SqlStdOperatorTable.instance();

    //~ Methods ----------------------------------------------------------------

    private static final Set createMultisetOperatorSet()
    {
        SqlOperator [] operators =
        {
            SqlStdOperatorTable.cardinalityFunc,
            SqlStdOperatorTable.castFunc,
            SqlStdOperatorTable.elementFunc,
            SqlStdOperatorTable.elementSlicefunc,
            SqlStdOperatorTable.multisetExceptAllOperator,
            SqlStdOperatorTable.multisetExceptOperator,
            SqlStdOperatorTable.multisetIntersectAllOperator,
            SqlStdOperatorTable.multisetIntersectOperator,
            SqlStdOperatorTable.multisetUnionAllOperator,
            SqlStdOperatorTable.multisetUnionOperator,
            SqlStdOperatorTable.isASetOperator,
            SqlStdOperatorTable.memberOfOperator,
            SqlStdOperatorTable.submultisetOfOperator
        };
        return new HashSet(Arrays.asList(operators));
    }

    /**
     * Returns true if any expression in a program contains a mixing between
     * multiset and non-multiset calls.
     */
    public static boolean containsMixing(RexProgram program)
    {
        RexCallMultisetOperatorCounter counter =
            new RexCallMultisetOperatorCounter();
        for (RexNode expr : program.getExprList()) {
            counter.reset();
            expr.accept(counter);

            if ((counter.totalCount != counter.multisetCount)
                && (0 != counter.multisetCount))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if a node contains a mixing between multiset and
     * non-multiset calls.
     */
    public static boolean containsMixing(RexNode node)
    {
        RexCallMultisetOperatorCounter counter =
            new RexCallMultisetOperatorCounter();
        node.accept(counter);
        return (counter.totalCount != counter.multisetCount)
            && (0 != counter.multisetCount);
    }

    /**
     * Returns true if node contains a multiset operator, otherwise false. Use
     * it with deep=false when checking if a RexCall is a multiset call.
     *
     * @param node Expression
     * @param deep If true, returns whether expression contains a multiset. If
     * false, returns whether expression <em>is</em> a multiset.
     */
    public static boolean containsMultiset(final RexNode node, boolean deep)
    {
        return null != findFirstMultiset(node, deep);
    }

    /**
     * Returns whether a program contains a multiset.
     */
    public static boolean containsMultiset(RexProgram program)
    {
        final boolean deep = true;
        for (RexNode expr : program.getExprList()) {
            if (containsMultiset(expr, deep)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if call is call to <code>CAST</code> and the to/from cast
     * types are of multiset types
     */
    public static boolean isMultisetCast(RexCall call)
    {
        if (!call.getOperator().equals(SqlStdOperatorTable.castFunc)) {
            return false;
        }
        return call.getType().getSqlTypeName() == SqlTypeName.MULTISET;
    }

    /**
     * Returns a reference to the first found multiset call or null if none was
     * found
     */
    public static RexCall findFirstMultiset(final RexNode node, boolean deep)
    {
        if (node instanceof RexFieldAccess) {
            return findFirstMultiset(
                ((RexFieldAccess) node).getReferenceExpr(),
                deep);
        }

        if (!(node instanceof RexCall)) {
            return null;
        }
        final RexCall call = (RexCall) node;
        assert call != null;
        RexCall firstOne = null;
        Iterator it = multisetOperators.iterator();
        while (it.hasNext()) {
            SqlOperator op = (SqlOperator) it.next();
            firstOne = RexUtil.findOperatorCall(op, call);
            if (null != firstOne) {
                if (firstOne.getOperator().equals(
                        SqlStdOperatorTable.castFunc)
                    && !isMultisetCast(firstOne))
                {
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

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A RexShuttle that traverse all RexNode and counts total number of
     * RexCalls traversed and number of multiset calls traversed.
     *
     * <p>totalCount >= multisetCount always holds true.
     */
    private static class RexCallMultisetOperatorCounter
        extends RexVisitorImpl<Void>
    {
        int totalCount = 0;
        int multisetCount = 0;

        RexCallMultisetOperatorCounter()
        {
            super(true);
        }

        void reset()
        {
            totalCount = 0;
            multisetCount = 0;
        }

        public Void visitCall(RexCall call)
        {
            ++totalCount;
            if (multisetOperators.contains(call.getOperator())) {
                if (!call.getOperator().equals(SqlStdOperatorTable.castFunc)
                    || isMultisetCast(call))
                {
                    ++multisetCount;
                }
            }
            return super.visitCall(call);
        }
    }
}

// End RexMultisetUtil.java
