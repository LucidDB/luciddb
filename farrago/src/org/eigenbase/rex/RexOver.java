/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
import org.eigenbase.sql.*;
import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Call to an aggregate function over a window.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 6, 2004
 */
public class RexOver extends RexCall
{
    private final RexWindow window;

    /**
     * Creates a RexOver.
     *
     * <p>For example, "SUM(x) OVER (ROWS 3 PRECEDING)" is represented as:<ul>
     * <li>type = Integer,
     * <li>op = {@link org.eigenbase.sql.fun.SqlStdOperatorTable#sumOperator},
     * <li>operands = { {@link RexFieldAccess}("x") }
     * <li>window = {@link SqlWindow}(ROWS 3 PRECEDING)
     * </ul>
     *
     * @param type Result type
     * @param op Aggregate operator
     * @param operands Operands list
     * @param window Window specification
     *
     * @pre op.isAggregator()
     * @pre window != null
     * @pre window.getRefName() == null
     */
    RexOver(
        RelDataType type,
        SqlAggFunction op,
        RexNode[] operands,
        RexWindow window)
    {
        super(type, op, operands);
        assert op.isAggregator() : "precondition: op.isAggregator()";
        assert op instanceof SqlAggFunction;
        assert window != null : "precondition: window != null";
        this.window = window;
        this.digest = computeDigest(true);
    }

    /**
     * Returns the aggregate operator for this expression.
     */
    public SqlAggFunction getAggOperator()
    {
        return (SqlAggFunction) getOperator();
    }

    public RexWindow getWindow()
    {
        return window;
    }

    protected String computeDigest(boolean withType)
    {
        return super.computeDigest(withType) + " OVER " + window;
    }

    public Object clone()
    {
        return new RexOver(
            getType(), getAggOperator(), operands, window);
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitOver(this);
    }

    public RexNode accept(RexShuttle shuttle)
    {
        return shuttle.visitOver(this);
    }

    /**
     * Returns whether an expression contains an OVER clause.
     */
    public static boolean containsOver(RexNode expr)
    {
        return Finder.instance.containsOver(expr);
    }

    /**
     * Returns whether an expression list contains an OVER clause.
     */
    public static boolean containsOver(RexNode[] exprs, RexNode expr)
    {
        for (int i = 0; i < exprs.length; i++) {
            if (Finder.instance.containsOver(exprs[i])) {
                return true;
            }
        }
        if (expr != null &&
            Finder.instance.containsOver(expr)) {
            return true;
        }
        return false;
    }

    private static class OverFound extends RuntimeException
    {
        public static final OverFound instance = new OverFound();
    };

    /**
     * Visitor which detects a {@link RexOver} inside a {@link RexNode}
     * expression.
     *
     * <p>It is re-entrant (two threads can use an instance at the same time)
     * and it can be re-used for multiple visits.
     */
    private static class Finder extends RexVisitorImpl
    {
        static final RexOver.Finder instance = new RexOver.Finder();

        public Finder()
        {
            super(true);
        }

        public void visitOver(RexOver over)
        {
            throw OverFound.instance;
        }

        /**
         * Returns whether an expression contains an OVER clause.
         */
        boolean containsOver(RexNode expr)
        {
            try {
                expr.accept(this);
                return false;
            } catch (OverFound e) {
                Util.swallow(e, null);
                return true;
            }
        }
    }

    /**
     * Window specification.
     *
     * <p>Treat it as immutable!
     */
    public static class RexWindow {
        public final RexNode[] partitionKeys;
        public final RexNode[] orderKeys;
        private final SqlNode lowerBound;
        private final SqlNode upperBound;
        private final boolean physical;
        private String digest;

        RexWindow(
            RexNode[] partitionKeys,
            RexNode[] orderKeys,
            SqlNode lowerBound,
            SqlNode upperBound,
            boolean physical)
        {
            assert partitionKeys != null;
            assert orderKeys != null;
            this.partitionKeys = partitionKeys;
            this.orderKeys = orderKeys;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.physical = physical;
            this.digest = computeDigest();
            if (!physical) {
                assert orderKeys.length > 0 :
                    "logical window requires sort key";
            }
        }

        public String toString()
        {
            return digest;
        }

        public int hashCode()
        {
            return digest.hashCode();
        }

        public boolean equals(Object that)
        {
            if (that instanceof RexWindow) {
                RexWindow window = (RexWindow) that;
                return digest.equals(window.digest);
            }
            return false;
        }

        private String computeDigest()
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.print("(");
            int clauseCount = 0;
            if (partitionKeys.length > 0) {
                if (clauseCount++ > 0) {
                    pw.print(' ');
                }
                pw.print("PARTITION BY ");
                for (int i = 0; i < partitionKeys.length; i++) {
                    RexNode partitionKey = partitionKeys[i];
                    pw.print(partitionKey.toString());
                }
            }
            if (orderKeys.length > 0) {
                if (clauseCount++ > 0) {
                    pw.print(' ');
                }
                pw.print("ORDER BY ");
                for (int i = 0; i < orderKeys.length; i++) {
                    RexNode orderKey = orderKeys[i];
                    pw.print(orderKey.toString());
                }
            }
            if (lowerBound == null) {
                // No ROWS or RANGE clause
            } else if (upperBound == null) {
                if (clauseCount++ > 0) {
                    pw.println();
                }
                if (physical) {
                    pw.print("ROWS ");
                } else {
                    pw.print("RANGE ");
                }
                pw.print(lowerBound.toString());
            } else {
                if (clauseCount++ > 0) {
                    pw.println();
                }
                if (physical) {
                    pw.print("ROWS BETWEEN ");
                } else {
                    pw.print("RANGE BETWEEN ");
                }
                pw.print(lowerBound.toString());
                pw.print(" AND ");
                pw.print(upperBound.toString());
            }
            pw.print(")");
            return sw.toString();
        }

        public SqlNode getLowerBound() {
            return lowerBound;
        }

        public SqlNode getUpperBound() {
            return upperBound;
        }

        public boolean isRows() {
            return physical;
        }

    }
}

// End RexOver.java
