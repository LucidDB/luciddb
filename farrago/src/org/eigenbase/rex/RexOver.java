/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlWindow;
import org.eigenbase.util.Util;

/**
 * Call to an aggregate function over a window.
 */
public class RexOver extends RexCall {
    private final RexNode[] operands;
    private final SqlWindow window;
    private RelDataType type;

    /**
     * Creates a RexOver.
     *
     * For example, "SUM(x) OVER (ROWS 3 PRECEDING)" is represented as:<ul>
     * <li>type = Integer,
     * <li>op = {@link org.eigenbase.sql.fun.SqlStdOperatorTable#sumOperator},
     * <li>operands = { {@link RexFieldAccess}("x") }
     * <li>window = {@link SqlWindow}(ROWS 3 PRECEDING)
     * </ul>
     *
     * @param type Result type
     * @param op Aggregate operator
     * @param operands Operands list
     * @param window Fully-resolved window specification
     *
     * @pre op.isAggregator()
     * @pre window != null
     * @pre window.getRefName() == null
     */
    RexOver(
        RelDataType type,
        SqlOperator op,
        RexNode[] operands,
        SqlWindow window)
    {
        super(type, op, operands);
        assert op.isAggregator() : "precondition: op.isAggregator()";
        assert window != null : "precondition: window != null";
        // window specification must be fully-resolved
        assert window.getRefName() == null : "window.getRefName() == null";
        this.operands = operands;
        this.window = window;
        this.type = type;
        this.digest = computeDigest(true);
    }

    protected String computeDigest(boolean withType) {
        return super.computeDigest(withType) + " OVER " + window;
    }

    public RelDataType getType() {
        return type;
    }

    public Object clone() {
        return new RexOver(type, op, operands, window);
    }

    public void accept(RexVisitor visitor) {
        throw Util.needToImplement(this); // TODO: add RexVisitor.visitOver
    }
}
