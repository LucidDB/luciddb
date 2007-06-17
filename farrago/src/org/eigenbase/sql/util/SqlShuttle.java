/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package org.eigenbase.sql.util;

import java.util.*;

import org.eigenbase.sql.*;


/**
 * Basic implementation of {@link SqlVisitor} which returns each leaf node
 * unchanged.
 *
 * <p>This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface and have {@link SqlNode} as the return type. The
 * derived class can override whichever methods it chooses.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlShuttle
    extends SqlBasicVisitor<SqlNode>
{
    //~ Methods ----------------------------------------------------------------

    public SqlNode visit(SqlLiteral literal)
    {
        return literal;
    }

    public SqlNode visit(SqlIdentifier id)
    {
        return id;
    }

    public SqlNode visit(SqlDataTypeSpec type)
    {
        return type;
    }

    public SqlNode visit(SqlDynamicParam param)
    {
        return param;
    }

    public SqlNode visit(SqlIntervalQualifier intervalQualifier)
    {
        return intervalQualifier;
    }

    public SqlNode visit(final SqlCall call)
    {
        // Handler creates a new copy of 'call' only if one or more operands
        // change.
        ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(call, false);
        call.getOperator().acceptCall(this, call, false, argHandler);
        return argHandler.result();
    }

    public SqlNode visit(SqlNodeList nodeList)
    {
        boolean update = false;
        List<SqlNode> exprs = nodeList.getList();
        int exprCount = exprs.size();
        List<SqlNode> newList = new ArrayList<SqlNode>(exprCount);
        for (int i = 0; i < exprCount; i++) {
            SqlNode operand = exprs.get(i);
            SqlNode clonedOperand;
            if (operand == null) {
                clonedOperand = null;
            } else {
                clonedOperand = operand.accept(this);
                if (clonedOperand != operand) {
                    update = true;
                }
            }
            newList.add(clonedOperand);
        }
        if (update) {
            return new SqlNodeList(
                newList,
                nodeList.getParserPosition());
        } else {
            return nodeList;
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of {@link ArgHandler} which deep-copies {@link SqlCall}s
     * and their operands.
     */
    protected class CallCopyingArgHandler
        implements ArgHandler<SqlNode>
    {
        boolean update;
        SqlNode [] clonedOperands;
        private final SqlCall call;
        private final boolean alwaysCopy;

        public CallCopyingArgHandler(SqlCall call, boolean alwaysCopy)
        {
            this.call = call;
            this.update = false;
            this.clonedOperands = call.operands.clone();
            this.alwaysCopy = alwaysCopy;
        }

        public SqlNode result()
        {
            if (update || alwaysCopy) {
                return call.getOperator().createCall(
                    call.getFunctionQuantifier(),
                    call.getParserPosition(),
                    clonedOperands);
            } else {
                return call;
            }
        }

        public SqlNode visitChild(
            SqlVisitor<SqlNode> visitor,
            SqlNode expr,
            int i,
            SqlNode operand)
        {
            if (operand == null) {
                return null;
            }
            SqlNode newOperand = operand.accept(SqlShuttle.this);
            if (newOperand != operand) {
                update = true;
            }
            clonedOperands[i] = newOperand;
            return newOperand;
        }
    }
}

// End SqlShuttle.java
