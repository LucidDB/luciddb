/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import org.eigenbase.sql.*;


/**
 * Basic implementation of {@link SqlVisitor} which does nothing at each node.
 *
 * <p>This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface. The derived class can override whichever
 * methods it chooses.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlBasicVisitor<R>
    implements SqlVisitor<R>
{
    //~ Methods ----------------------------------------------------------------

    public R visit(SqlLiteral literal)
    {
        return null;
    }

    public R visit(SqlCall call)
    {
        return call.getOperator().acceptCall(this, call);
    }

    public R visit(SqlNodeList nodeList)
    {
        R result = null;
        for (int i = 0; i < nodeList.size(); i++) {
            SqlNode node = nodeList.get(i);
            result = node.accept(this);
        }
        return result;
    }

    public R visit(SqlIdentifier id)
    {
        return null;
    }

    public R visit(SqlDataTypeSpec type)
    {
        return null;
    }

    public R visit(SqlDynamicParam param)
    {
        return null;
    }

    public R visit(SqlIntervalQualifier intervalQualifier)
    {
        return null;
    }

    //~ Inner Interfaces -------------------------------------------------------

    // REVIEW jvs 16-June-2006:  Without javadoc, the interaction between
    // ArgHandler and SqlBasicVisitor isn't obvious (nor why this interface
    // belongs here instead of at top-level).  visitChild already returns
    // R; why is a separate result() call needed?
    public interface ArgHandler<R>
    {
        R result();

        R visitChild(
            SqlVisitor<R> visitor,
            SqlNode expr,
            int i,
            SqlNode operand);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Default implementation of {@link ArgHandler} which merely calls {@link
     * SqlNode#accept} on each operand.
     */
    public static class ArgHandlerImpl<R>
        implements ArgHandler<R>
    {
        // REVIEW jvs 16-June-2006:  This doesn't actually work, because it
        // is type-erased, and if you try to add <R>, you get the error
        // "non-static class R cannot be referenced from a static context."
        public static final ArgHandler instance = new ArgHandlerImpl();

        public R result()
        {
            return null;
        }

        public R visitChild(
            SqlVisitor<R> visitor,
            SqlNode expr,
            int i,
            SqlNode operand)
        {
            if (operand == null) {
                return null;
            }
            return operand.accept(visitor);
        }
    }
}

// End SqlBasicVisitor.java
