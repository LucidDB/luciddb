/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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
package org.eigenbase.sql.util;

import org.eigenbase.sql.*;

/**
 * Basic implementation of {@link SqlVisitor} which does nothing at each
 * node.
 *
 * This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface. The derived class can override whichever
 * methods it chooses.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlBasicVisitor implements SqlVisitor
 {
    /**
     * Used to keep track of the current SqlNode parent of a visiting node.
     * A value of null mean no parent.
     * NOTE: In case of extending SqlBasicVisitor, remember that
     * parent value might not be set depending on if and how
     * visit(SqlCall) and visit(SqlNodeList) is implemented.
     */
    public SqlNode currentParent = null;
    /**
     *  Only valid if currentParrent is a SqlCall or SqlNodeList
     *  Describes the offset within the parent
     */
    public Integer currentOffset = null;

    public void visit(SqlLiteral literal)
    {
    }

    public void visit(SqlCall call)
    {
        call.operator.acceptCall(this, call);
    }

    public void visit(SqlNodeList nodeList)
    {
        for (int i = 0; i < nodeList.size(); i++) {
            currentParent = nodeList;
            currentOffset = new Integer(i);
            SqlNode node = nodeList.get(i);
            node.accept(this);
        }
        currentParent = null;
    }

    public void visit(SqlIdentifier id)
    {
    }

    public void visit(SqlDataTypeSpec type)
    {
    }

    public void visit(SqlDynamicParam param)
    {
    }

    public void visit(SqlIntervalQualifier intervalQualifier)
    {
    }

    public void visitChild(SqlNode parent, int ordinal, SqlNode child)
    {
        currentParent = parent;
        currentOffset = new Integer(ordinal);
        child.accept(this);
        currentParent = null;
    }
}

// End SqlBasicVisitor.java
