/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
 * Visitor class, follows the
 * {@link org.eigenbase.util.Glossary#VisitorPattern visitor pattern}.
 *
 * @see SqlBasicVisitor
 * @see SqlNode#accept(SqlVisitor)
 * @see SqlOperator#acceptCall(SqlVisitor, org.eigenbase.sql.SqlCall)
 *
 * @author jhyde
 * @version $Id$
 */
public interface SqlVisitor {
    void visit(SqlLiteral literal);
    void visit(SqlCall call);
    void visit(SqlNodeList nodeList);
    void visit(SqlIdentifier id);
    void visit(SqlDataTypeSpec type);
    void visit(SqlDynamicParam param);
    void visit(SqlIntervalQualifier intervalQualifier);
    /**
     * Recurses to a particular child of a node.
     *
     * <p>This method is principally used by
     * implementations of the {@link SqlNodeList#accept(SqlVisitor)} and
     * {@link SqlOperator#acceptCall(SqlVisitor, org.eigenbase.sql.SqlCall)}
     * methods.
     *
     * @param parent Parent node
     * @param ordinal Ordinal of child in parent
     * @param child Child node
     */
    void visitChild(SqlNode parent, int ordinal, SqlNode child);
}

// End SqlVisitor.java
