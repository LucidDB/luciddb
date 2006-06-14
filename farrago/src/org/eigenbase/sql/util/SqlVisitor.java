/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
 * <p>The type parameter <code>R</code> is the return type of each
 * <code>visit()</code> method. If the methods do not need to return a value,
 * use {@link Void}.
 *
 * @see SqlBasicVisitor
 * @see SqlNode#accept(SqlVisitor)
 * @see SqlOperator#acceptCall(SqlVisitor
 *
 * @author jhyde
 * @version $Id$
 */
public interface SqlVisitor <R>
{
    /**
     * Visits a literal.
     *
     * @see SqlLiteral#accept(SqlVisitor)
     * @param literal Literal
     */
    R visit(SqlLiteral literal);

    /**
     * Visits a call to a {@link SqlOperator}.
     *
     * @see SqlCall#accept(SqlVisitor)
     * @param call
     */
    R visit(SqlCall call);

    /**
     * Visits a list of {@link SqlNode} objects.
     *
     * @see SqlNodeList#accept(SqlVisitor)
     * @param nodeList list of nodes
     */
    R visit(SqlNodeList nodeList);

    /**
     * Visits an identifier.
     *
     * @see SqlIdentifier#accept(SqlVisitor)
     * @param id identifier
     */
    R visit(SqlIdentifier id);

    /**
     * Visits a datatype specification.
     *
     * @see SqlDataTypeSpec#accept(SqlVisitor)
     * @param type datatype specification
     */
    R visit(SqlDataTypeSpec type);

    /**
     * Visits a dynamic parameter.
     *
     * @see SqlDynamicParam#accept(SqlVisitor)
     * @param param Dynamic parameter
     */
    R visit(SqlDynamicParam param);

    /**
     * Visits an interval qualifier
     *
     * @see SqlIntervalQualifier#accept(SqlVisitor)
     * @param intervalQualifier
     */
    R visit(SqlIntervalQualifier intervalQualifier);

}

// End SqlVisitor.java
