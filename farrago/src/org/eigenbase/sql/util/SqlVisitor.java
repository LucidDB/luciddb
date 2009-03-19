/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
 * Visitor class, follows the {@link org.eigenbase.util.Glossary#VisitorPattern
 * visitor pattern}.
 *
 * <p>The type parameter <code>R</code> is the return type of each <code>
 * visit()</code> method. If the methods do not need to return a value, use
 * {@link Void}.
 *
 * @author jhyde
 * @version $Id$
 * @see SqlBasicVisitor
 * @see SqlNode#accept(SqlVisitor)
 * @see SqlOperator#acceptCall
 */
public interface SqlVisitor<R>
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Visits a literal.
     *
     * @param literal Literal
     *
     * @see SqlLiteral#accept(SqlVisitor)
     */
    R visit(SqlLiteral literal);

    /**
     * Visits a call to a {@link SqlOperator}.
     *
     * @param call Call
     *
     * @see SqlCall#accept(SqlVisitor)
     */
    R visit(SqlCall call);

    /**
     * Visits a list of {@link SqlNode} objects.
     *
     * @param nodeList list of nodes
     *
     * @see SqlNodeList#accept(SqlVisitor)
     */
    R visit(SqlNodeList nodeList);

    /**
     * Visits an identifier.
     *
     * @param id identifier
     *
     * @see SqlIdentifier#accept(SqlVisitor)
     */
    R visit(SqlIdentifier id);

    /**
     * Visits a datatype specification.
     *
     * @param type datatype specification
     *
     * @see SqlDataTypeSpec#accept(SqlVisitor)
     */
    R visit(SqlDataTypeSpec type);

    /**
     * Visits a dynamic parameter.
     *
     * @param param Dynamic parameter
     *
     * @see SqlDynamicParam#accept(SqlVisitor)
     */
    R visit(SqlDynamicParam param);

    /**
     * Visits an interval qualifier
     *
     * @param intervalQualifier Interval qualifier
     *
     * @see SqlIntervalQualifier#accept(SqlVisitor)
     */
    R visit(SqlIntervalQualifier intervalQualifier);
}

// End SqlVisitor.java
