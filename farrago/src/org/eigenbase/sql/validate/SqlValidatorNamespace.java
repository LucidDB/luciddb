/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004 The Eigenbase Project
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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * A namespace describes the relation returned by a section of a SQL query.
 *
 * <p>For example, in the query <code>SELECT emp.deptno, age FROM emp,
 * dept</code>, the FROM clause forms a namespace consisting of two tables EMP
 * and DEPT, and a row type consisting of the combined columns of those tables.
 *
 * <p>Other examples of namespaces include a table in the from list (the
 * namespace contains the constituent columns) and a subquery (the namespace
 * contains the columns in the SELECT clause of the subquery).
 *
 * <p>These various kinds of namespace are implemented by classes {@link
 * IdentifierNamespace} for table names, {@link SelectNamespace} for SELECT
 * queries, {@link SetopNamespace} for UNION, EXCEPT and INTERSECT, and so
 * forth. But if you are looking at a SELECT query and call {@link
 * SqlValidator#getNamespace(org.eigenbase.sql.SqlNode)}, you may not get a
 * SelectNamespace. Why? Because the validator is allowed to wrap namespaces in
 * other objects which implement {@link SqlValidatorNamespace}. Your
 * SelectNamespace will be there somewhere, but might be one or two levels deep.
 * Don't try to cast the namespace or use <code>instanceof</code>; use {@link
 * SqlValidatorNamespace#unwrap(Class)} and {@link
 * SqlValidatorNamespace#isWrapperFor(Class)} instead.</p>
 *
 * @author jhyde
 * @version $Id$
 * @see SqlValidator
 * @see SqlValidatorScope
 * @since Mar 25, 2003
 */
public interface SqlValidatorNamespace
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the validator.
     *
     * @return validator
     */
    SqlValidator getValidator();

    /**
     * Returns the underlying table, or null if there is none.
     */
    SqlValidatorTable getTable();

    /**
     * Returns the row type of this namespace, which comprises a list of names
     * and types of the output columns. If the scope's type has not yet been
     * derived, derives it. Never returns null.
     *
     * @post return != null
     */
    RelDataType getRowType();

    /**
     * Allows RowType for the namespace to be explicitly set.
     */
    void setRowType(RelDataType rowType);

    /**
     * Returns the row type of this namespace, sans any system columns.
     *
     * @return Row type sans system columns
     */
    RelDataType getRowTypeSansSystemColumns();

    /**
     * Validates this namespace.
     *
     * <p>If the scope has already been validated, does nothing.</p>
     *
     * <p>Please call {@link SqlValidatorImpl#validateNamespace} rather than
     * calling this method directly.</p>
     */
    void validate();

    /**
     * Returns the parse tree node at the root of this namespace.
     *
     * @return parse tree node
     */
    SqlNode getNode();

    /**
     * Returns the parse tree node that at is at the root of this namespace and
     * includes all decorations. If there are no decorations, returns the same
     * as {@link #getNode()}.
     */
    SqlNode getEnclosingNode();

    /**
     * Looks up a child namespace of a given name.
     *
     * <p>For example, in the query <code>select e.name from emps as e</code>,
     * <code>e</code> is an {@link IdentifierNamespace} which has a child <code>
     * name</code> which is a {@link FieldNamespace}.
     *
     * @param name Name of namespace
     *
     * @return Namespace
     */
    SqlValidatorNamespace lookupChild(String name);

    /**
     * Returns whether this namespace has a field of a given name.
     *
     * @param name Field name
     *
     * @return Whether field exists
     */
    boolean fieldExists(String name);

    /**
     * Returns a list of expressions which are monotonic in this namespace. For
     * example, if the namespace represents a relation ordered by a column
     * called "TIMESTAMP", then the list would contain a {@link
     * org.eigenbase.sql.SqlIdentifier} called "TIMESTAMP".
     */
    List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs();

    /**
     * Returns whether and how a given column is sorted.
     */
    SqlMonotonicity getMonotonicity(String columnName);

    /**
     * Makes all fields in this namespace nullable (typically because it is on
     * the outer side of an outer join.
     */
    void makeNullable();

    /**
     * Translates a field name to the name in the underlying namespace.
     */
    String translate(String name);

    /**
     * Returns this namespace, or a wrapped namespace, cast to a particular
     * class.
     *
     * @param clazz Desired type
     *
     * @return This namespace cast to desired type
     *
     * @throws ClassCastException if no such interface is available
     */
    <T> T unwrap(Class<T> clazz);

    /**
     * Returns whether this namespace implements a given interface, or wraps a
     * class which does.
     *
     * @param clazz Interface
     *
     * @return Whether namespace implements given interface
     */
    boolean isWrapperFor(Class<?> clazz);
}

// End SqlValidatorNamespace.java
