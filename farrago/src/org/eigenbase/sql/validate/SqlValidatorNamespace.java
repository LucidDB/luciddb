/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.Pair;


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
     * Looks up hints from this namespace.
     */
    void lookupHints(SqlParserPos pos, List<SqlMoniker> hintList);

    SqlNode getNode();

    SqlValidatorNamespace lookupChild(
        String name,
        SqlValidatorScope [] ancestorOut,
        int [] offsetOut);

    boolean fieldExists(String name);

    /**
     * Returns the object containing implementation-specific information.
     */
    Object getExtra();

    /**
     * Saves an object containing implementation-specific information.
     */
    void setExtra(Object o);

    /**
     * Returns a list of expressions which are monotonic in this namespace. For
     * example, if the namespace represents a relation ordered by a column
     * called "TIMESTAMP", then the list would contain a {@link
     * org.eigenbase.sql.SqlIdentifier} called "TIMESTAMP".
     */
    List<Pair<SqlNode,SqlMonotonicity>> getMonotonicExprs();

    /**
     * Returns whether and how a given column is sorted.
     */
    SqlMonotonicity getMonotonicity(String columnName);

    /**
     * Makes all fields in this namespace nullable (typically because it is on
     * the outer side of an outer join.
     */
    void makeNullable();
}

// End SqlValidatorNamespace.java
