/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
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
package org.eigenbase.sql.validate;

import org.eigenbase.sql.*;

import java.util.List;

/**
 * Name-resolution scope. Represents any position in a parse tree than an
 * expression can be, or anything in the parse tree which has columns.
 *
 * <p>When validating an expression, say "foo"."bar", you first use the
 * {@link #resolve} method of the scope where the expression is defined
 * to locate "foo". If successful, this returns a {@link
 * SqlValidatorNamespace namespace} describing the type of the
 * resulting object.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public interface SqlValidatorScope
{
    /**
     * Returns the validator which created this scope.
     */
    SqlValidator getValidator();

    /**
     * Returns the root node of this scope.
     *
     * @post return != null
     */
    SqlNode getNode();

    /**
     * Looks up a node with a given name. Returns null if none is found.
     *
     * @param name Name of node to find
     * @param ancestorOut If not null, writes the ancestor scope here
     * @param offsetOut If not null, writes the offset within the ancestor
     *   here
     */
    SqlValidatorNamespace resolve(
        String name,
        SqlValidatorScope[] ancestorOut,
        int[] offsetOut);

    /**
     * Finds the table alias which is implicitly qualifying an
     * unqualified column name. Throws an error if there is not exactly
     * one table.
     *
     * <p>This method is only implemented in scopes (such as
     * {@link org.eigenbase.sql.validate.SelectScope}) which can be the context for name-resolution.
     * In scopes such as {@link org.eigenbase.sql.validate.IdentifierNamespace}, it throws
     * {@link UnsupportedOperationException}.</p>
     *
     * @param columnName
     * @param ctx Validation context, to appear in any error thrown
     * @return Table alias
     */
    String findQualifyingTableName(String columnName, SqlNode ctx);

    /**
     * Collects the {@link SqlMoniker}s of all possible columns in this scope.
     *
     * @param parentObjName if not null, used to resolve a namespace
     * from which to query the column names
     * @param result an array list of strings to add the result to
     */
    void findAllColumnNames(String parentObjName, List result);

    /**
     * Collects the {@link SqlMoniker}s of all possible tables in this scope.
     *
     * @param result an array list of strings to add the result to
     */
    void findAllTableNames(List result);

    /**
     * Converts an identifier into a fully-qualified identifier. For
     * example, the "empno" in "select empno from emp natural join dept"
     * becomes "emp.empno".
     */
    SqlIdentifier fullyQualify(SqlIdentifier identifier);

    void addChild(SqlValidatorNamespace ns, String alias);

    /**
     * Finds a window with a given name. Returns null if not found.
     */
    SqlWindow lookupWindow(String name);

    /**
     * Returns whether an expression is monotonic in this scope.
     * For example, if the scope has previously been sorted by columns
     * X, Y, then X is monotonic in this scope, but Y is not.
     */
    boolean isMonotonic(SqlNode expr);
}

// End SqlValidatorScope.java

