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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import java.util.Map;

/**
 * Validates the parse tree of a SQL statement, and
 * provides semantic information about the parse tree.
 *
 * <p>To create an instance of the default validator implementation, call
 * {@link SqlValidatorUtil#newValidator}.
 *
 * <h2>Visitor pattern</h2>
 *
 * <p>The validator interface is an instance of the
 * {@link org.eigenbase.util.Glossary#VisitorPattern visitor pattern}.
 * Implementations of the {@link SqlNode#validate} method call the
 * <code>validateXxx</code> method appropriate to the kind of node:
 * {@link SqlLiteral#validate(SqlValidator, SqlValidatorScope)} calls
 * {@link #validateLiteral(org.eigenbase.sql.SqlLiteral)};
 * {@link SqlCall#validate(SqlValidator, SqlValidatorScope)} calls
 * {@link #validateCall(org.eigenbase.sql.SqlCall, SqlValidatorScope)};
 * and so forth.
 *
 * <p>The {@link SqlNode#validateExpr(SqlValidator, SqlValidatorScope)} method
 * is as {@link SqlNode#validate(SqlValidator, SqlValidatorScope)} but is
 * called when the node is known to be a scalar expression.
 *
 * <h2>Scopes and namespaces</h2>
 *
 * <p>In order to resolve names to objects, the validator builds a map
 * of the structure of the query. This map consists of two types of
 * objects. A {@link SqlValidatorScope} describes the tables and
 * columns accessible at a particular point in the query; and a {@link
 * SqlValidatorNamespace} is a description of a data source used in a
 * query.
 *
 * <p>The validator builds the map by making a quick scan over the
 * query when the root {@link SqlNode} is first provided. Thereafter,
 * it supplies the correct scope or namespace object when it calls
 * validation methods.
 *
 * <p>The methods {@link #getSelectScope}, {@link #getFromScope},
 * {@link #getWhereScope}, {@link #getGroupScope}, {@link
 * #getHavingScope}, {@link #getOrderScope} and {@link #getJoinScope}
 * get the correct scope to resolve names in a particular clause of a
 * SQL statement.
 *
 * @author jhyde
 * @since Oct 28, 2004
 * @version $Id$
 */
public interface SqlValidator
{
    /**
     * Returns the catalog reader used by this validator.
     */
    SqlValidatorCatalogReader getCatalogReader();

    /**
     * Returns the operator table used by this validator.
     */
    SqlOperatorTable getOperatorTable();

    /**
     * Validates an expression tree. You can call this method multiple times,
     * but not reentrantly.
     *
     * @param topNode top of expression tree to be validated
     *
     * @return validated tree (possibly rewritten)
     *
     * @pre outermostNode == null
     */
    SqlNode validate(SqlNode topNode);

    /**
     * Validates an expression tree. You can call this method multiple times,
     * but not reentrantly.
     *
     * @param topNode top of expression tree to be validated
     *
     * @param nameToTypeMap map of simple name (String) to {@link RelDataType};
     * used to resolve {@link SqlIdentifier} references
     *
     * @return validated tree (possibly rewritten)
     *
     * @pre outermostNode == null
     */
    SqlNode validateParameterizedExpression(
        SqlNode topNode,
        Map nameToTypeMap);

    /**
     * Checks that a query (<code>select</code> statement, or a set operation
     * <code>union</code>, <code>intersect</code>, <code>except</code>) is
     * valid.
     *
     * @throws RuntimeException if the query is not valid
     */
    void validateQuery(SqlNode node);

    /**
     * Returns the type assigned to a node by validation.
     *
     * @param node the node of interest
     *
     * @return validated type, never null
     */
    RelDataType getValidatedNodeType(SqlNode node);

    /**
     * Resolves an identifier to a fully-qualified name.
     *
     * @param id Identifier
     */
    void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope);

    /**
     * Validates a literal.
     */
    void validateLiteral(SqlLiteral literal);

    /**
     * Validates a {@link SqlIntervalQualifier}
     */
    void validateIntervalQualifier(SqlIntervalQualifier qualifier);

    /**
     * Validates an INSERT statement.
     */
    void validateInsert(SqlInsert insert);

    /**
     * Validates an UPDATE statement.
     */
    void validateUpdate(SqlUpdate update);

    /**
     * Validates a DELETE statement.
     */
    void validateDelete(SqlDelete delete);

    /**
     * Validates a data type expression.
     */
    void validateDataType(SqlDataTypeSpec dataType);

    /**
     * Validates a dynamic parameter.
     */
    void validateDynamicParam(SqlDynamicParam dynamicParam);

    /**
     * Validates the right-hand side of an OVER expression. It might be
     * either an {@link SqlIdentifier identifier} referencing a window, or
     * an {@link SqlWindow inline window specification}.
     */
    void validateWindow(
        SqlNode windowOrId,
        SqlValidatorScope scope,
        SqlCall call);

    /**
     * Validates a call to an operator.
     */
    void validateCall(SqlCall call, SqlValidatorScope scope);

    /**
     * Derives the type of a node in a given scope. If the type has already
     * been inferred, returns the previous type.
     *
     * @param scope  Syntactic scope
     * @param operand Parse tree node
     * @return Type of the SqlNode. Should never return <code>NULL</code>
     */
    RelDataType deriveType(
        SqlValidatorScope scope,
        SqlNode operand);

    /**
     * Adds "line x, column y" context to a validator exception.
     *
     * <p>Note that the input exception is checked (it derives from
     * {@link Exception}) and the output exception is unchecked (it derives
     * from {@link RuntimeException}). This is intentional -- it should remind
     * code authors to provide context for their validation errors.
     *
     * @param e The validation error
     * @param node The place where the exception occurred
     * @return
     *
     * @pre node != null
     * @post return != null
     */
    EigenbaseException newValidationError(
        SqlNode node,
        SqlValidatorException e);

    /**
     * Returns whether a SELECT statement is an aggregation. Criteria are:
     * (1) contains GROUP BY, or
     * (2) contains HAVING, or
     * (3) SELECT or ORDER BY clause contains aggregate functions. (Windowed
     *     aggregate functions, such as <code>SUM(x) OVER w</code>, don't
     *     count.)
     */
    boolean isAggregate(SqlSelect select);

    /**
     * Converts a window specification or window name into a fully-resolved
     * window specification.
     *
     * For example, in
     *
     * <code>SELECT sum(x) OVER (PARTITION BY x ORDER BY y),
     *   sum(y) OVER w1,
     *   sum(z) OVER (w ORDER BY y)
     * FROM t
     * WINDOW w AS (PARTITION BY x)</code>
     *
     * all aggregations have the same resolved window specification
     * <code>(PARTITION BY x ORDER BY y)</code>.
     *
     * @param windowOrRef Either the name of a window (a {@link SqlIdentifier})
     *   or a window specification (a {@link SqlWindow}).
     *
     * @param scope Scope in which to resolve window names
     *
     * @return A window
     * @throws RuntimeException Validation exception if window does not exist
     */
    SqlWindow resolveWindow(SqlNode windowOrRef, SqlValidatorScope scope);

    /**
     * Finds the namespace corresponding to a given node.
     *
     * <p>For example, in the query <code>SELECT * FROM (SELECT * FROM t), t1
     * AS alias</code>, the both items in the FROM clause have a corresponding
     * namespace.
     */
    SqlValidatorNamespace getNamespace(SqlNode node);

    /**
     * Derives an alias for an expression. If no alias can be derived, returns
     * null if <code>ordinal</code> is less than zero, otherwise generates an
     * alias <code>EXPR$<i>ordinal</i></code>.
     */
    String deriveAlias(
        SqlNode node,
        int ordinal);

    /**
     * Returns a list of expressions, with every occurrence of "&#42;" or
     * "TABLE.&#42;" expanded.
     */
    SqlNodeList expandStar(
        SqlNodeList selectList,
        SqlSelect query,
        boolean includeSystemVars);

    /**
     * Returns the scope that expressions in the WHERE and GROUP BY clause of
     * this query should use. This scope consists of the tables in the FROM
     * clause, and the enclosing scope.
     */
    SqlValidatorScope getWhereScope(SqlSelect select);

    /**
     * Returns the type factory used by this validator.
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * Saves the type of a {@link SqlNode}, now that it has been validated.
     *
     * @param node A SQL parse tree node
     * @param type Its type; must not be null
     *
     * @deprecated This method should not be in the {@link SqlValidator}
     *   interface. The validator should drive the type-derivation process, and
     *   store nodes' types when they have been derived.
     * @pre type != null
     * @pre node != null
     */
    void setValidatedNodeType(
        SqlNode node,
        RelDataType type);

    /**
     * Returns an object representing the "unknown" type.
     */
    RelDataType getUnknownType();

    /**
     * Returns the appropriate scope for validating a particular clause of
     * a SELECT statement.
     *
     * <p>Consider
     *
     * <blockquote><code><pre>SELECT *
     * FROM foo
     * WHERE EXISTS (
     *    SELECT deptno AS x
     *    FROM emp
     *       JOIN dept ON emp.deptno = dept.deptno
     *    WHERE emp.deptno = 5
     *    GROUP BY deptno
     *    ORDER BY x)</pre></code></blockquote>
     *
     * What objects can be seen in each part of the sub-query?<ul>
     *
     * <li>In FROM ({@link #getFromScope} , you can only see 'foo'.
     *
     * <li>In WHERE ({@link #getWhereScope}),
     * GROUP BY ({@link #getGroupScope}),
     * SELECT ({@link #getSelectScope}), and
     * the ON clause of the JOIN ({@link #getJoinScope})
     * you can see 'emp', 'dept', and 'foo'.
     *
     * <li>In ORDER BY ({@link #getOrderScope}), you can see the column alias
     * 'x'; and tables 'emp', 'dept', and 'foo'.
     * </ul>
     */
    SqlValidatorScope getSelectScope(SqlSelect select);

    /**
     * Returns a scope containing the objects visible from the FROM clause
     * of a query.
     */
    SqlValidatorScope getFromScope(SqlSelect select);

    /**
     * Returns a scope containing the objects visible from the ON and USING
     * sections of a JOIN clause.
     *
     * @param node The item in the FROM clause which contains the ON or USING
     *   expression
     *
     * @see #getFromScope
     */
    SqlValidatorScope getJoinScope(SqlNode node);

    /**
     * Returns a scope containing the objects visible from the GROUP BY clause
     * of a query.
     */
    SqlValidatorScope getGroupScope(SqlSelect select);

    /**
     * Returns a scope containing the objects visible from the HAVING clause
     * of a query.
     */
    SqlValidatorScope getHavingScope(SqlSelect select);

    /**
     * Returns the scope that expressions in the SELECT and HAVING clause of
     * this query should use. This scope consists of the FROM clause and the
     * enclosing scope. If the query is aggregating, only columns in the GROUP
     * BY clause may be used.
     */
    SqlValidatorScope getOrderScope(SqlSelect select);

    /**
     * Returns the boolean result of testing the node to
     * see if it's a constant
     */
    boolean isConstant(SqlNode expr);
}

// End SqlValidator.java
