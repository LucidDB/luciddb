/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import org.eigenbase.reltype.*;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.resgen.*;
import org.eigenbase.util.*;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Default implementation of {@link SqlValidator}.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 3, 2005
 */
public class SqlValidatorImpl implements SqlValidatorWithHints
{
    //~ Static fields/initializers --------------------------------------------

    //~ Instance fields -------------------------------------------------------

    private final SqlOperatorTable opTab;
    final SqlValidatorCatalogReader catalogReader;

    /**
     * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
     * objects at these positions
     */
    protected final Map<String, SqlIdentifier> sqlIds =
        new HashMap<String, SqlIdentifier>();

    /**
     * Maps ParsePosition strings to the {@link SqlValidatorScope} scope
     * objects at these positions
     */
    protected final Map<String, SqlValidatorScope> idScopes =
        new HashMap<String, SqlValidatorScope>();

    /**
     * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
     * scope created from them}.
     */
    protected final Map<SqlNode, SqlValidatorScope> scopes =
        new HashMap<SqlNode, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
     * clauses.
     */
    private final Map<SqlSelect, SqlValidatorScope> whereScopes =
        new HashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
     * clauses.
     */
    private final Map<SqlSelect, SqlValidatorScope> selectScopes =
        new HashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
     */
    private final Map<SqlSelect, SqlValidatorScope> orderScopes =
        new HashMap<SqlSelect, SqlValidatorScope>();

    /**
     * Maps a {@link SqlNode node} to the {@link SqlValidatorNamespace
     * namespace} which describes what columns they contain.
     */
    protected final Map<SqlNode, SqlValidatorNamespace> namespaces =
        new HashMap<SqlNode, SqlValidatorNamespace>();

    /**
     * Set of select expressions used as cursor definitions.  In standard
     * SQL, only the top-level SELECT is a cursor; Eigenbase extends
     * this with cursors as inputs to table functions.
     */
    private final Set<SqlNode> cursorSet = new HashSet<SqlNode>();

    private int nextGeneratedId;
    protected final RelDataTypeFactory typeFactory;
    protected final RelDataType unknownType;
    private final RelDataType booleanType;

    /**
     * Map of derived RelDataType for each node.  This is an IdentityHashMap
     * since in some cases (such as null literals) we need to discriminate by
     * instance.
     */
    private final Map<SqlNode, RelDataType> nodeToTypeMap =
        new IdentityHashMap<SqlNode, RelDataType>();

    public static final Logger tracer = EigenbaseTrace.parserTracer;
    private final AggFinder aggFinder = new AggFinder();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a validator.
     *
     * @pre opTab != null
     * @pre // node is a "query expression" (per SQL standard)
     * @pre catalogReader != null
     * @pre typeFactory != null
     */
    protected SqlValidatorImpl(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory)
    {
        Util.pre(opTab != null, "opTab != null");
        Util.pre(catalogReader != null, "catalogReader != null");
        Util.pre(typeFactory != null, "typeFactory != null");
        this.opTab = opTab;
        this.catalogReader = catalogReader;
        this.typeFactory = typeFactory;

        // NOTE jvs 23-Dec-2003:  This is used as the type for dynamic
        // parameters and null literals until a real type is imposed for them.
        unknownType = typeFactory.createSqlType(SqlTypeName.Null);
        booleanType = typeFactory.createSqlType(SqlTypeName.Boolean);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlValidatorCatalogReader getCatalogReader()
    {
        return catalogReader;
    }

    public SqlOperatorTable getOperatorTable()
    {
        return opTab;
    }

    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    public RelDataType getUnknownType()
    {
        return unknownType;
    }

    public SqlNodeList expandStar(
        SqlNodeList selectList,
        SqlSelect query,
        boolean includeSystemVars)
    {
        ArrayList list = new ArrayList();
        ArrayList aliases = new ArrayList();
        ArrayList types = new ArrayList();
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode selectItem = selectList.get(i);
            expandSelectItem(selectItem, query, list, aliases, types,
                includeSystemVars);
        }
        return new SqlNodeList(list, SqlParserPos.ZERO);
    }

    // implement SqlValidator
    public void declareCursor(SqlSelect select)
    {
        cursorSet.add(select);
    }

    /**
     * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
     * true; otherwise writes the unexpanded item.
     *
     * @param selectItem   Select-list item
     * @param select       Containing select clause
     * @param selectItems  List that expanded items are written to
     * @param aliases      List of aliases
     * @param types        List of data types in alias order
     * @param includeSystemVars If true include system vars in lists
     * @return Whether the node was expanded
     */
    private boolean expandSelectItem(
        final SqlNode selectItem,
        SqlSelect select,
        ArrayList selectItems,
        List aliases,
        List types,
        final boolean includeSystemVars)
    {
        final SelectScope scope = (SelectScope) getWhereScope(select);
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            if (identifier.names.length == 1 &&
                identifier.names[0].equals("*"))
            {
                SqlParserPos starPosition = identifier.getParserPosition();
                for (String tableName : scope.childrenNames) {
                    final SqlValidatorNamespace childScope =
                        scope.getChild(tableName);
                    final SqlNode from = childScope.getNode();
                    final SqlValidatorNamespace fromNs = getNamespace(from);
                    assert fromNs != null;
                    final RelDataType rowType = fromNs.getRowType();
                    final RelDataTypeField [] fields = rowType.getFields();
                    for (int j = 0; j < fields.length; j++) {
                        RelDataTypeField field = fields[j];
                        String columnName = field.getName();

                        //todo: do real implicit collation here
                        final SqlNode exp =
                            new SqlIdentifier(
                                new String [] {tableName, columnName},
                                starPosition);
                        addToSelectList(selectItems, aliases, types, exp,
                            scope, includeSystemVars);
                    }
                }
                return true;
            } else if (identifier.names.length == 2 &&
                identifier.names[1].equals("*"))
            {
                final String tableName = identifier.names[0];
                SqlParserPos starPosition = identifier.getParserPosition();
                final SqlValidatorNamespace childNs =
                    scope.getChild(tableName);
                if (childNs == null) {
                    // e.g. "select r.* from e"
                    throw newValidationError(
                        identifier,
                        EigenbaseResource.instance().UnknownIdentifier.ex(
                            tableName));
                }
                final SqlNode from = childNs.getNode();
                final SqlValidatorNamespace fromNs = getNamespace(from);
                assert fromNs != null;
                final RelDataType rowType = fromNs.getRowType();
                final RelDataTypeField [] fields = rowType.getFields();
                for (int i = 0; i < fields.length; i++) {
                    RelDataTypeField field = fields[i];
                    String columnName = field.getName();

                    //todo: do real implicit collation here
                    final SqlIdentifier exp =
                        new SqlIdentifier(
                            new String [] { tableName, columnName },
                            starPosition);
                    addToSelectList(selectItems, aliases, types, exp, scope,
                        includeSystemVars);
                }
                return true;
            }
        }
        selectItems.add(selectItem);
        final String alias = deriveAlias(selectItem, aliases.size());
        aliases.add(alias);

        final RelDataType type = deriveType(scope, selectItem);
        setValidatedNodeTypeImpl(selectItem, type);
        types.add(type);
        return false;
    }

    public SqlNode validate(SqlNode topNode)
    {
        SqlValidatorScope scope = new EmptyScope(this);
        return validateScopedExpression(topNode, scope);
    }

    /**
     * Looks up completion hints for a syntatically correct SQL that has been
     * parsed into an expression tree
     * (Note this should be called after validate())
     *
     * @param topNode top of expression tree in which to lookup completion hints
     *
     * @param pos indicates the position in the sql statement we want to get
     * completion hints for. For example,
     * "select a.ename, b.deptno from sales.emp a join sales.dept b
     * "on a.deptno=b.deptno where empno=1";
     * setting pos to 'Line 1, Column 17' returns all the possible column names
     * that can be selected from sales.dept table
     * setting pos to 'Line 1, Column 31' returns all the possible table names
     * in 'sales' schema
     *
     * @return an array of {@link SqlMoniker} (sql identifiers) that can fill in at
     * the indicated position
     *
     */
    public SqlMoniker[] lookupHints(SqlNode topNode, SqlParserPos pos)
    {
        SqlValidatorScope scope = new EmptyScope(this);
        SqlNode outermostNode = performUnconditionalRewrites(topNode);
        cursorSet.add(outermostNode);
        if (outermostNode.getKind().isA(SqlKind.TopLevel)) {
            registerQuery(scope, null, outermostNode, null, false);
        }
        final SqlValidatorNamespace ns = getNamespace(outermostNode);
        if (ns == null) {
            throw Util.newInternal("Not a query: " + outermostNode);
        }
        return ns.lookupHints(pos);
    }

    /**
     * Looks up the fully qualified name for a {@link SqlIdentifier} at a given
     * Parser Position in a parsed expression tree
     * Note: call this only after {@link #validate} has been called.
     *
     * @param topNode top of expression tree in which to lookup the qualfied
     * name for the SqlIdentifier
     * @param pos indicates the position of the {@link SqlIdentifier} in the sql
     * statement we want to get the qualified name for
     *
     * @return a string of the fully qualified name of the {@link SqlIdentifier}
     * if the Parser position represents a valid {@link SqlIdentifier}.  Else
     * return an empty string
     *
     */
    public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos)
    {
        final String posString = pos.toString();
        SqlIdentifier id = sqlIds.get(posString);
        SqlValidatorScope scope = idScopes.get(posString);
        if (id != null && scope != null) {
            return new SqlIdentifierMoniker(scope.fullyQualify(id));
        } else {
            return null;
        }
    }

    /**
     * Looks up completion hints for a syntatically correct select SQL
     * that has been parsed into an expression tree
     *
     * @param select the Select node of the parsed expression tree
     * @param pos indicates the position in the sql statement we want to get
     * completion hints for
     *
     * @return an array list of {@link SqlMoniker} (sql identifiers) that can fill
     * in at the indicated position
     *
     */
    SqlMoniker[] lookupSelectHints(SqlSelect select, SqlParserPos pos)
    {
        SqlIdentifier dummyId = sqlIds.get(pos.toString());
        SqlValidatorScope dummyScope = idScopes.get(pos.toString());
        if (dummyId == null || dummyScope == null) {
            SqlNode fromNode = select.getFrom();
            final SqlValidatorScope fromScope = getFromScope(select);
            return lookupFromHints(fromNode, fromScope, pos);
        } else {
            return dummyId.findValidOptions(this, dummyScope);
        }
    }

    private SqlMoniker[] lookupFromHints(SqlNode node,
        SqlValidatorScope scope, SqlParserPos pos)
    {
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns instanceof IdentifierNamespace) {
            IdentifierNamespace idNs = (IdentifierNamespace) ns;
            if (pos.toString().equals(
                    idNs.getId().getParserPosition().toString()))
            {
                SqlMoniker[] objNames = catalogReader.getAllSchemaObjectNames(
                    idNs.getId().names);

                ArrayList result = new ArrayList();
                for (int i = 0; i < objNames.length; i++)  {
                    if (objNames[i].getType() != SqlMonikerType.Function) {
                        result.add(objNames[i]);
                    }
                }
                return (SqlMoniker[])result.toArray(new SqlMoniker[0]);
            }
        }
        switch (node.getKind().getOrdinal()) {
        case SqlKind.JoinORDINAL:
            return lookupJoinHints((SqlJoin) node, scope, pos);
        default:
            return getNamespace(node).lookupHints(pos);
        }
    }

    private SqlMoniker[] lookupJoinHints(
        SqlJoin join, SqlValidatorScope scope, SqlParserPos pos)
    {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        SqlMoniker [] result = lookupFromHints(left, scope, pos);
        if (result.length > 0) {
            return result;
        }
        result = lookupFromHints(right, scope, pos);
        if (result.length > 0) {
            return result;
        }
        SqlJoinOperator.ConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = (SqlValidatorScope) scopes.get(join);
        switch (conditionType.getOrdinal()) {
        case SqlJoinOperator.ConditionType.On_ORDINAL:
            return condition.findValidOptions(this, joinScope, pos);
        default:
        // not supporting lookup hints for other types such as 'Using' yet
            return Util.emptySqlMonikerArray;
        }
    }

    public SqlNode validateParameterizedExpression(
        SqlNode topNode,
        final Map nameToTypeMap)
    {
        SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
        return validateScopedExpression(topNode, scope);
    }

    private SqlNode validateScopedExpression(
        SqlNode topNode, SqlValidatorScope scope)
    {
        SqlNode outermostNode = performUnconditionalRewrites(topNode);
        cursorSet.add(outermostNode);
        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer("After unconditional rewrite: " +
                outermostNode.toString());
        }
        if (outermostNode.getKind().isA(SqlKind.TopLevel)) {
            registerQuery(scope, null, outermostNode, null, false);
        }
        outermostNode.validate(this, scope);
        if (!outermostNode.getKind().isA(SqlKind.TopLevel)) {
            // force type derivation so that we can provide it to the
            // caller later without needing the scope
            deriveType(scope, outermostNode);
        }
        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer("After validation: " + outermostNode.toString());
        }
        return outermostNode;
    }

    public void validateQuery(SqlNode node)
    {
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns == null) {
            throw Util.newInternal("Not a query: " + node);
        }

        validateNamespace(ns);
        validateAccess(ns.getTable(), SqlAccessEnum.SELECT);
    }

    /**
     * Validates a namespace.
     */
    protected void validateNamespace(final SqlValidatorNamespace namespace)
    {
        namespace.validate();
        setValidatedNodeType(namespace.getNode(), namespace.getRowType());
    }

    public SqlValidatorScope getWhereScope(SqlSelect select)
    {
        return whereScopes.get(select);
    }

    public SqlValidatorScope getSelectScope(SqlSelect select)
    {
        return selectScopes.get(select);
    }

    public SqlValidatorScope getHavingScope(SqlSelect select)
    {
        // Yes, it's the same as getSelectScope
        return selectScopes.get(select);
    }

    public SqlValidatorScope getGroupScope(SqlSelect select)
    {
        // Yes, it's the same as getWhereScope
        return whereScopes.get(select);
    }

    public SqlValidatorScope getFromScope(SqlSelect select)
    {
        return (SqlValidatorScope) scopes.get(select);
    }

    public SqlValidatorScope getOrderScope(SqlSelect select)
    {
        return orderScopes.get(select);
    }

    public SqlValidatorScope getJoinScope(SqlNode node) {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            return getJoinScope(((SqlCall) node).operands[0]);
        default:
            return (SqlValidatorScope) scopes.get(node);
        }
    }

    public SqlValidatorScope getOverScope(SqlNode node)
    {
        return (SqlValidatorScope) scopes.get(node);
    }

    /**
     * Returns the appropriate scope for validating a particular clause of
     * a SELECT statement.
     *
     * <p>Consider
     *
     * SELECT *
     * FROM foo
     * WHERE EXISTS (
     *    SELECT deptno AS x
     *    FROM emp, dept
     *    WHERE emp.deptno = 5
     *    GROUP BY deptno
     *    ORDER BY x)
     *
     * In FROM, you can only see 'foo'.
     * In WHERE, GROUP BY and SELECT, you can see 'emp', 'dept', and 'foo'.
     * In ORDER BY, you can see the column alias 'x', 'emp', 'dept', and 'foo'.
     */
    public SqlValidatorScope getScope(SqlSelect select, int operandType)
    {
        switch (operandType) {
        case SqlSelect.FROM_OPERAND:
            return (SqlValidatorScope) scopes.get(select);
        case SqlSelect.WHERE_OPERAND:
        case SqlSelect.GROUP_OPERAND:
            return whereScopes.get(select);
        case SqlSelect.HAVING_OPERAND:
        case SqlSelect.SELECT_OPERAND:
            return selectScopes.get(select);
        case SqlSelect.ORDER_OPERAND:
            return orderScopes.get(select);
        default:
            throw Util.newInternal("Unexpected operandType " + operandType);
        }
    }

    public SqlValidatorNamespace getNamespace(SqlNode node) {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
        case SqlKind.OverORDINAL:
        case SqlKind.CollectionTableORDINAL:
        case SqlKind.OrderByORDINAL:
            return getNamespace(((SqlCall) node).operands[0]);
        default:
            return namespaces.get(node);
        }
    }

    /**
     * Performs expression rewrites which are always used unconditionally.
     * These rewrites massage the expression tree into a standard form so that
     * the rest of the validation logic can be similar.
     *
     * @param node expression to be rewritten
     *
     * @return rewritten expression
     */
    protected SqlNode performUnconditionalRewrites(SqlNode node)
    {
        if (node == null) {
            return node;
        }

        // first transform operands and invoke generic call rewrite
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            final SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                SqlNode newOperand = performUnconditionalRewrites(operand);
                if (newOperand != null) {
                    call.setOperand(i, newOperand);
                }
            }

            if (call.getOperator() instanceof SqlFunction) {
                SqlFunction function = (SqlFunction) call.getOperator();
                if (function.getFunctionType() == null) {
                    // This function hasn't been resolved yet.  Perform
                    // a half-hearted resolution now in case it's a
                    // builtin function requiring special casing.  If it's
                    // not, we'll handle it later during overload
                    // resolution.
                    List overloads = opTab.lookupOperatorOverloads(
                        function.getNameAsId(),
                        null,
                        SqlSyntax.Function);
                    if (overloads.size() == 1) {
                        call.setOperator((SqlOperator) overloads.get(0));
                    }
                }
            }
            node = call.getOperator().rewriteCall(this, call);
        } else if (node instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) node;
            for (int i = 0, count = list.size(); i < count; i++) {
                SqlNode operand = list.get(i);
                SqlNode newOperand = performUnconditionalRewrites(operand);
                if (newOperand != null) {
                    list.getList().set(i, newOperand);
                }
            }
        }

        // now transform node itself
        if (node.isA(SqlKind.Values)) {
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            SqlSelect wrapperNode =
                SqlStdOperatorTable.selectOperator.createCall(
                    null, selectList, node, null, null, null, null, null,
                    node.getParserPosition());
            return wrapperNode;
        } else if (node.isA(SqlKind.OrderBy)) {
            SqlCall orderBy = (SqlCall) node;
            SqlNode query =
                orderBy.getOperands()[SqlOrderByOperator.QUERY_OPERAND];
            SqlNodeList orderList = (SqlNodeList)
                orderBy.getOperands()[SqlOrderByOperator.ORDER_OPERAND];
            if (query instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) query;

                // Don't clobber existing ORDER BY.  It may be needed for
                // an order-sensitive function like RANK.
                if (select.getOrderList() == null) {
                    // push ORDER BY into existing select
                    select.setOperand(SqlSelect.ORDER_OPERAND, orderList);
                    return select;
                }
            }
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            SqlSelect wrapperNode =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, query, null, null, null, null, orderList,
                        SqlParserPos.ZERO);
            return wrapperNode;
        } else if (node.isA(SqlKind.ExplicitTable)) {
            // (TABLE t) is equivalent to (SELECT * FROM t)
            SqlCall call = (SqlCall) node;
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            SqlSelect wrapperNode =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, call.getOperands()[0], null, null, null, null,
                    null, SqlParserPos.ZERO);
            return wrapperNode;
        } else if (node.isA(SqlKind.Delete)) {
            SqlDelete call = (SqlDelete) node;
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            SqlNode sourceTable = call.getTargetTable();
            if (call.getAlias() != null) {
                sourceTable = SqlValidatorUtil.addAlias(
                    sourceTable, call.getAlias().getSimple());
            }
            SqlSelect select =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, sourceTable, call.getCondition(),
                    null, null, null, null, SqlParserPos.ZERO);
            call.setOperand(SqlDelete.SOURCE_SELECT_OPERAND, select);
        } else if (node.isA(SqlKind.Update)) {
            SqlUpdate call = (SqlUpdate) node;
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            Iterator iter =
                call.getSourceExpressionList().getList().iterator();
            int ordinal = 0;
            while (iter.hasNext()) {
                SqlNode exp = (SqlNode) iter.next();
             
                // Force unique aliases to avoid a duplicate for Y with
                // SET X=Y
                String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
                selectList.add(SqlValidatorUtil.addAlias(exp, alias));
                ++ordinal;
            }
            SqlNode sourceTable = call.getTargetTable();
            if (call.getAlias() != null) {
                sourceTable = SqlValidatorUtil.addAlias(
                    sourceTable, call.getAlias().getSimple());
            }
            SqlSelect select =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, sourceTable, call.getCondition(),
                    null, null, null, null, SqlParserPos.ZERO);
            call.setOperand(SqlUpdate.SOURCE_SELECT_OPERAND, select);
        } else if (node.isA(SqlKind.Merge)) {
            SqlMerge call = (SqlMerge) node;
            SqlUpdate updateStmt = call.getUpdateCall();
            // just clone the select list from the update statement's source
            // since it's the same as what we want for the select list of
            // the merge source -- '*' followed by the update set expressions
            SqlNodeList selectList = (SqlNodeList)
                updateStmt.getSourceSelect().getSelectList().clone();
            SqlNode targetTable = call.getTargetTable();
            if (call.getAlias() != null) {
                targetTable = SqlValidatorUtil.addAlias(
                    targetTable, call.getAlias().getSimple());
            }
            // Source select for the merge is a left outer join between the
            // source in the USING clause and the target table; need to clone
            // source table reference in order for validation to work
            SqlNode sourceTableRef = call.getSourceTableRef();
            call.setSourceTableRef(sourceTableRef);
            SqlNode leftJoinTerm = (SqlNode) sourceTableRef.clone();
            SqlNode outerJoin = SqlStdOperatorTable.joinOperator.createCall(
                leftJoinTerm,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                SqlLiteral.createSymbol(
                    SqlJoinOperator.JoinType.Left, SqlParserPos.ZERO),
                targetTable,
                SqlLiteral.createSymbol(
                    SqlJoinOperator.ConditionType.On, SqlParserPos.ZERO),
                call.getCondition(),
                SqlParserPos.ZERO);
            SqlSelect select =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, outerJoin, null,
                    null, null, null, null, SqlParserPos.ZERO);
            call.setOperand(SqlMerge.SOURCE_SELECT_OPERAND, select);
            
            // Source for the insert call is a select of the source table
            // reference with the select list being the value expressions;
            // note that the values clause has already been converted to a
            // select on the values row constructor; so we need to extract
            // that via the from clause on the select
            SqlInsert insertCall = call.getInsertCall();
            SqlSelect valuesSelect = (SqlSelect) insertCall.getSource();
            SqlCall valuesCall = (SqlCall) valuesSelect.getFrom();
            SqlCall rowCall = (SqlCall) valuesCall.getOperands()[0];
            selectList = new SqlNodeList(
                Arrays.asList(rowCall.getOperands()), SqlParserPos.ZERO);
            SqlNode insertSource = (SqlNode) sourceTableRef.clone();
            select =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, insertSource, null, null, null, null, null,
                    SqlParserPos.ZERO);
            insertCall.setOperand(SqlInsert.SOURCE_OPERAND, select);
        }
        return node;
    }

    RelDataType getTableConstructorRowType(
        SqlCall values,
        SqlValidatorScope scope)
    {
        assert values.getOperands().length >= 1;
        RelDataType [] rowTypes = new RelDataType[values.getOperands().length];
        for (int iRow = 0; iRow < values.getOperands().length; ++iRow) {
            final SqlNode operand = values.getOperands()[iRow];
            assert(operand.isA(SqlKind.Row));
            SqlCall rowConstructor = (SqlCall) operand;

            // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
            // rows, need to infer aliases from there.
            SqlNode [] operands = rowConstructor.getOperands();
            final List<String> aliasList = new ArrayList<String>();
            final List<RelDataType> typeList = new ArrayList<RelDataType>();
            for (int iCol = 0; iCol < operands.length; ++iCol) {
                final String alias = deriveAlias(operands[iCol], iCol);
                aliasList.add(alias);
                final RelDataType type = deriveType(scope, operands[iCol]);
                typeList.add(type);
            }
            rowTypes[iRow] = typeFactory.createStructType(typeList, aliasList);
        }
        if (values.getOperands().length == 1) {
            // TODO jvs 10-Oct-2005:  get rid of this workaround once
            // leastRestrictive can handle all cases
            return rowTypes[0];
        }
        return typeFactory.leastRestrictive(rowTypes);
    }

    public RelDataType getValidatedNodeType(SqlNode node)
    {
        RelDataType type = getValidatedNodeTypeIfKnown(node);
        if (type == null) {
            throw Util.needToImplement(node);
        } else {
            return type;
        }
    }

    public RelDataType getValidatedNodeTypeIfKnown(SqlNode node)
    {
        final RelDataType type = nodeToTypeMap.get(node);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns != null) {
            return ns.getRowType();
        }
        return null;
    }
    
    public void setValidatedNodeType(
        SqlNode node,
        RelDataType type)
    {
        setValidatedNodeTypeImpl(node, type);
    }

    void setValidatedNodeTypeImpl(SqlNode node, RelDataType type)
    {
        Util.pre(type != null, "type != null");
        Util.pre(node != null, "node != null");
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return;
        }
        nodeToTypeMap.put(node, type);
    }

    public RelDataType deriveType(
        SqlValidatorScope scope,
        SqlNode expr)
    {
        Util.pre(scope != null, "scope != null");
        Util.pre(expr != null, "expr != null");

        // if we already know the type, no need to re-derive
        RelDataType type = nodeToTypeMap.get(expr);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(expr);
        if (ns != null) {
            return ns.getRowType();
        }
        type = deriveTypeImpl(scope, expr);
        Util.permAssert(type != null,
            "SqlValidator.deriveTypeInternal returned null");
        setValidatedNodeTypeImpl(expr, type);
        return type;
    }

    /**
     * Derives the type of a node.
     *
     * @post return != null
     */
    RelDataType deriveTypeImpl(
        SqlValidatorScope scope,
        SqlNode operand)
    {
        DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
        return operand.accept(v);
    }

    public RelDataType deriveConstructorType(
        SqlValidatorScope scope,
        SqlCall call,
        SqlFunction unresolvedConstructor,
        SqlFunction resolvedConstructor,
        RelDataType [] argTypes)
    {
        SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
        assert(sqlIdentifier != null);
        RelDataType type = catalogReader.getNamedType(sqlIdentifier);
        if (type == null) {
            // TODO jvs 12-Feb-2005:  proper type name formatting
            throw newValidationError(
                sqlIdentifier,
                EigenbaseResource.instance().UnknownDatatypeName.ex(
                    sqlIdentifier.toString()));
        }

        if (resolvedConstructor == null) {
            if (call.getOperands().length > 0) {
                // This is not a default constructor invocation, and
                // no user-defined constructor could be found
                handleUnresolvedFunction(call, unresolvedConstructor, argTypes);
            }
        } else {
            SqlCall testCall = resolvedConstructor.createCall(
                call.getOperands(),
                call.getParserPosition());
            RelDataType returnType =
                resolvedConstructor.validateOperands(
                    this,
                    scope,
                    testCall);
            assert(type == returnType);
        }

        if (shouldExpandIdentifiers()) {
            if (resolvedConstructor != null) {
                call.setOperator(resolvedConstructor);
            } else {
                // fake a fully-qualified call to the default constructor
                SqlReturnTypeInference returnTypeInference =
                    new ExplicitReturnTypeInference(type);
                call.setOperator(
                    new SqlFunction(
                        type.getSqlIdentifier(),
                        returnTypeInference,
                        null,
                        null,
                        null,
                        SqlFunctionCategory.UserDefinedConstructor));
            }
        }
        return type;
    }

    public void handleUnresolvedFunction(
        SqlCall call,
        SqlFunction unresolvedFunction,
        RelDataType [] argTypes)
    {
        // For builtins, we can give a better error message
        List overloads = opTab.lookupOperatorOverloads(
            unresolvedFunction.getNameAsId(),
            null,
            SqlSyntax.Function);
        if (overloads.size() == 1) {
            SqlFunction fun = (SqlFunction) overloads.get(0);
            if ((fun.getSqlIdentifier() == null)
                && (fun.getSyntax() != SqlSyntax.FunctionId))
            {
                final Integer expectedArgCount = (Integer)
                    fun.getOperandCountRange().getAllowedList().get(0);
                throw newValidationError(
                    call,
                    EigenbaseResource.instance().InvalidArgCount.ex(
                        call.getOperator().getName(),
                        expectedArgCount));
            }
        }

        AssignableOperandTypeChecker typeChecking =
            new AssignableOperandTypeChecker(argTypes);
        String signature =
            typeChecking.getAllowedSignatures(
                unresolvedFunction, unresolvedFunction.getName());
        throw newValidationError(
            call,
            EigenbaseResource.instance().ValidatorUnknownFunction.ex(
                signature));
    }

    private void inferUnknownTypes(
        RelDataType inferredType,
        SqlValidatorScope scope,
        SqlNode node)
    {
        final SqlValidatorScope newScope = (SqlValidatorScope) scopes.get(node);
        if (newScope != null) {
            scope = newScope;
        }
        boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
        if ((node instanceof SqlDynamicParam) || isNullLiteral) {
            if (inferredType.equals(unknownType)) {
                if (isNullLiteral) {
                    throw newValidationError(node,
                        EigenbaseResource.instance().NullIllegal.ex());
                } else {
                    throw newValidationError(node,
                        EigenbaseResource.instance().DynamicParamIllegal.ex());
                }
            }

            // REVIEW:  should dynamic parameter types always be nullable?
            RelDataType newInferredType =
                typeFactory.createTypeWithNullability(inferredType, true);
            if (SqlTypeUtil.inCharFamily(inferredType)) {
                newInferredType =
                    typeFactory.createTypeWithCharsetAndCollation(
                        newInferredType,
                        inferredType.getCharset(),
                        inferredType.getCollation());
            }
            setValidatedNodeTypeImpl(node, newInferredType);
        } else if (node instanceof SqlNodeList) {
            SqlNodeList nodeList = (SqlNodeList) node;
            if (inferredType.isStruct()) {
                if (inferredType.getFieldCount() != nodeList.size()) {
                    // this can happen when we're validating an INSERT
                    // where the source and target degrees are different;
                    // bust out, and the error will be detected higher up
                    return;
                }
            }
            Iterator iter = nodeList.getList().iterator();
            int i = 0;
            while (iter.hasNext()) {
                SqlNode child = (SqlNode) iter.next();
                RelDataType type;
                if (inferredType.isStruct()) {
                    type = inferredType.getFields()[i].getType();
                    ++i;
                } else {
                    type = inferredType;
                }
                inferUnknownTypes(type, scope, child);
            }
        } else if (node instanceof SqlCase) {
            // REVIEW wael: can this be done in a paramtypeinference strategy
            // object?
            SqlCase caseCall = (SqlCase) node;
            RelDataType returnType = deriveType(scope, node);

            SqlNodeList whenList = caseCall.getWhenOperands();
            for (int i = 0; i < whenList.size(); i++) {
                SqlNode sqlNode = whenList.get(i);
                inferUnknownTypes(unknownType, scope, sqlNode);
            }
            SqlNodeList thenList = caseCall.getThenOperands();
            for (int i = 0; i < thenList.size(); i++) {
                SqlNode sqlNode = thenList.get(i);
                inferUnknownTypes(returnType, scope, sqlNode);
            }

            if (!SqlUtil.isNullLiteral(
                        caseCall.getElseOperand(),
                        false)) {
                inferUnknownTypes(
                    returnType,
                    scope,
                    caseCall.getElseOperand());
            } else {
                setValidatedNodeTypeImpl(
                    caseCall.getElseOperand(),
                    returnType);
            }
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            SqlOperandTypeInference operandTypeInference =
                call.getOperator().getOperandTypeInference();
            SqlNode [] operands = call.getOperands();
            RelDataType [] operandTypes = new RelDataType[operands.length];
            if (operandTypeInference == null) {
                // TODO:  eventually should assert(operandTypeInference != null)
                // instead; for now just eat it
                Arrays.fill(operandTypes, unknownType);
            } else {
                operandTypeInference.inferOperandTypes(
                    new SqlCallBinding(this, scope, call),
                    inferredType, operandTypes);
            }
            for (int i = 0; i < operands.length; ++i) {
                inferUnknownTypes(operandTypes[i], scope, operands[i]);
            }
        }
    }

    /**
     * Adds an expression to a select list, ensuring that its alias does not
     * clash with any existing expressions on the list.
     */
    protected void addToSelectList(
        ArrayList list,
        List aliases,
        List types,
        SqlNode exp,
        SqlValidatorScope scope,
        final boolean includeSystemVars)
    {
        String alias = SqlValidatorUtil.getAlias(exp, -1);
        String uniqueAlias = SqlValidatorUtil.uniquify(alias, aliases);
        if (!alias.equals(uniqueAlias)) {
            exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
        }
        types.add(deriveType(scope, exp));
        list.add(exp);
    }

    public String deriveAlias(
        SqlNode node,
        int ordinal)
    {
        return SqlValidatorUtil.getAlias(node, ordinal);
    }

    protected boolean shouldExpandIdentifiers()
    {
        return false;
    }

    protected boolean shouldAllowIntermediateOrderBy()
    {
        return true;
    }

    /**
     * Registers a new namespace, and adds it as a child of its parent scope.
     *
     * @param usingScope Parent scope (which will want to look for things in
     *   this namespace)
     * @param alias Alias by which parent will refer to this namespace
     * @param ns Namespace
     */
    private void registerNamespace(
        SqlValidatorScope usingScope,
        String alias,
        SqlValidatorNamespace ns,
        boolean forceNullable)
    {
        if (forceNullable) {
            ns.makeNullable();
        }
        namespaces.put(ns.getNode(), ns);
        if (usingScope != null) {
            usingScope.addChild(ns, alias);
        }
    }

    private SqlNode registerFrom(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        final SqlNode node,
        String alias,
        boolean forceNullable)
    {
        final SqlKind kind = node.getKind();

        // Add an alias if necessary.
        SqlNode newNode = node;
        if (alias == null) {
            switch (kind.getOrdinal()) {
            case SqlKind.IdentifierORDINAL:
            case SqlKind.OverORDINAL:
                alias = deriveAlias(node, -1);
                if (shouldExpandIdentifiers()) {
                    newNode = SqlValidatorUtil.addAlias(node, alias);
                }
                break;

            case SqlKind.SelectORDINAL:
            case SqlKind.UnionORDINAL:
            case SqlKind.IntersectORDINAL:
            case SqlKind.ExceptORDINAL:
            case SqlKind.ValuesORDINAL:
            case SqlKind.UnnestORDINAL:
            case SqlKind.FunctionORDINAL:
            case SqlKind.CollectionTableORDINAL:
                // give this anonymous construct a name since later
                // query processing stages rely on it
                alias = deriveAlias(node, nextGeneratedId++);
                if (shouldExpandIdentifiers()) {
                    // Since we're expanding identifiers, we should make the
                    // aliases explicit too, otherwise the expanded query
                    // will not be consistent if we convert back to SQL, e.g.
                    // "select EXPR$1.EXPR$2 from values (1)".
                    newNode = SqlValidatorUtil.addAlias(node, alias);
                }
                break;
            }
        }

        switch (kind.getOrdinal()) {
        case SqlKind.AsORDINAL:
            {
                final SqlCall call = (SqlCall) node;
                if (alias == null) {
                    alias = call.operands[1].toString();
                }
                final SqlNode expr = call.operands[0];
                final SqlNode newExpr = registerFrom(
                    parentScope, usingScope, expr, alias, forceNullable);
                if (newExpr != expr) {
                    call.setOperand(0, newExpr);
                }
                return node;
            }

        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) node;
            final JoinScope joinScope = new JoinScope(parentScope, usingScope, join);
            scopes.put(join, joinScope);
            final SqlNode left = join.getLeft();
            boolean forceLeftNullable = forceNullable;
            boolean forceRightNullable = forceNullable;
            if (join.getJoinType() == SqlJoinOperator.JoinType.Left) {
                forceRightNullable = true;
            }
            if (join.getJoinType() == SqlJoinOperator.JoinType.Right) {
                forceLeftNullable = true;
            }
            if (join.getJoinType() == SqlJoinOperator.JoinType.Full) {
                forceLeftNullable = true;
                forceRightNullable = true;
            }
            final SqlNode newLeft = registerFrom(parentScope, joinScope, left,
                null, forceLeftNullable);
            if (newLeft != left) {
                join.setOperand(SqlJoin.LEFT_OPERAND, newLeft);
            }
            final SqlNode right = join.getRight();
            final SqlValidatorScope rightParentScope;
            if (right.isA(SqlKind.Lateral) ||
                    (right.isA(SqlKind.As) &&
                    ((SqlCall) right).operands[0].isA(SqlKind.Lateral))) {
                rightParentScope = joinScope;
            } else {
                rightParentScope = parentScope;
            }
            final SqlNode newRight = registerFrom(rightParentScope, joinScope,
                right, null, forceRightNullable);
            if (newRight != right) {
                join.setOperand(SqlJoin.RIGHT_OPERAND, newRight);
            }
            final JoinNamespace joinNamespace = new JoinNamespace(this, join);
            registerNamespace(null, null, joinNamespace, forceNullable);
            return join;

        case SqlKind.IdentifierORDINAL:
            final SqlIdentifier id = (SqlIdentifier) node;
            final IdentifierNamespace newNs = new IdentifierNamespace(
                this, id);
            registerNamespace(usingScope, alias, newNs, forceNullable);
            return newNode;
            
        case SqlKind.LateralORDINAL:
            return registerFrom(
                parentScope,
                usingScope,
                ((SqlCall) node).operands[0],
                alias,
                forceNullable);

        case SqlKind.CollectionTableORDINAL:
            {
                SqlCall call = (SqlCall) node;
                final SqlNode operand = call.operands[0];
                final SqlNode newOperand =
                    registerFrom(
                        parentScope,
                        usingScope,
                        operand,
                        alias,
                        forceNullable);
                if (newOperand != operand) {
                    call.setOperand(0, newOperand);
                }
                return newNode;
            }

        case SqlKind.SelectORDINAL:
        case SqlKind.UnionORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.ValuesORDINAL:
        case SqlKind.UnnestORDINAL:
        case SqlKind.FunctionORDINAL:
            registerQuery(parentScope, usingScope, node, alias, forceNullable);
            return newNode;

        case SqlKind.OverORDINAL:
            if (!shouldAllowOverRelation()) {
                throw kind.unexpected();
            }
            SqlCall call = (SqlCall) node;
            final OverScope overScope = new OverScope(usingScope, call);
            scopes.put(call, overScope);
            final SqlNode operand = call.operands[0];
            final SqlNode newOperand =
                registerFrom(parentScope, overScope, operand, alias, false);
            if (newOperand != operand) {
                call.setOperand(0, newOperand);
            }

            for (String tableName : overScope.childrenNames) {
                final SqlValidatorNamespace childSpace =
                    overScope.getChild(tableName);
                registerNamespace(usingScope, tableName, childSpace, false);
            }

            return newNode;

        default:
            throw kind.unexpected();
        }
    }

    protected boolean shouldAllowOverRelation()
    {
        return false;
    }

    protected SelectNamespace allocSelectNamespace(SqlSelect select)
    {
        return new SelectNamespace( this, select);
    }

    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to
     *   resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node
     * @param alias Name of this query within its parent. Must be specified
     *   if usingScope != null
     * @pre usingScope == null || alias != null
     */
    private void registerQuery(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        SqlNode node,
        String alias,
        boolean forceNullable)
    {
        Util.pre(usingScope == null || alias != null,
            "usingScope == null || alias != null");

        SqlCall call;
        SqlNode [] operands;
        switch (node.getKind().getOrdinal()) {
        case SqlKind.SelectORDINAL:
            final SqlSelect select = (SqlSelect) node;
            final SelectNamespace selectNs = allocSelectNamespace(select);
            registerNamespace(usingScope, alias, selectNs, forceNullable);
            SelectScope selectScope = new SelectScope(parentScope, select);
            scopes.put(select, selectScope);
            // Start by registering the WHERE clause
            whereScopes.put(select, selectScope);
            registerSubqueries(
                selectScope,
                select.getWhere());
            // Register FROM with the inherited scope 'parentScope', not
            // 'selectScope', otherwise tables in the FROM clause would be
            // able to see each other.
            final SqlNode from = select.getFrom();
            final SqlNode newFrom = registerFrom(
                parentScope,
                selectScope,
                from,
                null,
                false);
            if (newFrom != from) {
                select.setOperand(SqlSelect.FROM_OPERAND, newFrom);
            }
            // If this is an aggregating query, the SELECT list and HAVING
            // clause use a different scope, where you can only reference
            // columns which are in the GROUP BY clause.
            SqlValidatorScope aggScope = selectScope;
            if (isAggregate(select)) {
                aggScope = new AggregatingSelectScope(selectScope, select);
                selectScopes.put(select, aggScope);
            } else {
                selectScopes.put(select, selectScope);
            }
            registerSubqueries(
                selectScope,
                select.getGroup());
            registerSubqueries(
                aggScope,
                select.getHaving());
            registerSubqueries(
                aggScope,
                select.getSelectList());
            final SqlNodeList orderList = select.getOrderList();
            if (orderList != null) {
                SqlValidatorScope orderByScope =
                    new OrderByScope(aggScope, orderList, select);
                orderScopes.put(select, orderByScope);
                registerSubqueries(orderByScope, orderList);
            }
            break;

        case SqlKind.IntersectORDINAL:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_F302,
                node.getParserPosition());
            registerSetop(parentScope, usingScope, node, alias, forceNullable);
            break;
            
        case SqlKind.ExceptORDINAL:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_E071_03,
                node.getParserPosition());
            registerSetop(parentScope, usingScope, node, alias, forceNullable);
            break;
            
        case SqlKind.UnionORDINAL:
            registerSetop(parentScope, usingScope, node, alias, forceNullable);
            break;

        case SqlKind.ValuesORDINAL:
            final TableConstructorNamespace tableConstructorNamespace =
                new TableConstructorNamespace(this, node, parentScope);
            registerNamespace(
                usingScope, alias, tableConstructorNamespace, forceNullable);
            call = (SqlCall) node;
            operands = call.getOperands();
            for (int i = 0; i < operands.length; ++i) {
                assert(operands[i].isA(SqlKind.Row));

                // FIXME jvs 9-Feb-2005:  Correlation should
                // be illegal in these subqueries.  Same goes for
                // any non-lateral SELECT in the FROM list.
                registerSubqueries(parentScope, operands[i]);
            }
            break;

        case SqlKind.InsertORDINAL:
            SqlInsert insertCall = (SqlInsert) node;
            InsertNamespace insertNs = new InsertNamespace(this, insertCall);
            registerNamespace(usingScope, null, insertNs, forceNullable);
            registerQuery(
                parentScope,
                usingScope,
                insertCall.getSource(),
                null,
                false);
            break;

        case SqlKind.DeleteORDINAL:
            SqlDelete deleteCall = (SqlDelete) node;
            DeleteNamespace deleteNs = new DeleteNamespace(this, deleteCall);
            registerNamespace(usingScope, null, deleteNs, forceNullable);
            registerQuery(
                parentScope,
                usingScope,
                deleteCall.getSourceSelect(),
                null,
                false);
            break;

        case SqlKind.UpdateORDINAL:
            SqlUpdate updateCall = (SqlUpdate) node;
            UpdateNamespace updateNs = new UpdateNamespace(this, updateCall);
            registerNamespace(usingScope, null, updateNs, forceNullable);
            registerQuery(
                parentScope,
                usingScope,
                updateCall.getSourceSelect(),
                null,
                false);
            break;
            
        case SqlKind.MergeORDINAL:
            validateFeature(
                EigenbaseResource.instance().SQLFeature_F312,
                node.getParserPosition());
            SqlMerge mergeCall = (SqlMerge) node;
            MergeNamespace mergeNs = new MergeNamespace(this, mergeCall);
            registerNamespace(usingScope, null, mergeNs, forceNullable);
            registerQuery(
                parentScope,
                usingScope,
                mergeCall.getSourceSelect(),
                null,
                false);
            // update call can reference either the source table reference
            // or the target table, so set its parent scope to the merge's
            // source select
            registerQuery(
                (ListScope) whereScopes.get(mergeCall.getSourceSelect()),
                null,
                mergeCall.getUpdateCall(),
                null,
                false);
            registerQuery(
                parentScope,
                null,
                mergeCall.getInsertCall(),
                null,
                false);
            break;

        case SqlKind.UnnestORDINAL:
            call = (SqlCall) node;
            final UnnestNamespace unnestNs =
                new UnnestNamespace(this, call, usingScope);
            registerNamespace(
                usingScope, alias, unnestNs, forceNullable);
            registerSubqueries(usingScope, call.operands[0]);
            break;

        case SqlKind.FunctionORDINAL:
            call = (SqlCall) node;
            ProcedureNamespace procNs =
                new ProcedureNamespace(this, parentScope, call);
            registerNamespace(
                usingScope, alias, procNs, forceNullable);
            registerSubqueries(parentScope, call);
            break;
            
        case SqlKind.MultisetQueryConstructorORDINAL:
            call = (SqlCall) node;
            CollectScope cs =
                new CollectScope(parentScope, usingScope, call);
             final CollectNamespace ttableConstructorNs =
                new CollectNamespace(call, cs);
            final String alias2 = deriveAlias(node, nextGeneratedId++);
            registerNamespace(
                usingScope, alias2, ttableConstructorNs, forceNullable);
            operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                registerSubqueries(parentScope, operand);
            }
            break;

        default:
            throw node.getKind().unexpected();
        }
    }

    private void registerSetop(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        SqlNode node,
        String alias,
        boolean forceNullable)
    {
        SqlCall call = (SqlCall) node;
        final SetopNamespace setopNamespace =
            new SetopNamespace(this, call);
        registerNamespace(usingScope, alias, setopNamespace, forceNullable);
        // A setop is in the same scope as its parent.
        scopes.put(call, parentScope);
        registerQuery(parentScope, null, call.operands[0], null, false);
        registerQuery(parentScope, null, call.operands[1], null, false);
    }

    public boolean isAggregate(SqlSelect select) {
        return select.getGroup() != null ||
            select.getHaving() != null ||
            aggFinder.findAgg(select.getSelectList()) != null;
    }

    public boolean isConstant(SqlNode expr)
    {
        return expr instanceof SqlLiteral ||
                expr instanceof SqlDynamicParam ||
                expr instanceof SqlDataTypeSpec;
    }

    private void registerSubqueries(
        SqlValidatorScope parentScope,
        SqlNode node)
    {
        if (node == null) {
            return;
        } else if (node.isA(SqlKind.Query)) {
            registerQuery(parentScope, null, node, null, false);
        } else if (node.isA(SqlKind.MultisetQueryConstructor)) {
            registerQuery(parentScope, null, node, null, false);
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            final SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                registerSubqueries(parentScope, operand);
            }
        } else if (node instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) node;
            for (int i = 0, count = list.size(); i < count; i++) {
                registerSubqueries(parentScope, list.get(i));
            }
        } else {
            ; // atomic node -- can be ignored
        }
    }

    public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope)
    {
        final SqlIdentifier fqId = scope.fullyQualify(id);
        Util.discard(fqId);
    }

    public void validateLiteral(SqlLiteral literal)
    {
        switch (literal.getTypeName().getOrdinal()) {
        case SqlTypeName.Binary_ordinal:
            final BitString bitString = (BitString) literal.getValue();
            if (bitString.getBitCount() % 8 != 0) {
                throw newValidationError(literal,
                    EigenbaseResource.instance().BinaryLiteralOdd.ex());
            }
            break;
        case SqlTypeName.IntervalYearMonth_ordinal:
        case SqlTypeName.IntervalDayTime_ordinal:
            if (literal instanceof SqlIntervalLiteral) {
                SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue)
                        ((SqlIntervalLiteral) literal).getValue();
                int[] values = SqlParserUtil.parseIntervalValue(interval);
                if (values == null) {
                    throw newValidationError(
                        literal,
                        EigenbaseResource.instance()
                        .UnsupportedIntervalLiteral.ex(
                            interval.toString(),
                            "INTERVAL " +
                            interval.getIntervalQualifier().toString()));
                }
            }
            break;
        default:
            // default is to do nothing
        }
    }

    public void validateIntervalQualifier(SqlIntervalQualifier qualifier)
    {
        // default is to do nothing
    }

    /**
     * Validates the FROM clause of a query, or (recursively) a child node of
     * the FROM clause: AS, OVER, JOIN, VALUES, or subquery.
     *
     * @param node
     * @param targetRowType Desired row type of this expression, or
     *   {@link #unknownType} if not fussy. Must not be null.
     * @param scope
     */
    protected void validateFrom(
        SqlNode node,
        RelDataType targetRowType,
        SqlValidatorScope scope)
    {
        Util.pre(targetRowType != null, "targetRowType != null");
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            validateFrom(((SqlCall) node).getOperands()[0], targetRowType, scope);
            return;
        case SqlKind.ValuesORDINAL:
            validateValues((SqlCall) node, targetRowType, scope);
            return;
        case SqlKind.JoinORDINAL:
            validateJoin((SqlJoin) node, scope);
            return;
        case SqlKind.OverORDINAL:
            validateOver((SqlCall) node, scope);
            return;
        default:
            validateQuery(node);
        }
    }

    protected void validateOver(SqlCall call, SqlValidatorScope scope)
    {
        throw Util.newInternal("OVER unexpected in this context");
    }

    protected void validateJoin(SqlJoin join, SqlValidatorScope scope)
    {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        boolean natural = join.isNatural();
        SqlJoinOperator.JoinType joinType = join.getJoinType();
        SqlJoinOperator.ConditionType conditionType =
            join.getConditionType();
        final SqlValidatorScope joinScope = (SqlValidatorScope) scopes.get(join);
        validateFrom(left, unknownType, joinScope);
        validateFrom(right, unknownType, joinScope);

        // Validate condition.
        switch (conditionType.getOrdinal()) {
        case SqlJoinOperator.ConditionType.None_ORDINAL:
            Util.permAssert(condition == null, "condition == null");
            break;
        case SqlJoinOperator.ConditionType.On_ORDINAL:
            Util.permAssert(condition != null, "condition != null");
            condition.validate(this, joinScope);
            break;
        case SqlJoinOperator.ConditionType.Using_ORDINAL:
            SqlNodeList list = (SqlNodeList) condition;
            // Parser ensures that using clause is not empty.
            Util.permAssert(list.size() > 0, "Empty USING clause");
            for (int i = 0; i < list.size(); i++) {
                SqlIdentifier id = (SqlIdentifier) list.get(i);
                validateUsingCol(id, left);
                validateUsingCol(id, right);
            }
            break;
        default:
            throw conditionType.unexpected();
        }
        // Validate NATURAL.
        if (natural && condition != null) {
            throw newValidationError(condition,
                EigenbaseResource.instance()
                .NaturalDisallowsOnOrUsing.ex());
        }
        // Which join types require/allow a ON/USING condition, or allow
        // a NATURAL keyword?
        switch (joinType.getOrdinal()) {
        case SqlJoinOperator.JoinType.Inner_ORDINAL:
        case SqlJoinOperator.JoinType.Left_ORDINAL:
        case SqlJoinOperator.JoinType.Right_ORDINAL:
        case SqlJoinOperator.JoinType.Full_ORDINAL:
            if (condition == null && !natural) {
                throw newValidationError(
                    join,
                    EigenbaseResource.instance()
                    .JoinRequiresCondition.ex());
            }
            break;
        case SqlJoinOperator.JoinType.Comma_ORDINAL:
        case SqlJoinOperator.JoinType.Cross_ORDINAL:
            if (condition != null) {
                throw newValidationError(
                    join.operands[SqlJoin.CONDITION_TYPE_OPERAND],
                    EigenbaseResource.instance()
                    .CrossJoinDisallowsCondition.ex());
            }
            if (natural) {
                throw newValidationError(
                    join.operands[SqlJoin.CONDITION_TYPE_OPERAND],
                    EigenbaseResource.instance()
                    .CrossJoinDisallowsCondition.ex());
            }
            break;
        default:
            throw joinType.unexpected();
        }
    }

    private void validateUsingCol(SqlIdentifier id, SqlNode leftOrRight) {
        if (id.names.length == 1) {
            String name = id.names[0];
            final SqlValidatorNamespace namespace = getNamespace(leftOrRight);
            boolean exists = namespace.fieldExists(name);
            if (exists) {
                return;
            }
        }
        throw newValidationError(id,
            EigenbaseResource.instance().ColumnNotFound.ex(
                id.toString()));
    }

    /**
     * Validates a SELECT statement.
     *
     * @param select Select statement
     * @param targetRowType Desired row type, must not be null, may be the
     *   data type 'unknown'.
     * @pre targetRowType != null
     */
    protected void validateSelect(
        SqlSelect select,
        RelDataType targetRowType)
    {
        Util.pre(targetRowType != null, "targetRowType != null");
        final SelectNamespace ns = (SelectNamespace) getNamespace(select);
        assert ns.rowType == null;

        if (select.isDistinct()) {
            validateFeature(
                EigenbaseResource.instance().SQLFeature_E051_01,
                select.getModifierNode(
                    SqlSelectKeyword.Distinct).getParserPosition());
        }

        final SqlNodeList selectItems = select.getSelectList();
        RelDataType fromType = unknownType;
        if (selectItems.size() == 1) {
            final SqlNode selectItem = selectItems.get(0);
            if (selectItem instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) selectItem;
                if (id.isStar() && id.names.length == 1) {
                    // Special case: for INSERT ... VALUES(?,?), the SQL standard
                    // says we're supposed to propagate the target types down.  So
                    // iff the select list is an unqualified star (as it will be
                    // after an INSERT ... VALUES has been expanded), then
                    // propagate.
                    fromType = targetRowType;
                }
            }
        }
        final SqlValidatorScope fromScope = getFromScope(select);
        validateFrom(select.getFrom(), fromType, fromScope);

        validateWhereClause(select);
        validateGroupClause(select);
        validateHavingClause(select);
        validateWindowClause(select);

        // Validate the SELECT clause late, because a select item might
        // depend on the GROUP BY list, or the window function might reference
        // window name in the WINDOW clause etc.
        ns.rowType = validateSelectList(selectItems, select, targetRowType);

        // Validate ORDER BY after we have set ns.rowType because in some
        // dialects you can refer to columns of the select list, e.g.
        // "SELECT empno AS x FROM emp ORDER BY x"
        validateOrderList(select);
    }

    private void validateWindowClause(SqlSelect select)
    {
        final SqlNodeList windowList = select.getWindowList();
        if (windowList == null || windowList.size() == 0) {
            return;
        }

        final SelectScope windowScope = (SelectScope)getFromScope(select);
        Util.permAssert(windowScope != null, "windowScope != null");

        // 1. ensure window names are simple
        // 2. ensure they are unique within this scope
        Iterator iter = windowList.getList().iterator();
        while (iter.hasNext()) {
            final SqlWindow child = (SqlWindow) iter.next();

            SqlIdentifier declName = child.getDeclName();
            if (!declName.isSimple()){
                throw newValidationError(declName,
                    EigenbaseResource.instance().WindowNameMustBeSimple.ex());
            }

            if (windowScope.existingWindowName(declName.toString())) {
                throw newValidationError(declName,
                    EigenbaseResource.instance().DuplicateWindowName.ex());
            } else {
                windowScope.addWindowName(declName.toString());
            }
        }
        // 7.10 rule 2
        if (2 <= windowList.size()) {
            SqlNode[] winArr = windowList.toArray();

            for (int i = 0; i < windowList.size() - 1; i++) {
                for (int j = i + 1; j < windowList.size(); j++) {
                    if (winArr[i].equalsDeep(winArr[j], false)) {
                        throw newValidationError(winArr[j],
                            EigenbaseResource.instance().DupWindowSpec.ex());
                    }
                }
            }
        }

        // Hand off to validate window spec components
        windowList.validate(this, windowScope);
    }

    private void validateOrderList(SqlSelect select)
    {
        // ORDER BY is validated in a scope where aliases in the SELECT clause
        // are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
        // is valid.
        SqlNodeList orderList = select.getOrderList();
        if (orderList == null) {
            return;
        }
        if (!shouldAllowIntermediateOrderBy()) {
            if (!cursorSet.contains(select)) {
                throw newValidationError(select,
                    EigenbaseResource.instance().InvalidOrderByPos.ex());
            }
        }
        final SqlValidatorScope orderScope = getOrderScope(select);
        Util.permAssert(orderScope != null, "orderScope != null");
        orderList.validate(this, orderScope);
    }

    /**
     * Validates the GROUP BY clause of a SELECT statement. This method is
     * called even if no GROUP BY clause is present.
     */
    protected void validateGroupClause(SqlSelect select)
    {
        SqlNodeList group = select.getGroup();
        if (group == null) {
            return;
        }
        final SqlValidatorScope groupScope = getGroupScope(select);
        inferUnknownTypes(unknownType, groupScope, group);
        group.validate(this, groupScope);
    }

    protected void validateWhereClause(SqlSelect select)
    {
        // validate WHERE clause
        final SqlNode where = select.getWhere();
        if (where == null) {
            return;
        }
        final SqlValidatorScope whereScope = getWhereScope(select);
        final SqlNode agg = aggFinder.findAgg(where);
        if (agg != null) {
            throw newValidationError(agg,
                EigenbaseResource.instance().AggregateIllegalInWhere.ex());
        }
        inferUnknownTypes(
            booleanType,
            whereScope,
            where);
        where.validate(this, whereScope);
        final RelDataType type = deriveType(whereScope, where);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(where,
                EigenbaseResource.instance().WhereMustBeBoolean.ex());
        }
    }

    protected void validateHavingClause(SqlSelect select) {
        // HAVING is validated in the scope after groups have been created.
        // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
        // deptno HAVING empno = 10", the reference to 'empno' in the HAVING
        // clause is illegal.
        final SqlNode having = select.getHaving();
        if (having == null) {
            return;
        }
        final SqlValidatorScope havingScope = getSelectScope(select);
        SqlValidatorScope inferScope = havingScope;
        if (havingScope instanceof AggregatingScope) {
            AggregatingScope aggScope = (AggregatingScope) havingScope;
            aggScope.checkAggregateExpr(having);
            inferScope = aggScope.getScopeAboveAggregation();
        }
        inferUnknownTypes(
            booleanType,
            inferScope,
            having);
        having.validate(this, havingScope);
        final RelDataType type = deriveType(havingScope, having);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(having,
                EigenbaseResource.instance().HavingMustBeBoolean.ex());
        }
    }

    protected RelDataType validateSelectList(
        final SqlNodeList selectItems,
        SqlSelect select,
        RelDataType targetRowType)
    {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        final SqlValidatorScope selectScope = getSelectScope(select);
        final ArrayList expandedSelectItems = new ArrayList();
        final List<String> aliasList = new ArrayList<String>();
        final List<RelDataType> typeList = new ArrayList<RelDataType>();
        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode selectItem = selectItems.get(i);
            if (selectScope instanceof AggregatingScope) {
                AggregatingScope aggScope =
                    (AggregatingScope) selectScope;
                boolean matches = aggScope.checkAggregateExpr(selectItem);
                Util.discard(matches);
            }
            if (selectItem instanceof SqlSelect) {
                handleScalarSubQuery(select, (SqlSelect)selectItem,
                    expandedSelectItems,aliasList, typeList);

            } else {
                expandSelectItem(selectItem, select, expandedSelectItems,
                    aliasList, typeList, false);
            }
        }

        // Create the new select list with expanded items.  Pass through
        // the original parser position so that any over all failures can
        // still reference the original input text
        SqlNodeList newSelectList =
            new SqlNodeList(expandedSelectItems, selectItems.getParserPosition());
        if (shouldExpandIdentifiers()) {
            select.setOperand(SqlSelect.SELECT_OPERAND, newSelectList);
        }

        // TODO: when SELECT appears as a value subquery, should be using
        // something other than unknownType for targetRowType
        inferUnknownTypes(targetRowType, selectScope, newSelectList);

        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode selectItem = selectItems.get(i);
            selectItem.validateExpr(this, selectScope);
        }

        assert typeList.size() == aliasList.size();
        return typeFactory.createStructType(typeList, aliasList);
    }

    /**
     * Processes SubQuery found in Select list.  Checks that is actually
     * Scalar subquery and makes proper entries in each of the 3 lists
     * used to create the final rowType entry.
     *
     * @param parentSelect  base SqlSelect item
     * @param selectItem  child SqlSelect from select list
     * @param expandedSelectItems Select items after processing
     * @param aliasList built from user or system values
     * @param typeList Built up entries for each select list entry
     */
    private void handleScalarSubQuery(
        SqlSelect parentSelect,
        SqlSelect selectItem,
        ArrayList expandedSelectItems,
        List aliasList,
        List typeList)
    {
        // A scalar subquery only has one output column.
        if (1 != selectItem.getSelectList().size()) {
            throw newValidationError(
                selectItem,
                EigenbaseResource.instance().OnlyScalarSubqueryAllowed.ex());
        }

        // No expansion in this routine just append to list.
        expandedSelectItems.add(selectItem);

        // Get or generate alias and add to list.
        final String alias = deriveAlias(selectItem, aliasList.size());
        aliasList.add(alias);

        final SelectScope scope = (SelectScope) getWhereScope(parentSelect);
        final RelDataType type = deriveType(scope, selectItem);
        setValidatedNodeTypeImpl(selectItem, type);

        // we do not want to pass on the RelRecordType returned
        // by the sub query.  Just the type of the single expression
        // in the subquery select list.
        assert type instanceof RelRecordType;
        RelRecordType rec = (RelRecordType) type;

        RelDataType nodeType = rec.getFields()[0].getType();
        nodeType = typeFactory.createTypeWithNullability(nodeType,true);
        typeList.add(nodeType);
    }


    /**
     * Derives a row-type for INSERT and UPDATE operations.
     *
     * @param table Target table for INSERT/UPDATE
     * @param targetColumnList List of target columns, or null if not specified
     * @param append Whether to append fields to those in
     *               <code>baseRowType</code>
     * @return
     */
    protected RelDataType createTargetRowType(
        SqlValidatorTable table,
        SqlNodeList targetColumnList,
        boolean append)
    {
        RelDataType baseRowType = table.getRowType();
        if (targetColumnList == null) {
            return baseRowType;
        }
        RelDataTypeField [] targetFields = baseRowType.getFields();
        int targetColumnCount = targetColumnList.size();
        if (append) {
            targetColumnCount += baseRowType.getFieldCount();
        }
        RelDataType [] types = new RelDataType[targetColumnCount];
        String [] fieldNames = new String[targetColumnCount];
        int iTarget = 0;
        if (append) {
            iTarget += baseRowType.getFieldCount();
            for (int i = 0; i < iTarget; ++i) {
                types[i] = targetFields[i].getType();
                fieldNames[i] = SqlUtil.deriveAliasFromOrdinal(i);
            }
        }
        Iterator iter = targetColumnList.getList().iterator();
        while (iter.hasNext()) {
            SqlIdentifier id = (SqlIdentifier) iter.next();
            int iColumn = baseRowType.getFieldOrdinal(id.getSimple());
            if (iColumn == -1) {
                throw newValidationError(id,
                    EigenbaseResource.instance().UnknownTargetColumn.ex(
                        id.getSimple()));
            }
            fieldNames[iTarget] = targetFields[iColumn].getName();
            types[iTarget] = targetFields[iColumn].getType();
            ++iTarget;
        }
        return typeFactory.createStructType(types, fieldNames);
    }

    public void validateInsert(SqlInsert insert)
    {
        InsertNamespace targetNamespace =
            (InsertNamespace) getNamespace(insert);
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        // INSERT has an optional column name list.  If present then
        // reduce the rowtype to the columns specified.  If not present
        // then the entire target rowtype is used.
        RelDataType targetRowType =
            createTargetRowType(
                table,
                insert.getTargetColumnList(),
                false);

        SqlNode source = insert.getSource();
        if (source instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) source;
            validateSelect(sqlSelect, targetRowType);
        } else {
            validateQuery(source);
        }
        RelDataType sourceRowType = getNamespace(source).getRowType();
        RelDataType logicalTargetRowType =
            getLogicalTargetRowType(targetRowType, insert);
        RelDataType logicalSourceRowType =
            getLogicalSourceRowType(sourceRowType, insert);
        
        checkFieldCount(logicalSourceRowType, logicalTargetRowType);

        checkTypeAssignment(logicalSourceRowType, logicalTargetRowType, insert);

        validateAccess(table, SqlAccessEnum.INSERT);
    }

    private void checkFieldCount(RelDataType sourceRowType, RelDataType logicalTargetRowType)
    {
        final int sourceFieldCount = sourceRowType.getFieldCount();
        final int targetFieldCount = logicalTargetRowType.getFieldCount();
        if (sourceFieldCount != targetFieldCount) {
            throw EigenbaseResource.instance().UnmatchInsertColumn.ex(
                targetFieldCount, sourceFieldCount);
        }
    }

    protected RelDataType getLogicalTargetRowType(
        RelDataType targetRowType,
        SqlInsert insert)
    {
        return targetRowType;
    }

    protected RelDataType getLogicalSourceRowType(
        RelDataType sourceRowType,
        SqlInsert insert)
    {
        return sourceRowType;
    }

    protected void checkTypeAssignment(
        RelDataType sourceRowType,
        RelDataType targetRowType,
        final SqlNode query)
    {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
        // representing system-maintained columns, so stop after all sources
        // matched
        RelDataTypeField [] sourceFields = sourceRowType.getFields();
        RelDataTypeField [] targetFields = targetRowType.getFields();
        final int sourceCount = sourceFields.length;
        for (int i = 0; i < sourceCount; ++i) {
            RelDataType sourceType = sourceFields[i].getType();
            RelDataType targetType = targetFields[i].getType();
            if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
                SqlNode node = getNthExpr(query, i, sourceCount);
                throw newValidationError(
                    node,
                    EigenbaseResource.instance().TypeNotAssignable.ex(
                        targetFields[i].getName(),
                        targetType.toString(),
                        sourceFields[i].getName(),
                        sourceType.toString()));
            }
        }
    }

    /**
     * Locates the n'th expression in an INSERT or UPDATE query.
     *
     * @param query Query
     * @param ordinal Ordinal of expression
     * @param sourceCount Number of expressions
     * @return Ordinal'th expression, never null
     */
    private SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount)
    {
        if (query instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) query;
            if (insert.getTargetColumnList() != null) {
                return insert.getTargetColumnList().get(ordinal);
            } else {
                return getNthExpr(insert.getSource(), ordinal, sourceCount);
            }
        } else if (query instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) query;
            if (update.getTargetColumnList() != null) {
                return update.getTargetColumnList().get(ordinal);
            } else if (update.getSourceExpressionList() != null) {
                return update.getSourceExpressionList().get(ordinal);
            } else {
                return getNthExpr(
                    update.getSourceSelect(), ordinal, sourceCount);
            }
        } else if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            if (select.getSelectList().size() == sourceCount) {
                return select.getSelectList().get(ordinal);
            } else {
                return query; // give up
            }
        } else {
            return query; // give up
        }
    }

    public void validateDelete(SqlDelete call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect, unknownType);

        IdentifierNamespace targetNamespace =
            (IdentifierNamespace) getNamespace(call.getTargetTable());
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        validateAccess(table, SqlAccessEnum.DELETE);
    }

    public void validateUpdate(SqlUpdate call)
    {    
        IdentifierNamespace targetNamespace =
            (IdentifierNamespace) getNamespace(call.getTargetTable());
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        RelDataType targetRowType =
            createTargetRowType(
                table,
                call.getTargetColumnList(),
                true);

        SqlSelect select = call.getSourceSelect();
        validateSelect(select, targetRowType);

        RelDataType sourceRowType = getNamespace(select).getRowType();
        checkTypeAssignment(sourceRowType, targetRowType, call);

        validateAccess(table, SqlAccessEnum.UPDATE);
    }
    
    public void validateMerge(SqlMerge call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();
        // REVIEW zfong 5/25/06 - Does an actual type have to be passed
        // into validateSelect()?
        
        // REVIEW jvs 6-June-2006:  In general, passing unknownType like
        // this means we won't be able to correctly infer the types
        // for dynamic parameter markers (SET x = ?).  But
        // maybe validateUpdate and validateInsert below will do
        // the job?
        validateSelect(sqlSelect, unknownType);

        IdentifierNamespace targetNamespace =
            (IdentifierNamespace) getNamespace(call.getTargetTable());
        validateNamespace(targetNamespace);      
        
        validateUpdate(call.getUpdateCall());
        validateInsert(call.getInsertCall());

        SqlValidatorTable table = targetNamespace.getTable();
        validateAccess(table, SqlAccessEnum.UPDATE);
    }

    /**
     * Validates access to a table
     */
    private void validateAccess(
        SqlValidatorTable table,
        SqlAccessEnum requiredAccess)
    {
        if (table != null) {
            SqlAccessType access = table.getAllowedAccess();
            if (!access.allowsAccess(requiredAccess)) {
                throw EigenbaseResource.instance().AccessNotAllowed.ex(
                    requiredAccess.getName(),
                    Arrays.asList(table.getQualifiedName()).toString());
            }
        }
    }

    /**
     * Validates a VALUES clause
     */
    protected void validateValues(
        SqlCall node,
        RelDataType targetRowType,
        SqlValidatorScope scope)
    {
        assert node.isA(SqlKind.Values);

        final SqlNode [] operands = node.getOperands();
        for (int i = 0; i < operands.length; i++) {
            if (!operands[i].isA(SqlKind.Row)) {
                throw Util.needToImplement(
                    "Values function where operands are scalars");
            }
            SqlCall rowConstructor = (SqlCall) operands[i];
            if (targetRowType.isStruct() &&
                rowConstructor.getOperands().length !=
                targetRowType.getFieldCount()) {
                return;
            }

            inferUnknownTypes(
                targetRowType,
                scope,
                rowConstructor);
        }

        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            operand.validate(this, scope);
        }

        // validate that all row types have the same number of columns
        //  and that expressions in each column are compatible.
        // A values expression is turned into something that looks like
        // ROW(type00, type01,...), ROW(type11,...),...
        final int numOfRows = operands.length;
        if (numOfRows >= 2) {
            SqlCall firstRow = (SqlCall) operands[0];
            final int numOfColumns = firstRow.getOperands().length;

            // 1. check that all rows have the same cols length
            for (int row = 0; row < numOfRows; row++) {
                SqlCall thisRow = (SqlCall) operands[row];
                if (numOfColumns != thisRow.operands.length) {
                    throw newValidationError(node,
                        EigenbaseResource.instance()
                        .IncompatibleValueType.ex(
                            SqlStdOperatorTable.valuesOperator.getName()));
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for (int col = 0; col < numOfColumns; col++) {
                RelDataType [] types = new RelDataType[numOfRows];
                for (int row = 0; row < numOfRows; row++) {
                    SqlCall thisRow = (SqlCall) operands[row];
                    final SqlNode operand = thisRow.operands[col];
                    types[row] = deriveType(scope, operand);
                }

                final RelDataType type =
                    typeFactory.leastRestrictive(types);

                if (null == type) {
                    throw newValidationError(node,
                        EigenbaseResource.instance()
                        .IncompatibleValueType.ex(
                            SqlStdOperatorTable.valuesOperator.getName()));
                }
            }
        }
    }

    public void validateDataType(SqlDataTypeSpec dataType)
    {
    }

    public void validateDynamicParam(SqlDynamicParam dynamicParam)
    {
    }

    public EigenbaseException newValidationError(
        SqlNode node,
        SqlValidatorException e)
    {
        Util.pre(node != null, "node != null");
        final SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }

    protected SqlWindow getWindowByName(
        SqlIdentifier id,
        SqlValidatorScope scope)
    {
        SqlWindow window = null;
        if (id.isSimple()) {
            final String name = id.getSimple();
            window = scope.lookupWindow(name);
        }
        if (window == null) {
            throw newValidationError(
                id,
                EigenbaseResource.instance().WindowNotFound.ex(
                    id.toString()));
        }
        return window;
    }

    public SqlWindow resolveWindow(
        SqlNode windowOrRef,
        SqlValidatorScope scope)
    {
        SqlWindow window;
        if (windowOrRef instanceof SqlIdentifier) {
            window = getWindowByName((SqlIdentifier) windowOrRef, scope);
        } else {
            window = (SqlWindow) windowOrRef;
        }
        while (true) {
            final SqlIdentifier refId = window.getRefName();
            if (refId == null) {
                return window;
            }
            final String refName = refId.getSimple();
            SqlWindow refWindow = scope.lookupWindow(refName);
            if (refWindow == null) {
                throw newValidationError(refId,
                    EigenbaseResource.instance().WindowNotFound.ex(refName));
            }
            window = window.overlay(refWindow, this);
        }
    }

    //~ Inner Interfaces ------------------------------------------------------

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Validation status.
     */
    public static class Status extends EnumeratedValues.BasicValue {
        public static final int Unvalidated_ordinal = 0;
        public static final int InProgress_ordinal = 1;
        public static final int Valid_ordinal = 2;

        private Status(String name, int ordinal) {
            super(name, ordinal, null);
        }

        /**
         * Validation has not started for this scope.
         */
        public static final Status Unvalidated =
            new Status("Unvalidated", Unvalidated_ordinal);
        /**
         * Validation is in progress for this scope.
         */
        public static final Status InProgress =
            new Status("InProgress", InProgress_ordinal);
        /**
         * Validation has completed (perhaps unsuccessfully).
         */
        public static final Status Valid =
            new Status("Valid", Valid_ordinal);
    }

    SqlValidatorNamespace lookupFieldNamespace(
        RelDataType rowType,
        String name,
        SqlValidatorScope[] ancestorOut,
        int[] offsetOut)
    {
        final RelDataType dataType =
            SqlValidatorUtil.lookupField(rowType, name);
        return new SqlValidatorNamespace() {
            public SqlValidatorTable getTable()
            {
                return null;
            }

            public RelDataType getRowType()
            {
                return dataType;
            }

            public void setRowType(RelDataType rowType)
            {
                // intentionally empty
            }

            public void validate()
            {
            }

            public SqlMoniker[] lookupHints(SqlParserPos pos)
            {
                return Util.emptySqlMonikerArray;
            }

            public SqlNode getNode()
            {
                return null;
            }

            public SqlValidatorNamespace lookupChild(String name,
                SqlValidatorScope[] ancestorOut,
                int[] offsetOut)
            {
                return null;
            }

            public boolean fieldExists(String name)
            {
                return false;
            }

            public Object getExtra()
            {
                return null;
            }

            public void setExtra(Object o)
            {
            }

            public SqlNodeList getMonotonicExprs()
            {
                return null;
            }

            public boolean isMonotonic(String columnName)
            {
                return false;
            }

            public void makeNullable()
            {
            }
        };
    }

    public void validateWindow(
        SqlNode windowOrId,
        SqlValidatorScope scope,
        SqlCall call)
    {
        final SqlWindow targetWindow;
        switch (windowOrId.getKind().getOrdinal()) {
        case SqlKind.IdentifierORDINAL:
            // Just verify the window exists in this query.  It will validate
            // when the definition is processed
            targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
            break;
        case SqlKind.WindowORDINAL:
            targetWindow = (SqlWindow)windowOrId;
            break;
        default:
            throw windowOrId.getKind().unexpected();
        }

        Util.pre(null == targetWindow.getWindowCall(),
            "(null == targetWindow.getWindowFunctionCall()");
        targetWindow.setWindowCall(call);
        targetWindow.validate(this,scope);
        targetWindow.setWindowCall(null);
    }

    public void validateCall(SqlCall call, SqlValidatorScope scope)
    {
        SqlValidatorScope operandScope = scope;
        if (scope instanceof AggregatingScope) {
            AggregatingScope aggScope =
                (AggregatingScope) scope;
            if (call.getOperator().isAggregator()) {
                // If we're the 'SUM' node in 'select a + sum(b + c) from t
                // group by a', then we should validate our arguments in
                // the non-aggregating scope, where 'b' and 'c' are valid
                // column references.
                operandScope = aggScope.getScopeAboveAggregation();
            } else if (call instanceof SqlWindow){
                operandScope = aggScope.getScopeAboveAggregation();
            } else {
                // Check whether expression is constant within the group.
                //
                // If not, throws. Example, 'empno' in
                //    SELECT empno FROM emp GROUP BY deptno
                //
                // If it perfectly matches an expression in the GROUP BY
                // clause, we validate its arguments in the non-aggregating
                // scope. Example, 'empno + 1' in
                //
                //   SELET empno + 1 FROM emp GROUP BY empno + 1

                final boolean matches = aggScope.checkAggregateExpr(call);
                if (matches) {
                    operandScope = aggScope.getScopeAboveAggregation();
                }
            }
        }
        // Delegate validation to the operator.
        call.getOperator().validateCall(call, this, scope, operandScope);
    }

    /**
     * Validates that a particular feature is enabled.  By default, all
     * features are enabled; subclasses may override this method
     * to be more discriminating.
     *
     * @param feature feature being used, represented as a
     * resource definition from {@link EigenbaseResource}
     *
     * @param context parser position context for error reporting,
     * or null if none available
     */
    protected void validateFeature(
        ResourceDefinition feature,
        SqlParserPos context)
    {
        // By default, do nothing except to verify that the resource
        // represents a real feature definition.
        assert(feature.getProperties().get("FeatureDefinition") != null);
    }

    /**
     * Namespace for an INSERT statement.
     */
    private static class InsertNamespace extends IdentifierNamespace
    {
        private final SqlInsert node;

        public InsertNamespace(
            SqlValidatorImpl validator,
            SqlInsert node)
        {
            super(validator, node.getTargetTable());
            this.node = node;
            assert node != null;
        }

        public SqlNode getNode()
        {
            return node;
        }
    }

    /**
     * Namespace for an UPDATE statement.
     */
    private static class UpdateNamespace extends IdentifierNamespace
    {
        private final SqlUpdate node;

        public UpdateNamespace(
            SqlValidatorImpl validator,
            SqlUpdate node)
        {
            super(validator, node.getTargetTable());
            this.node = node;
            assert node != null;
        }

        public SqlNode getNode()
        {
            return node;
        }
    }

    /**
     * Namespace for a DELETE statement.
     */
    private static class DeleteNamespace extends IdentifierNamespace
    {
        private final SqlDelete node;

        public DeleteNamespace(
            SqlValidatorImpl validator,
            SqlDelete node)
        {
            super(validator, node.getTargetTable());
            this.node = node;
            assert node != null;
        }

        public SqlNode getNode()
        {
            return node;
        }
    }
    
    /**
     * Namespace for a MERGE statement.
     */
    private static class MergeNamespace extends IdentifierNamespace
    {
        private final SqlMerge node;

        public MergeNamespace(
            SqlValidatorImpl validator,
            SqlMerge node)
        {
            super(validator, node.getTargetTable());
            this.node = node;
            assert node != null;
        }

        public SqlNode getNode()
        {
            return node;
        }
    }

    /**
     * Visitor which derives the type of a given {@link SqlNode}.
     *
     * <p>Each method must return the derived type.
     * This visitor is basically a single-use dispatcher; the visit is
     * never recursive.
     */
    private class DeriveTypeVisitor implements SqlVisitor<RelDataType>
    {
        private final SqlValidatorScope scope;

        public DeriveTypeVisitor(SqlValidatorScope scope)
        {
            this.scope = scope;
        }

        public RelDataType visit(SqlLiteral literal)
        {
            return literal.createSqlType(typeFactory);
        }

        public RelDataType visit(SqlCall call)
        {
            final SqlOperator operator = call.getOperator();
            return operator.deriveType(SqlValidatorImpl.this, scope, call);
        }

        public RelDataType visit(SqlNodeList nodeList)
        {
            // Operand is of a type that we can't derive a type for.
            // If the operand is of a peculiar type, such as a SqlNodeList, then
            // you should override the operator's validateCall() method so that
            // it doesn't try to validate that operand as an expression.
            throw Util.needToImplement(nodeList);
        }

        public RelDataType visit(SqlIdentifier id)
        {
            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            SqlCall call = SqlUtil.makeCall(opTab, id);
            if (call != null) {
                return call.getOperator().validateOperands(
                    SqlValidatorImpl.this, scope, call);
            }

            RelDataType type = null;
            for (int i = 0; i < id.names.length; i++) {
                String name = id.names[i];
                if (i == 0) {
                    // REVIEW jvs 9-June-2005: The name resolution rules used
                    // here are supposed to match SQL:2003 Part 2 Section 6.6
                    // (identifier chain), but we don't currently have enough
                    // information to get everything right.  In particular,
                    // routine parameters are currently looked
                    // up via resolve; we could do a better job
                    // if they were looked up via resolveColumn.

                    // TODO jvs 9-June-2005:  Support schema-qualified
                    // table names here.  This was illegal in SQL-92, but
                    // became legal in SQL:1999.  (SQL:2003 Part 2 Section
                    // 6.6 Syntax Rule 8.b.vi)

                    SqlValidatorNamespace resolvedNs =
                        scope.resolve(name, null, null);

                    if (resolvedNs != null) {
                        // There's a namespace with the name we seek.
                        type = resolvedNs.getRowType();
                    }

                    // Give precedence to namespace found, unless there
                    // are no more identifier components.
                    if ((type == null) || (id.names.length == 1)) {
                        RelDataType colType = null;
                        if (scope instanceof ListScope) {
                            // See if there's a column with the name we seek in
                            // precisely one of the namespaces in this scope.
                            colType = ((ListScope) scope).resolveColumn(
                                name, id);
                        }
                        if (colType != null) {
                            type = colType;
                        }
                    }

                    if (type == null) {
                        throw newValidationError(id,
                            EigenbaseResource.instance().UnknownIdentifier.ex(
                                name));
                    }
                } else {
                    RelDataType fieldType = SqlValidatorUtil.lookupField(
                        type, name);
                    if (fieldType == null) {
                        throw newValidationError(
                            id,
                            EigenbaseResource.instance().UnknownField.ex(name));
                    }
                    type = fieldType;
                }
            }
            type = SqlTypeUtil.addCharsetAndCollation(type, getTypeFactory());
            return type;
        }

        public RelDataType visit(SqlDataTypeSpec dataType)
        {
            // Q. How can a data type have a type?
            // A. When it appears in an expression. (Say as the 2nd arg to the
            //    CAST operator.)
            validateDataType(dataType);
            return dataType.deriveType(SqlValidatorImpl.this);
        }

        public RelDataType visit(SqlDynamicParam param)
        {
            return unknownType;
        }

        public RelDataType visit(SqlIntervalQualifier intervalQualifier)
        {
            return typeFactory.createSqlIntervalType(intervalQualifier);
        }

        public RelDataType visitChild(
            SqlNode parent, int ordinal, SqlNode child)
        {
            throw Util.newInternal("visitor should not be recursive");
        }
    }
}

// End SqlValidator.java

