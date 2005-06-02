/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.*;

import java.nio.charset.Charset;
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
public class SqlValidatorImpl implements SqlValidator
{
    //~ Static fields/initializers --------------------------------------------

    //~ Instance fields -------------------------------------------------------

    private final SqlOperatorTable opTab;
    final SqlValidatorCatalogReader catalogReader;

    /**
     * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
     * objects at these positions
     */
    protected final HashMap sqlids = new HashMap();

    /**
     * Maps ParsePosition strings to the {@link SqlValidatorScope} scope
     * objects at these positions
     */
    protected final HashMap idscopes = new HashMap();

    /**
     * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
     * scope created from them}.
     */
    protected final HashMap scopes = new HashMap();
    /**
     * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
     * clauses.
     */
    private final HashMap whereScopes = new HashMap();
    /**
     * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
     * clauses.
     */
    private final HashMap selectScopes = new HashMap();
    /**
     * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
     */
    private final HashMap orderScopes = new HashMap();
    /**
     * Maps a {@link SqlNode node} to the {@link SqlValidatorNamespace
     * namespace} which describes what columns they contain.
     */
    protected final HashMap namespaces = new HashMap();
    private SqlNode outermostNode;
    private int nextGeneratedId;
    private final RelDataTypeFactory typeFactory;
    protected final RelDataType unknownType;
    private final RelDataType booleanType;

    /**
     * Map of derived RelDataType for each node.  This is an IdentityHashMap
     * since in some cases (such as null literals) we need to discriminate by
     * instance.
     */
    private Map nodeToTypeMap = new IdentityHashMap();
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
        SqlSelect query)
    {
        ArrayList list = new ArrayList();
        ArrayList aliases = new ArrayList();
        ArrayList types = new ArrayList();
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode selectItem = selectList.get(i);
            expandSelectItem(selectItem, query, list, aliases, types);
        }
        return new SqlNodeList(list, SqlParserPos.ZERO);
    }

    /**
     * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
     * true; otherwise writes the unexpanded item.
     *
     * @param selectItem   Select-list item
     * @param select       Containing select clause
     * @param selectItems  List that expanded items are written to
     * @param aliases      List of aliases
     * @return Whether the node was expanded
     */
    private boolean expandSelectItem(
        final SqlNode selectItem,
        SqlSelect select,
        ArrayList selectItems,
        List aliases,
        List types)
    {
        final SelectScope scope = (SelectScope) getWhereScope(select);
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            if ((identifier.names.length == 1)
                    && identifier.names[0].equals("*")) {

                ArrayList tableNames = scope.childrenNames;
                for (int i = 0; i < tableNames.size(); i++) {
                    String tableName = (String) tableNames.get(i);
                    final SqlValidatorNamespace childScope = scope.getChild(tableName);
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
                                SqlParserPos.ZERO);
                        addToSelectList(selectItems, aliases, types, exp,
                            scope);
                    }
                }
                return true;
            } else if ((identifier.names.length == 2)
                    && identifier.names[1].equals("*")) {
                final String tableName = identifier.names[0];
                final SqlValidatorNamespace childScope = scope.getChild(tableName);
                if (childScope == null) {
                    // e.g. "select r.* from e"
                    throw newValidationError(identifier,
                        EigenbaseResource.instance().newUnknownIdentifier(
                            tableName));
                }
                final SqlNode from = childScope.getNode();
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
                            SqlParserPos.ZERO);
                    addToSelectList(selectItems, aliases, types, exp, scope);
                }
                return true;
            }
        }
        selectItems.add(selectItem);
        final String alias = deriveAlias(
                selectItem,
                aliases.size());
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
     * @param pp indicates the position in the sql statement we want to get
     * completion hints for. For example,
     * "select a.ename, b.deptno from sales.emp a join sales.dept b
     * "on a.deptno=b.deptno where empno=1";
     * setting pp to 'Line 1, Column 17' returns all the possible column names
     * that can be selected from sales.dept table
     * setting pp to 'Line 1, Column 31' returns all the possible table names
     * in 'sales' schema
     *
     * @return an array of string hints (sql identifiers) that can fill in at
     * the indicated position
     *
     */
    public String[] lookupHints(SqlNode topNode, SqlParserPos pp)
    {
        SqlValidatorScope scope = new EmptyScope(this);
        try {
            outermostNode = performUnconditionalRewrites(topNode);
            if (outermostNode.getKind().isA(SqlKind.TopLevel)) {
                registerQuery(scope, null, outermostNode, null);
            }
            final SqlValidatorNamespace namespace = getNamespace(outermostNode);
            if (namespace == null) {
                throw Util.newInternal("Not a query: " + outermostNode);
            }
            return namespace.lookupHints(pp);
        }
        finally {
            outermostNode = null;
        }
    }

    /**
     * Looks up the fully qualified name for a {@link SqlIdentifier} at a given
     * Parser Position in a parsed expression tree
     * Note: call this only after {@link #validate} has been called.
     *
     * @param topNode top of expression tree in which to lookup the qualfied
     * name for the SqlIdentifier
     * @param pp indicates the position of the {@link SqlIdentifier} in the sql
     * statement we want to get the qualified name for
     *
     * @return a string of the fully qualified name of the {@link SqlIdentifier}
     * if the Parser position represents a valid {@link SqlIdentifier}.  Else
     * return an empty string
     *
     */
    public Moniker lookupQualifiedName(SqlNode topNode, SqlParserPos pp)
    {
        SqlIdentifier id = null;
        Object o = sqlids.get(pp.toString());
        if (o != null) {
            id = (SqlIdentifier) o;
        }
        SqlValidatorScope scope = null;
        o = idscopes.get(pp.toString());
        if (o != null) {
            scope = (SqlValidatorScope) o;
        }
        if (id != null && scope != null) {
            return new IdentifierMoniker(scope.fullyQualify(id));
        } else {
            return null;
        }
    }

    /**
     * Looks up completion hints for a syntatically correct select SQL
     * that has been parsed into an expression tree
     *
     * @param select the Select node of the parsed expression tree
     * @param pp indicates the position in the sql statement we want to get
     * completion hints for
     *
     * @return an array list of strings (sql identifiers) that can fill in at
     * the indicated position
     *
     */
    String[] lookupSelectHints(SqlSelect select, SqlParserPos pp)
    {
        SqlIdentifier dummyId =  (SqlIdentifier) sqlids.get(pp.toString());
        SqlValidatorScope dummyScope = (SqlValidatorScope)
            idscopes.get(pp.toString());
        if (dummyId == null || dummyScope == null) {
            SqlNode fromNode = select.getFrom();
            final SqlValidatorScope fromScope = getFromScope(select);
            return lookupFromHints(fromNode, fromScope, pp);
        } else {
            return dummyId.findValidOptions(this, dummyScope);
        }
    }

    private String[] lookupFromHints(SqlNode node,
        SqlValidatorScope scope, SqlParserPos pp)
    {
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns instanceof IdentifierNamespace) {
            IdentifierNamespace idns = (IdentifierNamespace)ns;
            if (pp.toString().equals(
                    idns.getId().getParserPosition().toString()))
            {
                return catalogReader.getAllSchemaObjectNames(
                    idns.getId().names);
            }
        }
        switch (node.getKind().getOrdinal()) {
        case SqlKind.JoinORDINAL:
            return lookupJoinHints((SqlJoin) node, scope, pp);
        default:
            return getNamespace(node).lookupHints(pp);
        }
    }

    private String[] lookupJoinHints(SqlJoin join, SqlValidatorScope scope, SqlParserPos pp)
    {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        String [] result = lookupFromHints(left, scope, pp);
        if (result.length > 0) {
            return result;
        }
        result = lookupFromHints(right, scope, pp);
        if (result.length > 0) {
            return result;
        }
        SqlJoinOperator.ConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = (SqlValidatorScope) scopes.get(join);
        switch (conditionType.getOrdinal()) {
        case SqlJoinOperator.ConditionType.On_ORDINAL:
            return condition.findValidOptions(this, joinScope, pp);
        default:
        // not supporting lookup hints for other types such as 'Using' yet
            return Util.emptyStringArray;
        }
    }

    public SqlNode validateParameterizedExpression(
        SqlNode topNode,
        final Map nameToTypeMap)
    {
        SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
        return validateScopedExpression(topNode, scope);
    }

    private SqlNode validateScopedExpression(SqlNode topNode, SqlValidatorScope scope)
    {
        Util.pre(outermostNode == null, "outermostNode == null");
        try {
            outermostNode = performUnconditionalRewrites(topNode);
            if (tracer.isLoggable(Level.FINER)) {
                tracer.finer("After unconditional rewrite: " +
                    outermostNode.toString());
            }
            if (outermostNode.getKind().isA(SqlKind.TopLevel)) {
                registerQuery(scope, null, outermostNode, null);
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
            SqlNode returnNode = outermostNode;
            return returnNode;
        } finally {
            outermostNode = null;
        }
    }

    public void validateQuery(SqlNode node)
    {
        final SqlValidatorNamespace namespace = getNamespace(node);
        if (namespace == null) {
            throw Util.newInternal("Not a query: " + node);
        }
        validateNamespace(namespace);
    }

    /**
     * Validates a namespace.
     */
    protected void validateNamespace(final SqlValidatorNamespace namespace)
    {
        namespace.validate();
    }

    public SqlValidatorScope getWhereScope(SqlSelect select)
    {
        return (SqlValidatorScope) whereScopes.get(select);
    }

    public SqlValidatorScope getSelectScope(SqlSelect select)
    {
        return (SqlValidatorScope) selectScopes.get(select);
    }

    public SqlValidatorScope getHavingScope(SqlSelect select)
    {
        // Yes, it's the same as getSelectScope
        return (SqlValidatorScope) selectScopes.get(select);
    }

    public SqlValidatorScope getGroupScope(SqlSelect select)
    {
        // Yes, it's the same as getWhereScope
        return (SqlValidatorScope) whereScopes.get(select);
    }

    public SqlValidatorScope getFromScope(SqlSelect select)
    {
        return (SqlValidatorScope) scopes.get(select);
    }

    public SqlValidatorScope getOrderScope(SqlSelect select)
    {
        return (SqlValidatorScope) orderScopes.get(select);
    }

    public SqlValidatorScope getJoinScope(SqlNode node) {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            return getJoinScope(((SqlCall) node).operands[0]);
        default:
            return (SqlValidatorScope) scopes.get(node);
        }
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
            return (SqlValidatorScope) whereScopes.get(select);
        case SqlSelect.HAVING_OPERAND:
        case SqlSelect.SELECT_OPERAND:
            return (SqlValidatorScope) selectScopes.get(select);
        case SqlSelect.ORDER_OPERAND:
            return (SqlValidatorScope) orderScopes.get(select);
        default:
            throw Util.newInternal("Unexpected operandType " + operandType);
        }
    }

    public SqlValidatorNamespace getNamespace(SqlNode node) {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
        case SqlKind.OverORDINAL:
            return getNamespace(((SqlCall) node).operands[0]);
        default:
            return (SqlValidatorNamespace) namespaces.get(node);
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
    private SqlNode performUnconditionalRewrites(SqlNode node)
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
            node = call.getOperator().rewriteCall(call);
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
        if (node.isA(SqlKind.SetQuery) || node.isA(SqlKind.Values)) {
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
            SqlSelect wrapperNode =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, node, null, null, null, null, null,
                    SqlParserPos.ZERO);
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
            selectList.add(new SqlIdentifier("*", null));
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
            selectList.add(new SqlIdentifier("*", null));
            SqlSelect wrapperNode =
                SqlStdOperatorTable.selectOperator.createCall(null,
                    selectList, call.getOperands()[0], null, null, null, null,
                    null, SqlParserPos.ZERO);
            return wrapperNode;
        } else if (node.isA(SqlKind.Insert)) {
            SqlInsert call = (SqlInsert) node;
            call.setOperand(
                SqlInsert.SOURCE_SELECT_OPERAND,
                call.getSource());
        } else if (node.isA(SqlKind.Delete)) {
            SqlDelete call = (SqlDelete) node;
            final SqlNodeList selectList =
                new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(new SqlIdentifier("*", null));
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
            selectList.add(new SqlIdentifier("*", null));
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
        }
        return node;
    }

    RelDataType getTableConstructorRowType(
        SqlCall values,
        SqlValidatorScope scope)
    {
        assert values.getOperands().length >= 1;
        final SqlNode operand = values.getOperands()[0];
        assert(operand.isA(SqlKind.Row));
        SqlCall rowConstructor = (SqlCall) operand;

        // REVIEW jvs 10-Sept-2003: This assumes we can get everything we need
        // from the first row.  Once we support single-row queries as rows,
        // need to infer aliases from there.
        SqlNode [] operands = rowConstructor.getOperands();
        final ArrayList aliasList = new ArrayList();
        final ArrayList typeList = new ArrayList();
        for (int i = 0; i < operands.length; ++i) {
            final String alias = deriveAlias(operands[i], i);
            aliasList.add(alias);
            final RelDataType type = deriveType(scope, operands[i]);
            typeList.add(type);
        }
        final RelDataType[] types = (RelDataType [])
            typeList.toArray(new RelDataType[typeList.size()]);
        final String[] aliases = (String[])
            aliasList.toArray(new String[aliasList.size()]);
        return typeFactory.createStructType(types, aliases);
    }

    public RelDataType getValidatedNodeType(SqlNode node)
    {
        final RelDataType type = (RelDataType) nodeToTypeMap.get(node);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns != null) {
            return ns.getRowType();
        }
        throw Util.needToImplement(node);
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
        SqlNode operand)
    {
        Util.pre(scope != null, "scope != null");
        Util.pre(operand != null, "operand != null");

        // if we already know the type, no need to re-derive
        RelDataType type = (RelDataType) nodeToTypeMap.get(operand);
        if (type == null) {
            type = deriveTypeImpl(scope, operand);
            Util.permAssert(type != null,
                "SqlValidator.deriveTypeInternal returned null");
            setValidatedNodeTypeImpl(operand, type);
        }
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
        // REVIEW jvs 2-Dec-2004:  this method has outgrown its pants

        RelDataType type;
        if (operand instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier) operand;

            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            SqlCall call = SqlUtil.makeCall(opTab, id);
            if (call != null) {
                return call.getOperator().getType(this, scope, call);
            }

            type = null;
            for (int i = 0; i < id.names.length; i++) {
                String name = id.names[i];
                if (i == 0) {
                    // REVIEW jvs 23-Dec-2003:  what if a table and column have
                    // the same name?
                    final SqlValidatorNamespace resolvedNs =
                        scope.resolve(name, null, null);
                    if (resolvedNs != null) {
                        // There's a table with the name we seek.
                        type = resolvedNs.getRowType();
                    } else if (scope instanceof ListScope) {
                        // See if there's a column with the name we seek in
                        // precisely one of the tables in this scope.
                        type = ((ListScope) scope).resolveColumn(name, id);
                    }
                    if (type == null) {
                        throw newValidationError(id,
                            EigenbaseResource.instance().newUnknownIdentifier(
                                name));
                    }
                } else {
                    RelDataType fieldType = SqlValidatorUtil.lookupField(type, name);
                    if (fieldType == null) {
                        throw newValidationError(
                            id,
                            EigenbaseResource.instance().newUnknownField(name));
                    }
                    type = fieldType;
                }
            }
            type = SqlTypeUtil.addCharsetAndCollation(type, getTypeFactory());
            return type;
        }

        if (operand instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) operand;
            validateLiteral(literal);
            return literal.createSqlType(typeFactory);
        }

        if (operand instanceof SqlIntervalQualifier) {
            return typeFactory.createSqlIntervalType(
                (SqlIntervalQualifier) operand);
        }

        if (operand instanceof SqlDynamicParam) {
            return unknownType;
        }

        // SqlDataTypeSpec may occur in an expression as the 2nd arg to the CAST
        // function.
        if (operand instanceof SqlDataTypeSpec) {
            SqlDataTypeSpec dataType = (SqlDataTypeSpec) operand;
            return dataType.deriveType(this);
        }

        if (operand instanceof SqlCall) {
            SqlCall call = (SqlCall) operand;
            checkForIllegalNull(call);
            SqlNode[] operands = call.getOperands();
            // special case for AS:  never try to derive type for alias
            if (operand.isA(SqlKind.As)) {
                RelDataType nodeType = deriveType(scope, operands[0]);
                setValidatedNodeTypeImpl(operands[0], nodeType);
                type = call.getOperator().getType(this, scope, call);
                return type;
            }

            if (call.isA(SqlKind.MultisetQueryConstructor)) {
                SqlSelect subSelect = (SqlSelect) call.operands[0];
                subSelect.validateExpr(this, scope);
                SqlValidatorNamespace ns = getNamespace(subSelect);
                assert(null!=ns.getRowType());
                return SqlTypeUtil.createMultisetType(
                    typeFactory, ns.getRowType(), false);
            }

            final SqlSyntax syntax = call.getOperator().getSyntax();
            switch (syntax.getOrdinal()) {
            case SqlSyntax.Prefix_ordinal:
            case SqlSyntax.Postfix_ordinal:
            case SqlSyntax.Binary_ordinal:
                // TODO: use this switch statement to resolve functions instead
                // of all of these 'if's.
            }

            if (call.getOperator() instanceof SqlCaseOperator) {
                return call.getOperator().getType(this, scope, call);
            }
            if (call.isA(SqlKind.Over)) {
                return call.getOperator().getType(this, scope, call);
            }

            SqlValidatorScope subScope = scope;
            if (scope instanceof AggregatingScope &&
                call.getOperator().isAggregator()) {
                subScope = ((AggregatingScope) scope).getScopeAboveAggregation();
            }
            if ((call.getOperator() instanceof SqlFunction)
                    || (call.getOperator() instanceof SqlSpecialOperator)
                    || (call.getOperator() instanceof SqlRowOperator))
            {
                SqlOperator operator = call.getOperator();

                RelDataType[] argTypes = new RelDataType[operands.length];
                for (int i = 0; i < operands.length; ++i) {
                    RelDataType nodeType = deriveType(subScope, operands[i]);
                    setValidatedNodeTypeImpl(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }

                if (!(operator instanceof SqlJdbcFunctionCall)
                        && operator instanceof SqlFunction)
                {
                    SqlFunction unresolvedFunction = (SqlFunction) operator;

                    SqlFunction function;
                    if (operands.length == 0 &&
                        syntax == SqlSyntax.FunctionId) {
                        // For example, "LOCALTIME()" is illegal. (It should be
                        // "LOCALTIME", which would have been handled as a
                        // SqlIdentifier.)
                        function = null;
                    } else {
                        function = SqlUtil.lookupRoutine(
                            opTab,
                            unresolvedFunction.getNameAsId(),
                            argTypes,
                            unresolvedFunction.getFunctionType());
                        if (unresolvedFunction.getFunctionType() ==
                            SqlFunctionCategory.UserDefinedConstructor)
                        {
                            return deriveConstructorType(
                                scope,
                                call,
                                unresolvedFunction,
                                function,
                                argTypes);
                        }
                    }
                    if (function == null) {
                        handleUnresolvedFunction(
                            call,
                            unresolvedFunction,
                            argTypes);
                    }
                    // REVIEW jvs 25-Mar-2005:  This is, in a sense, expanding
                    // identifiers, but we ignore shouldExpandIdentifiers()
                    // because otherwise later validation code will
                    // choke on the unresolved function.
                    call.setOperator(function);
                    operator = function;
                }
                return operator.getType(this, scope, call);
            }
            if (call.getOperator() instanceof SqlBinaryOperator
                || call.getOperator() instanceof SqlPostfixOperator
                || call.getOperator() instanceof SqlPrefixOperator)
            {
                RelDataType[] argTypes = new RelDataType[operands.length];
                for (int i = 0; i < operands.length; ++i) {
                    RelDataType nodeType = deriveType(scope, operands[i]);
                    setValidatedNodeTypeImpl(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }
                type = call.getOperator().getType(this, scope, call);

                // Validate and determine coercibility and resulting collation
                // name of binary operator if needed.
                if (call.getOperator() instanceof SqlBinaryOperator) {
                    RelDataType operandType1 =
                        getValidatedNodeType(call.operands[0]);
                    RelDataType operandType2 =
                        getValidatedNodeType(call.operands[1]);
                    if (SqlTypeUtil.inCharFamily(operandType1)
                        && SqlTypeUtil.inCharFamily(operandType2))
                    {
                        Charset cs1 = operandType1.getCharset();
                        Charset cs2 = operandType2.getCharset();
                        assert ((null != cs1) && (null != cs2)) :
                            "An implicit or explicit charset should have been set";
                        if (!cs1.equals(cs2)) {
                            throw EigenbaseResource.instance()
                                .newIncompatibleCharset(
                                    call.getOperator().getName(),
                                    cs1.name(),
                                    cs2.name());
                        }

                        SqlCollation col1 = operandType1.getCollation();
                        SqlCollation col2 = operandType2.getCollation();
                        assert ((null != col1) && (null != col2)) :
                            "An implicit or explicit collation should have been set";

                        //validation will occur inside getCoercibilityDyadicOperator...
                        SqlCollation resultCol =
                            SqlCollation.getCoercibilityDyadicOperator(col1,
                                col2);

                        if (SqlTypeUtil.inCharFamily(type)) {
                            type =
                                typeFactory.createTypeWithCharsetAndCollation(
                                    type,
                                    type.getCharset(),
                                    resultCol);
                        }
                    }

                } else if (SqlTypeUtil.inCharFamily(type)) {
                    // Determine coercibility and resulting collation name of
                    // unary operator if needed.
                    RelDataType operandType =
                        getValidatedNodeType(call.operands[0]);
                    if (null == operandType) {
                        throw Util.newInternal(
                            "operand's type should have been derived");
                    }
                    if (SqlTypeUtil.inCharFamily(operandType)) {
                        SqlCollation collation = operandType.getCollation();
                        assert (null != collation) : "An implicit or explicit collation should have been set";
                        type =
                            typeFactory.createTypeWithCharsetAndCollation(
                                type,
                                type.getCharset(),
                                new SqlCollation(
                                    collation.getCollationName(),
                                    collation.getCoercibility()));
                    }
                }
                SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
                return type;
            } else {
                return getValidatedNodeType(operand);
            }
        }
        // Operand is of a type that we can't derive a type for.
        // If the operand is of a peculiar type, such as a SqlNodeList, then
        // you should override the operator's validateCall() method so that
        // it doesn't try to validate that operand as an expression.
        throw Util.needToImplement(operand);
    }

    private RelDataType deriveConstructorType(
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
                EigenbaseResource.instance().newUnknownDatatypeName(
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
            RelDataType returnType = resolvedConstructor.getType(
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

    private void handleUnresolvedFunction(
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
                    fun.getOperandsCountDescriptor()
                    .getPossibleNumOfOperands().get(0);
                throw newValidationError(call,
                    EigenbaseResource.instance().newInvalidArgCount(
                        call.getOperator().getName(),
                        expectedArgCount));
            }
        }

        AssignableOperandTypeChecker typeChecking =
            new AssignableOperandTypeChecker(argTypes);
        String signature =
            typeChecking.getAllowedSignatures(unresolvedFunction);
        throw newValidationError(
            call,
            EigenbaseResource.instance().newValidatorUnknownFunction(
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
        if (node instanceof SqlDynamicParam
            || SqlUtil.isNullLiteral(node, false)) {
            if (inferredType.equals(unknownType)) {
                throw newValidationError(node,
                    EigenbaseResource.instance().newNullIllegal());
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
                if (inferredType.getFieldList().size() != nodeList.size()) {
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
            //REVIEW wael: can this be done in a paramtypeinference strategy object?
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
            SqlOperandTypeInference paramTypeInference =
                call.getOperator().getUnknownParamTypeInference();
            SqlNode [] operands = call.getOperands();
            RelDataType [] operandTypes = new RelDataType[operands.length];
            if (paramTypeInference == null) {
                // TODO:  eventually should assert(paramTypeInference != null)
                // instead; for now just eat it
                Arrays.fill(operandTypes, unknownType);
            } else {
                paramTypeInference.inferOperandTypes(this, scope, call,
                    inferredType, operandTypes);
            }
            for (int i = 0; i < operands.length; ++i) {
                inferUnknownTypes(operandTypes[i], scope, operands[i]);
            }

            checkForIllegalNull(call);
        }
    }

    // TODO jvs 17-Jan-2005:  this should probably become part
    // the operand-checking interface instead for proper extensibility
    protected void checkForIllegalNull(SqlCall call)
    {
        if (call.isA(SqlKind.Case)
            || call.isA(SqlKind.Cast)
            || call.isA(SqlKind.Row)
            || call.isA(SqlKind.Select)) {
            return;
        }
        for (int i = 0; i < call.operands.length; i++) {
            final SqlNode operand = call.operands[i];
            if (SqlUtil.isNullLiteral(operand, false)) {
                throw newValidationError(operand,
                    EigenbaseResource.instance().newNullIllegal());
            }
        }
    }

    /**
     * Adds an expression to a select list, ensuring that its alias does not
     * clash with any existing expressions on the list.
     */
    private void addToSelectList(
        ArrayList list,
        List aliases,
        List types,
        SqlNode exp,
        SqlValidatorScope scope)
    {
        String alias = deriveAlias(exp, -1);
        if (alias == null) {
            alias = "EXPR$";
        }
        if (aliases.contains(alias)) {
            String aliasBase = alias;
            for (int j = 0;; j++) {
                alias = aliasBase + j;
                if (!aliases.contains(alias)) {
                    exp = SqlValidatorUtil.addAlias(exp, alias);
                    break;
                }
            }
        }
        aliases.add(alias);
        types.add(deriveType(scope, exp));
        list.add(exp);
    }

    public String deriveAlias(
        SqlNode node,
        int ordinal)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            // E.g. "1 + 2 as foo" --> "foo"
            return ((SqlCall) node).getOperands()[1].toString();

        case SqlKind.IdentifierORDINAL:
            // E.g. "foo.bar" --> "bar"
            final String [] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];

        default:
            if (ordinal < 0) {
                return null;
            } else {
                return SqlUtil.deriveAliasFromOrdinal(ordinal);
            }
        }
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
        SqlValidatorNamespace ns)
    {
        namespaces.put(ns.getNode(), ns);
        if (usingScope != null) {
            usingScope.addChild(ns, alias);
        }
    }

    private SqlNode registerFrom(
        SqlValidatorScope parentScope,
        SqlValidatorScope usingScope,
        SqlNode node,
        String alias)
    {
        SqlNode newNode;
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            final SqlCall sqlCall = (SqlCall) node;
            if (alias == null) {
                alias = sqlCall.operands[1].toString();
            }
            final SqlNode expr = sqlCall.operands[0];
            final SqlNode newExpr = registerFrom(parentScope, usingScope, expr, alias);
            if (newExpr != expr) {
                sqlCall.setOperand(0, newExpr);
            }
            return node;

        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) node;
            final JoinScope joinScope = new JoinScope(parentScope, usingScope, join);
            scopes.put(join, joinScope);
            final SqlNode left = join.getLeft();
            final SqlNode newLeft = registerFrom(parentScope, joinScope, left,
                null);
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
                right, null);
            if (newRight != right) {
                join.setOperand(SqlJoin.RIGHT_OPERAND, newRight);
            }
            final JoinNamespace joinNamespace = new JoinNamespace(this, join);
            registerNamespace(null, null, joinNamespace);
            return join;

        case SqlKind.IdentifierORDINAL:
            newNode = node;
            if (alias == null) {
                alias = deriveAlias(node, -1);
                if (shouldExpandIdentifiers()) {
                    newNode = SqlValidatorUtil.addAlias(node, alias);
                }
            }
            final SqlIdentifier id = (SqlIdentifier) node;
            final IdentifierNamespace newNs = new IdentifierNamespace(this, id);
            registerNamespace(usingScope, alias, newNs);
            return newNode;

        case SqlKind.LateralORDINAL:
            return registerFrom(
                    parentScope,
                    usingScope,
                    ((SqlCall) node).operands[0],
                    alias);

        case SqlKind.SelectORDINAL:
        case SqlKind.UnionORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.ValuesORDINAL:
        case SqlKind.UnnestORDINAL:
            newNode = node;
            if (alias == null) {
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
            }
            registerQuery(parentScope, usingScope, node, alias);
            return newNode;

        case SqlKind.OverORDINAL:
            if (!shouldAllowOverRelation()) {
                throw node.getKind().unexpected();
            }
            SqlCall call = (SqlCall) node;
            final SqlNode operand = call.operands[0];
            final SqlNode newOperand =
                registerFrom(parentScope, usingScope, operand, alias);
            if (newOperand != operand) {
                call.setOperand(0, newOperand);
            }
            return call;

        default:
            throw node.getKind().unexpected();
        }
    }

    protected boolean shouldAllowOverRelation()
    {
        return false;
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
        String alias)
    {
        Util.pre(usingScope == null || alias != null,
            "usingScope == null || alias != null");

        SqlCall call;
        SqlNode [] operands;
        switch (node.getKind().getOrdinal()) {
        case SqlKind.SelectORDINAL:
            final SqlSelect select = (SqlSelect) node;
            final SelectNamespace selectNs = new SelectNamespace(this, select);
            registerNamespace(usingScope, alias, selectNs);
            SelectScope selectScope = new SelectScope(parentScope, select);
            scopes.put(select, selectScope);
            // Register the subqueries in the FROM clause first.
            final SqlNode from = select.getFrom();
            whereScopes.put(select, selectScope);
            registerSubqueries(
                selectScope,
                select.getWhere());
            // Register FROM with the inherited scope 'parentScope', not
            // 'selectScope', otherwise tables in the FROM clause would be
            // able to see each other.
            final SqlNode newFrom = registerFrom(
                parentScope,
                selectScope,
                from,
                null);
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
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final SetopNamespace setopNamespace =
                new SetopNamespace(this, (SqlCall) node);
            registerNamespace(usingScope, alias, setopNamespace);
            call = (SqlCall) node;
            // A setop is in the same scope as its parent.
            scopes.put(call, parentScope);
            registerQuery(parentScope, null, call.operands[0], null);
            registerQuery(parentScope, null, call.operands[1], null);
            break;

        case SqlKind.ValuesORDINAL:
            final TableConstructorNamespace tableConstructorNamespace =
                new TableConstructorNamespace(this, node, parentScope);
            registerNamespace(usingScope, alias, tableConstructorNamespace);
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
            IdentifierNamespace insertScope =
                new IdentifierNamespace(this,
                    insertCall.getTargetTable());
            registerNamespace(usingScope, null, insertScope);
            registerQuery(
                parentScope,
                usingScope,
                insertCall.getSourceSelect(),
                null);
            break;

        case SqlKind.DeleteORDINAL:
            SqlDelete deleteCall = (SqlDelete) node;
            registerQuery(
                parentScope,
                usingScope,
                deleteCall.getSourceSelect(),
                null);
            break;

        case SqlKind.UpdateORDINAL:
            SqlUpdate updateCall = (SqlUpdate) node;
            registerQuery(
                parentScope,
                usingScope, updateCall.getSourceSelect(),
                null);
            break;

        case SqlKind.UnnestORDINAL:
            call = (SqlCall) node;
            final UnnestNamespace unnestNamespace =
                new UnnestNamespace(this, node, usingScope);
            registerNamespace(usingScope, alias, unnestNamespace);
            registerSubqueries(usingScope, call.operands[0]);
            break;

        case SqlKind.MultisetValueConstructorORDINAL:
        case SqlKind.MultisetQueryConstructorORDINAL:
            call = (SqlCall) node;
            CollectScope cs =
                new CollectScope(parentScope, usingScope, call);
             final CollectNamespace ttableConstructorNamespace =
                new CollectNamespace(node, cs);
            final String alias2 = deriveAlias(node, nextGeneratedId++);
            registerNamespace(cs,  alias2, ttableConstructorNamespace);
            operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                registerSubqueries(cs, operand);
            }
            break;

        default:
            throw node.getKind().unexpected();
        }
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
            registerQuery(parentScope, null, node, null);
        } else if (node.isA(SqlKind.MultisetValueConstructor) ||
                   node.isA(SqlKind.MultisetQueryConstructor)) {
                registerQuery(parentScope, null, node, null);
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
                    EigenbaseResource.instance().newBinaryLiteralOdd());
            }
            break;
        case SqlTypeName.IntervalYearMonth_ordinal:
        case SqlTypeName.IntervalDayTime_ordinal:
            if (literal instanceof SqlIntervalLiteral) {
                SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue)
                        ((SqlIntervalLiteral) literal).getValue();
                int[] values = SqlParserUtil.parseIntervalValue(interval);
                if (values == null) {
                    throw newValidationError(literal,
                            EigenbaseResource.instance().newUnsupportedIntervalLiteral
                            (interval.toString(), "INTERVAL " +
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
//        Namespace leftNs = getNamespace(left);
//        final RelDataType leftRowType = leftNs.getRowType();
//        Namespace rightNs = getNamespace(right);
//        final RelDataType rightRowType = rightNs.getRowType();
        SqlNode condition = join.getCondition();
        boolean natural = join.isNatural();
        SqlJoinOperator.JoinType joinType = join.getJoinType();
        SqlJoinOperator.ConditionType conditionType =
            join.getConditionType();
        validateFrom(left, unknownType, scope);
        validateFrom(right, unknownType, scope);

        // Validate condition.
        final SqlValidatorScope joinScope = (SqlValidatorScope) scopes.get(join);
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
                EigenbaseResource.instance().
                newNaturalDisallowsOnOrUsing());
        }
        // Which join types require/allow a ON/USING condition, or allow
        // a NATURAL keyword?
        switch (joinType.getOrdinal()) {
        case SqlJoinOperator.JoinType.Inner_ORDINAL:
        case SqlJoinOperator.JoinType.Left_ORDINAL:
        case SqlJoinOperator.JoinType.Right_ORDINAL:
        case SqlJoinOperator.JoinType.Full_ORDINAL:
            if (condition == null && !natural) {
                throw newValidationError(join,
                    EigenbaseResource.instance()
                    .newJoinRequiresCondition());
            }
            break;
        case SqlJoinOperator.JoinType.Comma_ORDINAL:
        case SqlJoinOperator.JoinType.Cross_ORDINAL:
            if (condition != null) {
                throw newValidationError(condition,
                    EigenbaseResource.instance()
                    .newCrossJoinDisallowsCondition());
            }
            if (natural) {
                throw newValidationError(join,
                    EigenbaseResource.instance()
                    .newCrossJoinDisallowsCondition());
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
            EigenbaseResource.instance().newColumnNotFound(
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
        if (windowList == null) {
            return;
        }
        // todo: validate window clause
        // 1. ensure window names are simple
        // 2. ensure they are unique within this scope
        // 3. validate window specifications
        //   3a. a window can refer to a window in an outer scope, or a
        //       previous window in this scope
        // validateExpression(windowList);
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
            if (select != outermostNode) {
                throw newValidationError(select,
                    EigenbaseResource.instance().newInvalidOrderByPos());
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
                EigenbaseResource.instance().newAggregateIllegalInWhere());
        }
        inferUnknownTypes(
            booleanType,
            whereScope,
            where);
        where.validate(this, whereScope);
        final RelDataType type = deriveType(whereScope, where);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(where,
                EigenbaseResource.instance().newWhereMustBeBoolean());
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
        having.validate(this, havingScope);
        inferUnknownTypes(
            booleanType,
            havingScope,
            having);
        final RelDataType type = deriveType(havingScope, having);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(having,
                EigenbaseResource.instance().newHavingMustBeBoolean());
        }
    }

    private RelDataType validateSelectList(final SqlNodeList selectItems,
        SqlSelect select, RelDataType targetRowType)
    {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        final SqlValidatorScope selectScope = getSelectScope(select);
        final ArrayList expandedSelectItems = new ArrayList();
        final ArrayList aliasList = new ArrayList();
        final ArrayList typeList = new ArrayList();
        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode selectItem = selectItems.get(i);
            if (selectScope instanceof AggregatingScope) {
                AggregatingScope aggScope =
                    (AggregatingScope) selectScope;
                boolean matches = aggScope.checkAggregateExpr(selectItem);
                Util.discard(matches);
            }
            expandSelectItem(selectItem, select, expandedSelectItems,
                aliasList, typeList);
        }

        SqlNodeList newSelectList =
            new SqlNodeList(expandedSelectItems, SqlParserPos.ZERO);
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
        final RelDataType[] types = (RelDataType [])
            typeList.toArray(new RelDataType[typeList.size()]);
        final String[] aliases = (String[])
            aliasList.toArray(new String[aliasList.size()]);
        return typeFactory.createStructType(types, aliases);
    }


    private RelDataType createTargetRowType(
        RelDataType baseRowType,
        SqlNodeList targetColumnList,
        boolean append)
    {
        RelDataTypeField [] targetFields = baseRowType.getFields();
        int targetColumnCount = targetColumnList.size();
        if (append) {
            targetColumnCount += baseRowType.getFieldList().size();
        }
        RelDataType [] types = new RelDataType[targetColumnCount];
        String [] fieldNames = new String[targetColumnCount];
        int iTarget = 0;
        if (append) {
            iTarget += baseRowType.getFieldList().size();
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
                    EigenbaseResource.instance().newUnknownTargetColumn(
                        id.getSimple()));
            }
            fieldNames[iTarget] = targetFields[iColumn].getName();
            types[iTarget] = targetFields[iColumn].getType();
            ++iTarget;
        }
        return typeFactory.createStructType(types, fieldNames);
    }

    /**
     * Validates an INSERT statement.
     */
    public void validateInsert(SqlInsert call)
    {
        IdentifierNamespace targetNamespace =
            (IdentifierNamespace) getNamespace(call.getTargetTable());
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        RelDataType targetRowType = table.getRowType();
        if (call.getTargetColumnList() != null) {
            targetRowType =
                createTargetRowType(
                    targetRowType,
                    call.getTargetColumnList(),
                    false);
        }

        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect, targetRowType);
        RelDataType sourceRowType = getNamespace(sqlSelect).getRowType();

        if (targetRowType.getFieldList().size()
            != sourceRowType.getFieldList().size())
        {
            throw EigenbaseResource.instance().newUnmatchInsertColumn(
                new Integer(targetRowType.getFieldList().size()),
                new Integer(sourceRowType.getFieldList().size()));
        }

        // TODO:  validate updatability, type compatibility, etc.
    }

    /**
     * Validates a DELETE statement.
     */
    public void validateDelete(SqlDelete call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect, unknownType);

        // TODO:  validate updatability, etc.
    }

    /**
     * Validates an UPDATE statement.
     */
    public void validateUpdate(SqlUpdate call)
    {
        SqlSelect select = call.getSourceSelect();

        SqlIdentifier id = call.getTargetTable();
        final String name = id.names[id.names.length - 1];
        final ListScope fromScope = (ListScope) getWhereScope(select);
        IdentifierNamespace targetNamespace =
            (IdentifierNamespace) fromScope.getChild(name);
        validateNamespace(targetNamespace);
        SqlValidatorTable table = targetNamespace.getTable();

        RelDataType targetRowType =
            createTargetRowType(
                table.getRowType(),
                call.getTargetColumnList(),
                true);

        validateSelect(select, targetRowType);

        // TODO:  validate updatability, type compatibility
    }

    /**
     * Validates a VALUES clause
     */
    private void validateValues(
        SqlCall node,
        RelDataType targetRowType,
        SqlValidatorScope scope)
    {
        assert node.isA(SqlKind.Values);

        final SqlNode [] operands = node.getOperands();
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            operand.validate(this, scope);
        }
        for (int i = 0; i < operands.length; i++) {
            if (!operands[i].isA(SqlKind.Row)) {
                throw Util.needToImplement(
                    "Values function where operands are scalars");
            }
            SqlCall rowConstructor = (SqlCall) operands[i];
            if (targetRowType.isStruct() &&
                rowConstructor.getOperands().length !=
                targetRowType.getFieldList().size()) {
                return;
            }

            inferUnknownTypes(
                targetRowType,
                scope,
                rowConstructor);
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
                        .newIncompatibleValueType());
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
                    SqlTypeUtil.getNullableBiggest(typeFactory, types);

                if (null == type) {
                    throw newValidationError(node,
                        EigenbaseResource.instance()
                        .newIncompatibleValueType());
                }
            }
        }
    }

    /**
     * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a
     * namespace. The returned namespace may represent a schema, table, column,
     * etc.
     *
     * @pre names.length > 0
     * @post return != null
     */
    protected SqlValidatorNamespace lookup(SqlValidatorScope scope, String[] names)
    {
        Util.pre(names.length > 0, "names.length > 0");
        SqlValidatorNamespace namespace = null;
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            if (i == 0) {
                namespace = scope.resolve(name, null, null);
            } else {
                namespace = namespace.lookupChild(name, null, null);
            }
        }
        Util.permAssert(namespace != null, "post: namespace != null");
        return namespace;
    }

    /**
     * Validates a data type expression.
     */
    public void validateDataType(SqlDataTypeSpec dataType)
    {
    }

    /**
     * Validates a dynamic parameter.
     */
    public void validateDynamicParam(SqlDynamicParam dynamicParam)
    {
    }

    public EigenbaseException newValidationError(
        SqlNode node,
        SqlValidatorException e)
    {
        Util.pre(node != null, "node != null");
        final SqlParserPos pos = node.getParserPosition();
        int line = pos.getLineNum();
        int col = pos.getColumnNum();
        int endLine = pos.getEndLineNum();
        int endCol = pos.getEndColumnNum();
        EigenbaseContextException contextExcn =
            line == endLine && col == endCol ?
            EigenbaseResource.instance().newValidatorContextPoint(
                new Integer(line),
                new Integer(col),
                e) :
            EigenbaseResource.instance().newValidatorContext(
                new Integer(line),
                new Integer(col),
                new Integer(endLine),
                new Integer(endCol),
                e);
        contextExcn.setPosition(line, col, endLine, endCol);
        return contextExcn;
    }

    private SqlWindow getWindowByName(
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
                EigenbaseResource.instance().newWindowNotFound(
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
                    EigenbaseResource.instance().newWindowNotFound(refName));
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

    SqlValidatorNamespace lookupFieldNamespace(RelDataType rowType, String name,
        SqlValidatorScope[] ancestorOut, int[] offsetOut)
    {
        final RelDataType dataType = SqlValidatorUtil.lookupField(rowType, name);
        return new SqlValidatorNamespace() {
            public SqlValidatorTable getTable()
            {
                return null;
            }

            public RelDataType getRowType()
            {
                return dataType;
            }

            public void validate()
            {
            }

            public String[] lookupHints(SqlParserPos pp)
            {
                return Util.emptyStringArray;
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
        };
    }

    /**
     * Validates the right-hand side of an OVER expression. It might be
     * either an {@link SqlIdentifier identifier} referencing a window, or
     * an {@link SqlWindow inline window specification}.
     */
    public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope) {
        switch (windowOrId.getKind().getOrdinal()) {
        case SqlKind.IdentifierORDINAL:
            final SqlWindow window = getWindowByName((SqlIdentifier) windowOrId, scope);
            break;
        case SqlKind.WindowORDINAL:
            windowOrId.validate(this, scope);
            break;
        default:
            throw windowOrId.getKind().unexpected();
        }
    }

    /**
     * Combines windows
     */

    /**
     * Validates a call to an operator.
     */
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

}

// End SqlValidator.java

