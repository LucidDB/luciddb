/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.SqlBasicVisitor;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.Util;
import org.eigenbase.trace.EigenbaseTrace;
import net.sf.farrago.util.FarragoException;

import java.nio.charset.Charset;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;


/**
 * <code>SqlValidator</code> validates the parse tree of a SQL statement, and
 * provides semantic information about the parse tree.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 25, 2003
 */
public class SqlValidator
{
    //~ Static fields/initializers --------------------------------------------

    public static final RelDataType [] emptyTypes = new RelDataType[0];
    public static final String [] emptyStrings = new String[0];

    //~ Instance fields -------------------------------------------------------

    public final SqlOperatorTable opTab;
    private final CatalogReader catalogReader;

    /**
     * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
     * objects at these positions
     */
    protected final HashMap sqlids = new HashMap();

    /**
     * Maps ParsePosition strings to the {@link Scope} scope
     * objects at these positions
     */
    protected final HashMap idscopes = new HashMap();

    /**
     * Maps {@link SqlNode query node} objects to the {@link Scope} scope
     * created from them}.
     */
    protected final HashMap scopes = new HashMap();
    /**
     * Maps a {@link SqlSelect} node to the scope used by its WHERE and
     * HAVING clauses.
     */
    private final HashMap whereScopes = new HashMap();
    /**
     * Maps a {@link SqlSelect} node to the scope used by its SELECT and
     * HAVING clauses.
     */
    private final HashMap selectScopes = new HashMap();
    /**
     * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
     */
    private final HashMap orderScopes = new HashMap();
    /**
     * Maps a {@link SqlNode node} to the {@link Namespace} the namespace which
     * describes what columns they contain.
     */
    protected final HashMap namespaces = new HashMap();
    private SqlNode outermostNode;
    private int nextGeneratedId;
    public final RelDataTypeFactory typeFactory;
    public final RelDataType unknownType;
    public final RelDataType anyType;
    public final RelDataType booleanType;

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
    public SqlValidator(
        SqlOperatorTable opTab,
        CatalogReader catalogReader,
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
        anyType = typeFactory.createSqlType(SqlTypeName.Any);
        booleanType = typeFactory.createSqlType(SqlTypeName.Boolean);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the catalog reader used by this validator
     */
    public CatalogReader getCatalogReader()
    {
        return catalogReader;
    }
    
    /**
     * Returns a list of expressions, with every occurrence of "&#42;" or
     * "TABLE.&#42;" expanded.
     */
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
        final Scope scope = getScope(select, SqlSelect.WHERE_OPERAND);
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            if ((identifier.names.length == 1)
                    && identifier.names[0].equals("*")) {

                ArrayList tableNames = ((ListScope) scope).childrenNames;
                for (int i = 0; i < tableNames.size(); i++) {
                    String tableName = (String) tableNames.get(i);
                    final SqlNode from = getChild(select, tableName);
                    final Namespace fromNs = getNamespace(from);
                    assert fromNs != null;
                    final RelDataType rowType = fromNs.getRowType();
                    final RelDataTypeField [] fields = rowType.getFields();
                    for (int j = 0; j < fields.length; j++) {
                        RelDataTypeField field = fields[j];
                        String columnName = field.getName();

                        //todo: do real implicit collation here
                        final SqlNode exp =
                            new SqlIdentifier(new String [] {
                                    tableName, columnName
                                },
                                SqlParserPos.ZERO);
                        addToSelectList(selectItems, aliases, types, exp,
                            scope);
                    }
                }
                return true;
            } else if ((identifier.names.length == 2)
                    && identifier.names[1].equals("*")) {
                final String tableName = identifier.names[0];
                final SqlNode from = getChild(select, tableName);
                final Namespace fromNs = getNamespace(from);
                assert fromNs != null;
                final RelDataType rowType = fromNs.getRowType();
                final RelDataTypeField [] fields = rowType.getFields();
                for (int i = 0; i < fields.length; i++) {
                    RelDataTypeField field = fields[i];
                    String columnName = field.getName();

                    //todo: do real implicit collation here
                    final SqlIdentifier exp =
                        new SqlIdentifier(new String [] { tableName, columnName },
                            null);
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
        setValidatedNodeType(selectItem, type);
        types.add(type);
        return false;
    }

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
    public SqlNode validate(SqlNode topNode)
    {
        Scope scope = new EmptyScope();
        return validateScopedExpression(topNode, scope);
    }

    /**
     * Look up completion hints for a syntatically correct SQL that has been
     * parsed into an expression tree
     *
     * @param topNode top of expression tree in which to lookup completion hints
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
        Scope scope = new EmptyScope();
        try {
            outermostNode = performUnconditionalRewrites(topNode);
            if (outermostNode.getKind().isA(SqlKind.TopLevel)) {
                registerQuery(scope, null, outermostNode, null);
            }
            final Namespace namespace = getNamespace(outermostNode);
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
     * Look up completion hints for a syntatically correct select SQL
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
    private String[] lookupSelectHints(SqlSelect select, SqlParserPos pp)
    {
        SqlIdentifier dummyId = (SqlIdentifier)sqlids.get(pp.toString());
        Scope dummyScope = (Scope)idscopes.get(pp.toString());
        if (dummyId == null || dummyScope == null) {
            SqlNode fromNode = select.getFrom();
            return lookupFromHints(fromNode,
                getScope(select, SqlSelect.FROM_OPERAND), pp);
        } else {
            return dummyId.findValidOptions(this, dummyScope);
        }
    }



    private String[] lookupFromHints(SqlNode node,
        Scope scope, SqlParserPos pp)
    {
        final Namespace ns = getNamespace(node);
        if (ns instanceof IdentifierNamespace) {
            IdentifierNamespace idns = (IdentifierNamespace)ns;
            if (pp.toString().equals(idns.id.getParserPosition().toString())) {
                return catalogReader.getAllSchemaObjectNames(idns.id.names);
            }
        }
        switch (node.getKind().getOrdinal()) {
        case SqlKind.JoinORDINAL:
            return lookupJoinHints((SqlJoin) node, scope, pp);
        default:
            return getNamespace(node).lookupHints(pp);
        }
    }

    private String[] lookupJoinHints(SqlJoin join, Scope scope, SqlParserPos pp)
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
        final Scope joinScope = (Scope) scopes.get(join);
        switch (conditionType.ordinal) {
        case SqlJoinOperator.ConditionType.On_ORDINAL:
            return condition.findValidOptions(this, joinScope, pp);
        default:
        // not supporting lookup hints for other types such as 'Using' yet
            return Util.emptyStringArray;
        }
    }

    /**
     * Validates an expression tree. You can call this method multiple times,
     * but not reentrantly.
     *
     * @param topNode top of expression tree to be validated
     *
     * @param nameToTypeMap map of simple name (String) to RelDataType;
     * used to resolve SqlIdentifier references
     *
     * @return validated tree (possibly rewritten)
     *
     * @pre outermostNode == null
     */
    public SqlNode validateParameterizedExpression(
        SqlNode topNode,
        final Map nameToTypeMap)
    {
        Scope scope = new EmptyScope()
            {
                public SqlIdentifier fullyQualify(SqlIdentifier identifier)
                {
                    return identifier;
                }

                public Namespace resolve(
                    String name,
                    Scope[] ancestorOut,
                    int[] offsetOut)
                {
                    final RelDataType type =
                        (RelDataType) nameToTypeMap.get(name);
                    return new AbstractNamespace()
                        {
                            public SqlNode getNode()
                            {
                                return null;
                            }

                            public RelDataType validateImpl()
                            {
                                return type;
                            }

                            public RelDataType getRowType()
                            {
                                return type;
                            }
                        };
                }
            };
        return validateScopedExpression(topNode, scope);
    }

    private SqlNode validateScopedExpression(SqlNode topNode, Scope scope)
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

    /**
     * Checks that a query (<code>select</code> statement, or a set operation
     * <code>union</code>, <code>intersect</code>, <code>except</code>) is
     * valid.
     *
     * @throws RuntimeException if the query is not valid
     */
    public void validateQuery(SqlNode node)
    {
        final Namespace namespace = getNamespace(node);
        if (namespace == null) {
            throw Util.newInternal("Not a query: " + node);
        }
        validateNamespace(namespace);
    }

    /**
     * Validates a namespace.
     */
    protected void validateNamespace(final Namespace namespace)
    {
        namespace.validate();
    }

    /**
     * Returns the scope that expressions in the WHERE and GROUP BY clause of
     * this query should use. This scope consists of the tables in the FROM
     * clause, and the enclosing scope.
     */
    public ListScope getWhereScope(SqlSelect select)
    {
        return (ListScope) whereScopes.get(select);
    }

    /**
     * Returns the scope that expressions in the SELECT and HAVING clause of
     * this query should use. This scope consists of the FROM clause and the
     * enclosing scope. If the query is aggregating, only columns in the GROUP
     * BY clause may be used.
     */
    public Scope getOrderScope(SqlSelect select) {
        return (Scope) orderScopes.get(select);
    }

    public Scope getJoinScope(SqlNode node) {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            return getJoinScope(((SqlCall) node).operands[0]);
        default:
            return (Scope) scopes.get(node);
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
    public Scope getScope(SqlSelect select, int operandType)
    {
        switch (operandType) {
        case SqlSelect.FROM_OPERAND:
            return (Scope) scopes.get(select);
        case SqlSelect.WHERE_OPERAND:
        case SqlSelect.GROUP_OPERAND:
            return (Scope) whereScopes.get(select);
        case SqlSelect.HAVING_OPERAND:
        case SqlSelect.SELECT_OPERAND:
            return (Scope) selectScopes.get(select);
        case SqlSelect.ORDER_OPERAND:
            return (Scope) orderScopes.get(select);
        default:
            throw Util.newInternal("Unexpected operandType " + operandType);
        }
    }

    public Namespace getNamespace(SqlNode node) {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
        case SqlKind.OverORDINAL:
            return getNamespace(((SqlCall) node).operands[0]);
        default:
            return (Namespace) namespaces.get(node);
        }
    }

    private SqlNode getChild(
        SqlSelect select,
        String alias)
    {
        ListScope selectScope = getWhereScope(select);
        final Namespace childScope = selectScope.getChild(alias);
        return childScope.getNode();
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

            if (call.operator instanceof SqlFunction) {
                SqlFunction function = (SqlFunction) call.operator;
                if (function.getFunctionType() == null) {
                    // This function hasn't been resolved yet.  Perform
                    // a half-hearted resolution now in case it's a
                    // builtin function requiring special casing.  If it's
                    // not, we'll handle it later during overload
                    // resolution.
                    List overloads = opTab.lookupOperatorOverloads(
                        function.getNameAsId(),
                        SqlSyntax.Function);
                    if (overloads.size() == 1) {
                        call.operator = (SqlOperator) overloads.get(0);
                    }
                }
            }
            node = call.operator.rewriteCall(call);
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
                SqlStdOperatorTable.instance().selectOperator.createCall(null,
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
                SqlStdOperatorTable.instance().selectOperator.createCall(null,
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
                SqlStdOperatorTable.instance().selectOperator.createCall(null,
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
                sourceTable = addAlias(
                    sourceTable, call.getAlias().getSimple());
            }
            SqlSelect select =
                SqlStdOperatorTable.instance().selectOperator.createCall(null,
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
                String alias = deriveAliasFromOrdinal(ordinal);
                selectList.add(addAlias(exp, alias));
                ++ordinal;
            }
            SqlNode sourceTable = call.getTargetTable();
            if (call.getAlias() != null) {
                sourceTable = addAlias(
                    sourceTable, call.getAlias().getSimple());
            }
            SqlSelect select =
                SqlStdOperatorTable.instance().selectOperator.createCall(null,
                    selectList, sourceTable, call.getCondition(),
                    null, null, null, null, SqlParserPos.ZERO);
            call.setOperand(SqlUpdate.SOURCE_SELECT_OPERAND, select);
        }
        return node;
    }

    private RelDataType getTableConstructorRowType(
        SqlCall values,
        Scope scope)
    {
        assert values.getOperands().length >= 1;
        final SqlNode operand = values.getOperands()[0];
        assert(operand.isA(SqlKind.Row));
        SqlCall rowConstructor = (SqlCall) operand;

        // REVIEW jvs 10-Sept-2003: This assumes we can get everything we need
        // from the first row.  Once we support single-row queries as rows,
        // need to infer aliases from there.
        SqlNode [] operands = rowConstructor.getOperands();
        final ArrayList aliases = new ArrayList();
        final ArrayList types = new ArrayList();
        for (int i = 0; i < operands.length; ++i) {
            final String alias = deriveAlias(operands[i], i);
            aliases.add(alias);
            final RelDataType type = deriveType(scope, operands[i]);
            types.add(type);
        }
        return typeFactory.createStructType((RelDataType []) types.toArray(
                emptyTypes), (String []) aliases.toArray(emptyStrings));
    }

    /**
     * Get the type assigned to a node by validation.
     *
     * @param node the node of interest
     *
     * @return validated type, never null
     */
    public RelDataType getValidatedNodeType(SqlNode node)
    {
        final RelDataType type = (RelDataType) nodeToTypeMap.get(node);
        if (type != null) {
            return type;
        }
        final Namespace ns = getNamespace(node);
        if (ns != null) {
            return ns.getRowType();
        }
        throw Util.needToImplement(node);
    }

    public void setValidatedNodeType(
        SqlNode node,
        RelDataType type)
    {
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return;
        }
        nodeToTypeMap.put(node, type);
    }

    public static void checkCharsetAndCollateConsistentIfCharType(
        RelDataType type)
    {
        //(every charset must have a default collation)
        if (SqlTypeUtil.inCharFamily(type)) {
            Charset strCharset = type.getCharset();
            Charset colCharset = type.getCollation().getCharset();
            assert (null != strCharset);
            assert (null != colCharset);
            if (!strCharset.equals(colCharset)) {
                if (false) {
                    // todo: enable this checking when we have a charset to
                    //   collation mapping
                    throw new Error(type.toString() +
                        " was found to have charset '" + strCharset.name() +
                        "' and a mismatched collation charset '" +
                        colCharset.name() + "'");
                }
            }
        }
    }

    /**
     * Derives the type of a node in a given scope. If the type has already
     * been inferred, returns the previous type.
     *
     * @param scope  Syntactic scope
     * @param operand Parse tree node
     * @return Type; todo: when does this method return null, if ever?
     */
    public RelDataType deriveType(
        Scope scope,
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
            setValidatedNodeType(operand, type);
        }
        return type;
    }

    /**
     * Derives the type of a node.
     *
     * @post return != null
     */
    private RelDataType deriveTypeImpl(
        Scope scope,
        SqlNode operand)
    {
        // REVIEW jvs 2-Dec-2004:  this method has outgrown its pants

        RelDataType type;
        if (operand instanceof SqlSelect) {
            return getValidatedNodeType(operand);
        }

        if (operand instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier) operand;

            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            SqlCall call = makeCall(id);
            if (call != null) {
                return call.operator.getType(this, scope, call);
            }

            type = null;
            for (int i = 0; i < id.names.length; i++) {
                String name = id.names[i];
                if (i == 0) {
                    // REVIEW jvs 23-Dec-2003:  what if a table and column have
                    // the same name?
                    final Namespace resolvedNs =
                        scope.resolve(name, null, null);
                    if (resolvedNs != null) {
                        // There's a table with the name we seek.
                        type = resolvedNs.getRowType();
                    } else if (scope instanceof ListScope
                            && ((type =
                                ((ListScope) scope).resolveColumn(name, id)) != null)) {
                        // There's a column with the name we seek in precisely
                        // one of the tables.
                        ;
                    } else {
                        throw newValidationError(id,
                            EigenbaseResource.instance().newUnknownIdentifier(
                                name));
                    }
                } else {
                    RelDataType fieldType = lookupField(type, name);
                    if (fieldType == null) {
                        throw newValidationError(
                            id,
                            EigenbaseResource.instance().newUnknownField(name));
                    }
                    type = fieldType;
                }
            }
            if (SqlTypeUtil.inCharFamily(type)) {
                Charset charset =
                    (type.getCharset() == null) ? Util.getDefaultCharset()
                    : type.getCharset();
                SqlCollation collation =
                    (type.getCollation() == null)
                    ? new SqlCollation(SqlCollation.Coercibility.Implicit)
                    : type.getCollation();

                // todo: should get the implicit collation from repository
                //   instead of null
                type =
                    typeFactory.createTypeWithCharsetAndCollation(type,
                        charset, collation);
                checkCharsetAndCollateConsistentIfCharType(type);
            }
            return type;
        }

        if (operand instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) operand;
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
                setValidatedNodeType(operands[0], nodeType);
                type = call.operator.getType(this, scope, call);
                return type;
            }

            if (call.isA(SqlKind.MultisetQueryConstructor)) {
                SqlSelect subSelect = (SqlSelect) call.operands[0];
                subSelect.validateExpr(this, scope);
                Namespace ns = getNamespace(subSelect);
                assert(null!=ns.getRowType());
                return SqlTypeUtil.createMultisetType(
                    typeFactory, ns.getRowType(), false);
            }

            final SqlSyntax syntax = call.operator.getSyntax();
            switch (syntax.ordinal) {
            case SqlSyntax.Prefix_ordinal:
            case SqlSyntax.Postfix_ordinal:
            case SqlSyntax.Binary_ordinal:
                // TODO: use this switch statement to resolve functions instead
                // of all of these 'if's.
            }

            if (call.operator instanceof SqlCaseOperator) {
                return call.operator.getType(this, scope, call);
            }
            if (call.isA(SqlKind.Over)) {
                return call.operator.getType(this, scope, call);
            }

            Scope subScope = scope;
            if (scope instanceof AggregatingScope &&
                call.operator.isAggregator()) {
                subScope = ((AggregatingScope) scope).getScopeAboveAggregation();
            }
            if ((call.operator instanceof SqlFunction)
                    || (call.operator instanceof SqlSpecialOperator)
                    || (call.operator instanceof SqlRowOperator)) {
                RelDataType[] argTypes = new RelDataType[operands.length];
                for (int i = 0; i < operands.length; ++i) {
                    // We can't derive a type for some operands.
                    if (operands[i] instanceof SqlSymbol) {
                        continue; // operand is a symbol e.g. LEADING
                    }
                    RelDataType nodeType = deriveType(subScope, operands[i]);
                    setValidatedNodeType(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }

                if (!(call.operator instanceof SqlJdbcFunctionCall)
                        && call.operator instanceof SqlFunction)
                {
                    SqlFunction unresolvedFunction =
                        (SqlFunction) call.operator;

                    SqlFunction function;
                    if (operands.length == 0 &&
                        syntax == SqlSyntax.FunctionId) {
                        // For example, "LOCALTIME()" is illegal. (It should be
                        // "LOCALTIME", which would have been handled as a
                        // SqlIdentifier.)
                        function = null;
                    } else {
                        if (unresolvedFunction.getFunctionType() == 
                            SqlFunction.SqlFuncTypeName.UserDefinedConstructor)
                        {
                            // TODO jvs 12-Feb-2005:  support real constructor
                            // methods
                            return deriveConstructorType(
                                call,
                                unresolvedFunction);
                        }
                        function = SqlUtil.lookupRoutine(
                            opTab,
                            unresolvedFunction.getNameAsId(),
                            argTypes,
                            unresolvedFunction.getFunctionType() ==
                            SqlFunction.SqlFuncTypeName.UserDefinedProcedure);
                    }
                    if (function == null) {
                        handleUnresolvedFunction(
                            call,
                            unresolvedFunction,
                            argTypes);
                    }
                    call.operator = function;
                }
                return call.operator.getType(this, scope, call);
            }
            if (call.operator instanceof SqlBinaryOperator ||
               call.operator instanceof SqlPostfixOperator ||
               call.operator instanceof SqlPrefixOperator ) {
                RelDataType[] argTypes = new RelDataType[operands.length];
                for (int i = 0; i < operands.length; ++i) {
                    RelDataType nodeType = deriveType(scope, operands[i]);
                    setValidatedNodeType(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }
                type = call.operator.getType(this, scope, call);

                // Validate and determine coercibility and resulting collation
                // name of binary operator if needed.
                if (call.operator instanceof SqlBinaryOperator) {
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
                                    call.operator.name,
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
                checkCharsetAndCollateConsistentIfCharType(type);
                return type;
            } else {
                // TODO: if function can take record type (select statement) as
                // parameter, we need to derive type of SqlSelectOperator,
                // SqlJoinOperator etc here.
                return unknownType;
            }
        }
        // Operand is of a type that we can't derive a type for.
        // If the operand is of a peculiar type, such as a SqlNodeList, then
        // you should override the operator's validateCall() method so that
        // it doesn't try to validate that operand as an expression.
        throw Util.needToImplement(operand);
    }

    private RelDataType deriveConstructorType(
        SqlCall call,
        SqlFunction constructor)
    {
        SqlIdentifier sqlIdentifier = constructor.getSqlIdentifier();
        assert(sqlIdentifier != null);
        RelDataType type = catalogReader.getNamedType(sqlIdentifier);
        if (type == null) {
            // TODO jvs 12-Feb-2005:  proper type name formatting
            throw newValidationError(
                sqlIdentifier,
                EigenbaseResource.instance().newUnknownDatatypeName(
                    sqlIdentifier.toString()));
        }
        // TODO jvs 12-Feb-2005:  constructor method lookup
        if (shouldExpandIdentifiers()) {
            call.operator = new SqlFunction(
                type.getSqlIdentifier(),
                null,
                null,
                null,
                null,
                SqlFunction.SqlFuncTypeName.UserDefinedConstructor);
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
                        call.operator.name,
                        expectedArgCount));
            }
        }

        AssignableOperandsTypeChecking typeChecking =
            new AssignableOperandsTypeChecking(argTypes);
        String signature =
            typeChecking.getAllowedSignatures(unresolvedFunction);
        throw newValidationError(
            call,
            EigenbaseResource.instance().newValidatorUnknownFunction(
                signature));
    }

    /**
     * If an identifier is a legitimate call to a function which has no
     * arguments and requires no parentheses (for example "CURRENT_USER"),
     * returns a call to that function, otherwise returns null.
     */
    public SqlCall makeCall(SqlIdentifier id) {
        if (id.names.length == 1) {
            List list = opTab.lookupOperatorOverloads(id, SqlSyntax.Function);
            for (int i = 0; i < list.size(); i++) {
                SqlOperator operator = (SqlOperator) list.get(i);
                if (operator.getSyntax() == SqlSyntax.FunctionId) {
                    // Even though this looks like an identifier, it is a
                    // actually a call to a function. Construct a fake
                    // call to this function, so we can use the regular
                    // operator validation.
                    return new SqlCall(operator, SqlNode.emptyArray,
                        id.getParserPosition());
                }
            }
        }
        return null;
    }


    private void inferUnknownTypes(
        RelDataType inferredType,
        Scope scope,
        SqlNode node)
    {
        final Scope newScope = (Scope) scopes.get(node);
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
            setValidatedNodeType(node, newInferredType);
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
                setValidatedNodeType(
                    caseCall.getElseOperand(),
                    returnType);
            }
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            UnknownParamInference paramTypeInference =
                call.operator.getUnknownParamTypeInference();
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
        Scope scope)
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
                    exp = addAlias(exp, alias);
                    break;
                }
            }
        }
        aliases.add(alias);
        types.add(deriveType(scope, exp));
        list.add(exp);
    }

    /**
     * Converts an expression "expr" into "expr AS alias".
     */
    private static SqlNode addAlias(
        SqlNode expr,
        String alias)
    {
        final SqlIdentifier id = new SqlIdentifier(alias, SqlParserPos.ZERO);
        return SqlStdOperatorTable.instance().asOperator.createCall(expr, id,
            SqlParserPos.ZERO);
    }

    /**
     * Derives an alias for an expression. If no alias can be derived, returns
     * null if <code>ordinal</code> is less than zero, otherwise generates an
     * alias <code>EXPR$<i>ordinal</i></code>.
     */
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
                return deriveAliasFromOrdinal(ordinal);
            }
        }
    }

    public static String deriveAliasFromOrdinal(int ordinal)
    {
        // Use a '$' so that queries can't easily reference the
        // generated name.
        return "EXPR$" + ordinal;
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
        Scope usingScope,
        String alias,
        Namespace ns)
    {
        namespaces.put(ns.getNode(), ns);
        if (usingScope != null) {
            usingScope.addChild(ns, alias);
        }
    }

    private SqlNode registerFrom(
        Scope parentScope,
        Scope usingScope,
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
            final SqlNode newRight = registerFrom(parentScope, joinScope,
                right, null);
            if (newRight != right) {
                join.setOperand(SqlJoin.RIGHT_OPERAND, newRight);
            }
            final JoinNamespace joinNamespace = new JoinNamespace(join);
            registerNamespace(null, null, joinNamespace);
            return join;

        case SqlKind.IdentifierORDINAL:
            newNode = node;
            if (alias == null) {
                alias = deriveAlias(node, -1);
                if (shouldExpandIdentifiers()) {
                    newNode = addAlias(node, alias);
                }
            }
            final SqlIdentifier id = (SqlIdentifier) node;
            final IdentifierNamespace newNs = new IdentifierNamespace(id);
            registerNamespace(usingScope, alias, newNs);
            return newNode;

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
                    newNode = addAlias(node, alias);
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
        Scope parentScope,
        Scope usingScope,
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
            final SelectNamespace selectNs = new SelectNamespace(select);
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
            Scope aggScope = selectScope;
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
                Scope orderByScope =
                    new OrderByScope(aggScope, orderList, select);
                orderScopes.put(select, orderByScope);
                registerSubqueries(orderByScope, orderList);
            }
            break;

        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final SetopNamespace setopNamespace =
                new SetopNamespace((SqlCall) node);
            registerNamespace(usingScope, alias, setopNamespace);
            call = (SqlCall) node;
            // A setop is in the same scope as its parent.
            scopes.put(call, parentScope);
            registerQuery(parentScope, null, call.operands[0], null);
            registerQuery(parentScope, null, call.operands[1], null);
            break;

        case SqlKind.ValuesORDINAL:
            final TableConstructorNamespace tableConstructorNamespace =
                new TableConstructorNamespace(node, parentScope);
            registerNamespace(usingScope, alias, tableConstructorNamespace);
            call = (SqlCall) node;
            operands = call.getOperands();
            for (int i = 0; i < operands.length; ++i) {
                assert(operands[i].isA(SqlKind.Row));

                // FIXME jvs 9-Feb-2004:  Correlation should
                // be illegal in these subqueries.  Same goes for
                // any non-lateral SELECT in the FROM list.
                registerSubqueries(parentScope, operands[i]);
            }
            break;

        case SqlKind.InsertORDINAL:
            SqlInsert insertCall = (SqlInsert) node;
            IdentifierNamespace insertScope =
                new IdentifierNamespace(
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
                new UnnestNamespace(node, usingScope);
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
            registerNamespace(cs,  deriveAlias(node, nextGeneratedId++), ttableConstructorNamespace);
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

    /**
     * Returns whether a SELECT statement is an aggregation. Criteria are:
     * (1) contains GROUP BY, or
     * (2) contains HAVING, or
     * (3) SELECT or ORDER BY clause contains aggregate functions. (Windowed
     *     aggregate functions, such as <code>SUM(x) OVER w</code>, don't
     *     count.)
     */
    public boolean isAggregate(SqlSelect select) {
        return select.getGroup() != null ||
            select.getHaving() != null ||
            aggFinder.findAgg(select.getSelectList()) != null;
    }

    private void registerSubqueries(
        Scope parentScope,
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

    /**
     * Resolves an identifier to a fully-qualified name.
     * @param id
     */
    public void validateIdentifier(SqlIdentifier id, Scope scope)
    {
        final SqlIdentifier fqId = scope.fullyQualify(id);
        Util.discard(fqId);
    }

    /**
     * Validates a literal.
     */
    public void validateLiteral(SqlLiteral literal)
    {
        // default is to do nothing
    }

    /**
     * Validates a {@link SqlIntervalQualifier}
     */
    public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
        // default is to do nothing
    }

    protected void validateFrom(
        SqlNode node,
        RelDataType targetRowType,
        Scope scope)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            validateFrom(((SqlCall) node).getOperands()[0], targetRowType, scope);
            return; // don't break -- AS doesn't have a scope to validate
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

    protected void validateOver(SqlCall call, Scope scope)
    {
        throw Util.newInternal("OVER unexpected in this context");
    }

    protected void validateJoin(SqlJoin join, Scope scope)
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
        final Scope joinScope = (Scope) scopes.get(join);
        switch (conditionType.ordinal) {
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
        switch (joinType.ordinal) {
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
            final Namespace namespace = getNamespace(leftOrRight);
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
        final Scope fromScope = getScope(select, SqlSelect.FROM_OPERAND);
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
        final Scope orderScope = getScope(select, SqlSelect.ORDER_OPERAND);
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
        final Scope groupScope = getScope(select, SqlSelect.GROUP_OPERAND);
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
        final Scope whereScope = getScope(select, SqlSelect.WHERE_OPERAND);
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
        final Scope havingScope = getScope(select, SqlSelect.HAVING_OPERAND);
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
        final Scope selectScope = getScope(select, SqlSelect.SELECT_OPERAND);
        final ArrayList expandedSelectItems = new ArrayList();
        final ArrayList aliasList = new ArrayList();
        final ArrayList typeList = new ArrayList();
        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode selectItem = selectItems.get(i);
            if (selectScope instanceof SqlValidator.AggregatingScope) {
                SqlValidator.AggregatingScope aggScope =
                    (SqlValidator.AggregatingScope) selectScope;
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
        final RelDataType [] types =
            (RelDataType []) typeList.toArray(emptyTypes);
        final String [] aliases = (String []) aliasList.toArray(emptyStrings);
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
                fieldNames[i] = deriveAliasFromOrdinal(i);
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
        Table table = targetNamespace.getTable();

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
        final ListScope fromScope = (ListScope) getScope(select, SqlSelect.WHERE_OPERAND);
        IdentifierNamespace targetNamespace =
            (IdentifierNamespace) fromScope.getChild(name);
        validateNamespace(targetNamespace);
        Table table = targetNamespace.getTable();

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
        Scope scope)
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

    private static RelDataType lookupField(
        final RelDataType rowType,
        String columnName)
    {
        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(columnName)) {
                return field.getType();
            }
        }
        return null;
    }

    /**
     * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a
     * namespace. The returned namespace may represent a schema, table, column,
     * etc.
     *
     * @pre names.length > 0
     * @post return != null
     */
    protected Namespace lookup(Scope scope, String[] names)
    {
        Util.pre(names.length > 0, "names.length > 0");
        Namespace namespace = null;
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
    public FarragoException newValidationError(SqlNode node,
        SqlValidatorException e)
    {
        Util.pre(node != null, "node != null");
        final SqlParserPos pos = node.getParserPosition();
        FarragoException contextExcn =
            EigenbaseResource.instance().newValidatorContext(
                new Integer(pos.getLineNum()),
                new Integer(pos.getColumnNum()),
                e);
        contextExcn.setPosition(pos.getLineNum(), pos.getColumnNum());
        return contextExcn;
    }

    /**
     * Derives an alias for a node. If it cannot derive an alias, returns null.
     *
     * <p>This method doesn't try very hard. It doesn't invent mangled aliases,
     * and doesn't even recognize an AS clause. There are other methods for
     * that. It just takes the last part of an identifier.
     */
    public static String getAlias(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            String[] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];
        } else {
            return null;
        }
    }

    private SqlNodeList deepCopy(SqlNodeList list) {
        SqlNodeList copy = new SqlNodeList(list.getParserPosition());
        for (int i = 0; i < list.size(); i++) {
            SqlNode node = list.get(i);
            copy.add(deepCopy(node));
        }
        return copy;
    }

    private SqlNode deepCopy(SqlNode node) {
        if (node instanceof SqlCall) {
            return deepCopy((SqlCall) node);
        } else {
            return (SqlNode) node.clone();
        }
    }

    private SqlCall deepCopy(SqlCall call) {
        SqlCall copy = (SqlCall) call.clone();
        for (int i = 0; i < copy.operands.length; i++) {
            copy.operands[i] = deepCopy(copy.operands[i]);
        }
        return copy;
    }

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
    public SqlWindow resolveWindow(SqlNode windowOrRef, Scope scope) {
        SqlWindow window;
        if (windowOrRef instanceof SqlIdentifier) {
            String windowName = ((SqlIdentifier) windowOrRef).getSimple();
            window = scope.lookupWindow(windowName);
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

    /**
     * A <code>CatalogReader</code> supplies catalog information for a {@link
     * SqlValidator}.
     */
    public interface CatalogReader
    {
        /**
         * Finds a table with the given name, possibly qualified.
         *
         * @return named table, or null if not found
         */
        Table getTable(String [] names);

        /**
         * Finds a user-defined type with the given name, possibly qualified.
         *
         *<p>
         *
         * NOTE jvs 12-Feb-2005:  the reason this method is defined here
         * instead of on RelDataTypeFactory is that it has to take into
         * account context-dependent information such as SQL schema path,
         * whereas a type factory is context-independent.
         *
         * @return named type, or null if not found
         */
        RelDataType getNamedType(SqlIdentifier typeName);

        /**
         * Gets schema object names as specified. They can be schema or table
         * object.
         * If names array contain 1 element, return all schema names and
         *    all table names under the default schema (if that is set)
         * If names array contain 2 elements, treat 1st element as schema name
         *    and return all table names in this schema
         *
         * @param names the array contains either 2 elements representing a
         * partially qualified object name in the format of 'schema.object',
         * or an unqualified name in the format of 'object'
         *
         * @return the list of all object (schema and table) names under
         *         the above criteria
         */
        String [] getAllSchemaObjectNames(String [] names);
    }

    /**
     * A <code>Table</code> supplies a {@link SqlValidator} with the metadata
     * for a table.
     */
    public interface Table
    {
        RelDataType getRowType();

        String [] getQualifiedName();
    }

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

    /**
     * Namespace in which to search for identifiers.
     */
    public interface Namespace {
        /**
         * Returns the underlying table, or null if there is none.
         */
        Table getTable();

        /**
         * Returns a list of names of output columns. If the scope's type has
         * not yet been derived, derives it. Never returns null.
         *
         * @post return != null
         */
        RelDataType getRowType();

        /**
         * Validates this scope.
         *
         * <p>If the scope has already been validated, does nothing.</p>
         *
         * <p>Please call {@link SqlValidator#validateNamespace} rather than
         * calling this method directly.</p>
         */
        void validate();

        /**
         * lookup hints from this namespace
         */
        String[] lookupHints(SqlParserPos pp);

        SqlNode getNode();

        Namespace lookupChild(String name, Scope [] ancestorOut, int [] offsetOut);

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
         * Returns a list of expressions which are monotonic in this namespace.
         * For example, if the namespace represents a relation ordered by
         * a column called "TIMESTAMP", then the list would contain a
         * {@link SqlIdentifier} called "TIMESTAMP".
         */
        SqlNodeList getMonotonicExprs();
    }

    private abstract class AbstractNamespace implements Namespace {
        /**
         * Whether this scope is currently being validated. Used to check for
         * cycles.
         */
        private Status status = Status.Unvalidated;
        /**
         * Type of the output row, which comprises the name and type of each
         * output column. Set on validate.
         */
        protected RelDataType rowType;
        private Object extra;

        /**
         * Creates an AbstractNamespace.
         */
        AbstractNamespace() {
        }

        public String[] lookupHints(SqlParserPos pp) {
            return Util.emptyStringArray;
        }

        public void validate() {
            switch (status.ordinal) {
            case Status.Unvalidated_ordinal:
                try {
                    status = Status.InProgress;
                    Util.permAssert(rowType == null,
                        "Namespace.rowType must be null before validate has been called");
                    rowType = validateImpl();
                    Util.permAssert(rowType != null,
                        "validateImpl() returned null");
                } finally {
                    status = Status.Valid;
                }
                break;
            case Status.InProgress_ordinal:
                throw Util
                    .newInternal("todo: Cycle detected during type-checking");
            case Status.Valid_ordinal:
                break;
            default:
                throw status.unexpected();
            }
        }

        /**
         * Validates this scope and returns the type of the records it returns.
         * External users should call {@link #validate}, which uses the
         * {@link #status} field to protect against cycles.
         *
         * @return record data type, never null
         * @post return != null
         */
        protected abstract RelDataType validateImpl();

        public RelDataType getRowType() {
            if (rowType == null) {
                validateNamespace(this);
                Util.permAssert(rowType != null, "validate must set rowType");
            }
            return rowType;
        }

        public Table getTable() {
            return null;
        }

        public Namespace lookupChild(
            String name,
            Scope[] ancestorOut,
            int[] offsetOut)
        {
            return lookupFieldNamespace(getRowType(), name, ancestorOut,
                offsetOut);
        }

        public boolean fieldExists(String name) {
            final RelDataType rowType = getRowType();
            final RelDataType dataType = lookupField(rowType, name);
            return dataType != null;
        }

        public Object getExtra()
        {
            return extra;
        }

        public void setExtra(Object o)
        {
            this.extra = o;
        }

        public SqlNodeList getMonotonicExprs()
        {
            return SqlNodeList.Empty;
        }
    }

    /**
     * Name-resolution scope. Represents any position in a parse tree than an
     * expression can be, or anything in the parse tree which has columns.
     *
     * <p>When validating an expression, say "foo"."bar", you first use the
     * {@link #resolve} method of the scope where the expression is defined
     * to locate "foo". If successful, this returns a new scope
     */
    public interface Scope {
        SqlValidator getValidator();

        SqlNode getNode();

        /**
         * Looks up a node with a given name. Returns null if none is found.
         *
         * @param name Name of node to find
         * @param ancestorOut If not null, writes the ancestor scope here
         * @param offsetOut If not null, writes the offset within the ancestor
         *   here
         */
        Namespace resolve(
            String name,
            Scope[] ancestorOut,
            int[] offsetOut);

        /**
         * Finds the table alias which is implicitly qualifying an
         * unqualified column name. Throws an error if there is not exactly
         * one table.
         *
         * <p>This method is only implemented in scopes (such as
         * {@link SelectScope}) which can be the context for name-resolution.
         * In scopes such as {@link IdentifierNamespace}, it throws
         * {@link UnsupportedOperationException}.</p>
         *
         * @param columnName
         * @param ctx Validation context, to appear in any error thrown
         * @return Table alias
         */
        String findQualifyingTableName(String columnName, SqlNode ctx);

        /**
         * Finds all possible column names in this scope
         *
         * @param parentObjName if not null, used to resolve a namespace
         * from which to query the column names
         * @param result an array list of strings to add the result to
         */
        void findAllColumnNames(String parentObjName, List result);

        /**
         * Finds all possible table names in this scope
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

        void addChild(Namespace ns, String alias);

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

    /**
     * A {@link Scope} implements this interface if and only if it is
     * aggregating. Such a scope will return the same set of identifiers as
     * its parent scope, but some of those identifiers may not be accessible
     * because they are not in the GROUP BY clause.
     */
    public interface AggregatingScope extends Scope {

        /**
         * If this scope is aggregating, return the non-aggregating parent
         * scope. Otherwise throws.
         */
        Scope getScopeAboveAggregation();

        /**
         * Checks whether an expression is constant within the GROUP BY clause.
         * If the expression completely matches an expression in the GROUP BY
         * clause, returns true.
         * If the expression is constant within the group, but does not exactly
         * match, returns false.
         * If the expression is not constant, throws an exception.
         *
         * Examples:<ul>
         *
         * <li>If we are 'f(b, c)' in 'SELECT a + f(b, c) FROM t GROUP BY
         * a', then the whole expression matches a group column. Return true.
         *
         * <li>Just an ordinary expression in a GROUP BY query, such as
         * 'f(SUM(a), 1, b)' in 'SELECT f(SUM(a), 1, b) FROM t GROUP BY
         * b'. Returns false.
         *
         * <li>Illegal expression, such as 'f(5, a, b)' in 'SELECT f(a, b) FROM
         * t GROUP BY a'. Throws when it enounters the 'b' operand, because
         * it is not in the group clause.
         *
         * </ul>
         */
        boolean checkAggregateExpr(SqlNode expr);

    }

    /**
     * Deviant implementation of {@link Scope} for the top of the scope stack.
     *
     * <p>It is convenient, because we never need to check whether a scope's
     * parent is null. (This scope knows not to ask about its parents, just like
     * Adam.)
     */
    private class EmptyScope implements Scope
    {
        EmptyScope() {
        }

        public SqlValidator getValidator() {
            return SqlValidator.this;
        }

        public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
            return null;
        }

        public SqlNode getNode() {
            throw new UnsupportedOperationException();
        }

        public Namespace resolve(
            String name,
            Scope[] ancestorOut,
            int[] offsetOut)
        {
            return null;
        }

        public void findAllColumnNames(String parentObjName, List result)
        {
        }

        public void findAllTableNames(List result)
        {
        }

        public String findQualifyingTableName(String columnName,
            SqlNode ctx)
        {
            throw newValidationError(ctx,
                EigenbaseResource.instance().newColumnNotFound(
                    columnName));
        }

        public void addChild(Namespace ns, String alias) {
            // cannot add to the empty scope
            throw new UnsupportedOperationException();
        }

        public SqlWindow lookupWindow(String name) {
            // No windows defined in this scope.
            return null;
        }

        public boolean isMonotonic(SqlNode expr)
        {
            return expr instanceof SqlLiteral ||
                expr instanceof SqlDynamicParam ||
                expr instanceof SqlDataTypeSpec;
        }
    }

    /**
     * Scope defined by a list of child namespaces.
     */
    abstract class ListScope extends DelegatingScope {
        /** List of child {@link SqlValidator.Namespace} objects. */
        protected final ArrayList children = new ArrayList();

        /** Aliases of the {@link SqlValidator.Namespace} objects. */
        protected final ArrayList childrenNames = new ArrayList();

        public ListScope(SqlValidator.Scope parent) {
            super(parent);
        }

        public void addChild(SqlValidator.Namespace ns, String alias) {
            Util.pre(alias != null, "alias != null");
            children.add(ns);
            childrenNames.add(alias);
        }

        protected SqlValidator.Namespace getChild(String alias) {
            if (alias == null) {
                if (children.size() != 1) {
                    throw Util.newInternal(
                        "no alias specified, but more than one table in from list");
                }
                return (SqlValidator.Namespace) children.get(0);
            } else {
                for (int i = 0; i < children.size(); i++) {
                    if (childrenNames.get(i).equals(alias)) {
                        return (SqlValidator.Namespace) children.get(i);
                    }
                }
                return null;
            }
        }

        public void findAllColumnNames(String parentObjName, List result)
        {
            if (parentObjName == null) {
                for (int i = 0; i < children.size(); i++) {
                    Namespace ns = (Namespace) children.get(i);
                    addColumnNames(ns, result);
                }
                parent.findAllColumnNames(parentObjName, result);
            } else {
                final Namespace ns = resolve(parentObjName, null, null);
                if (ns != null) {
                    addColumnNames(ns, result);
                }
            }
        }

        public void findAllTableNames(List result)
        {
            for (int i = 0; i < children.size(); i++) {
                Namespace ns = (Namespace) children.get(i);
                addTableNames(ns, result);
            }
            parent.findAllTableNames(result);
        }

        public String findQualifyingTableName(final String columnName,
            SqlNode ctx)
        {
            int count = 0;
            String tableName = null;
            for (int i = 0; i < children.size(); i++) {
                SqlValidator.Namespace ns =
                    (SqlValidator.Namespace) children.get(i);
                final RelDataType rowType = ns.getRowType();
                if (lookupField(rowType, columnName) != null) {
                    tableName = (String) childrenNames.get(i);
                    count++;
                }
            }
            switch (count) {
            case 0:
                return parent.findQualifyingTableName(columnName, ctx);
            case 1:
                return tableName;
            default:
                throw newValidationError(ctx,
                    EigenbaseResource.instance().newColumnAmbiguous(
                        columnName));
            }
        }

        public SqlValidator.Namespace resolve(
            String name,
            SqlValidator.Scope[] ancestorOut,
            int[] offsetOut)
        {
            // First resolve by looking through the child namespaces.
            final int i = childrenNames.indexOf(name);
            if (i >= 0) {
                if (ancestorOut != null) {
                    ancestorOut[0] = this;
                }
                if (offsetOut != null) {
                    offsetOut[0] = i;
                }
                return (SqlValidator.Namespace) children.get(i);
            }
            // Then call the base class method, which will delegate to the
            // parent scope.
            return parent.resolve(name, ancestorOut, offsetOut);
        }

        public RelDataType resolveColumn(String columnName, SqlNode ctx) {
            int found = 0;
            RelDataType theType = null;
            for (int i = 0; i < children.size(); i++) {
                SqlValidator.Namespace childNs = (SqlValidator.Namespace)
                    children.get(i);
                final RelDataType childRowType = childNs.getRowType();
                final RelDataType type = lookupField(childRowType, columnName);
                if (type != null) {
                    found++;
                    theType = type;
                }
            }
            if (found == 0) {
                return null;
            } else if (found > 1) {
                throw newValidationError(ctx,
                    EigenbaseResource.instance().newColumnAmbiguous(
                        columnName));
            } else {
                return theType;
            }
        }
    }

    /**
     * The name-resolution scope of a SELECT clause. The objects visible are
     * those in the FROM clause, and objects inherited from the parent scope.
     *
     * <p>This object is both a {@link Scope} and a {@link Namespace}. In the
     * query
     *
     * <blockquote>
     * <pre>SELECT name FROM (
     *     SELECT *
     *     FROM emp
     *     WHERE gender = 'F')</code></blockquote>
     *
     * <p>we need to use the {@link SelectScope} as a  {@link Namespace} when
     * resolving 'name', and
     * as a {@link Scope} when resolving 'gender'.</p>
     *
     * <h3>Scopes</h3>
     *
     * <p>In the query
     *
     * <blockquote>
     * <pre>
     * SELECT expr1
     * FROM t1,
     *     t2,
     *     (SELECT expr2 FROM t3) AS q3
     * WHERE c1 IN (SELECT expr3 FROM t4)
     * ORDER BY expr4</pre></blockquote>
     *
     * The scopes available at various points of the query are as follows:<ul>
     * <li>expr1 can see t1, t2, q3</li>
     * <li>expr2 can see t3</li>
     * <li>expr3 can see t4, t1, t2</li>
     * <li>expr4 can see t1, t2, q3, plus (depending upon the dialect) any
     *     aliases defined in the SELECT clause</li>
     * </ul>
     *
     * <h3>Namespaces</h3>
     *
     * <p>In the above query, there are 4 namespaces:<ul>
     * <li>t1</li>
     * <li>t2</li>
     * <li>(SELECT expr2 FROM t3) AS q3</li>
     * <li>(SELECT expr3 FROM t4)</li>
     * </ul>
     */
    public class SelectScope extends ListScope {
        public final SqlSelect select;

        /**
         * Creates a scope corresponding to a SELECT clause.
         *
         * @param parent Parent scope, or null
         * @param select
         */
        SelectScope(
            Scope parent,
            SqlSelect select)
        {
            super(parent);
            this.select = select;
        }

        public Table getTable()
        {
            return null;
        }

        public SqlNode getNode()
        {
            return select;
        }

        public SqlWindow lookupWindow(String name) {
            final SqlNodeList windowList = select.getWindowList();
            for (int i = 0; i < windowList.size(); i++) {
                SqlWindow window = (SqlWindow) windowList.get(i);
                final SqlIdentifier declId = window.getDeclName();
                assert declId.isSimple();
                if (declId.names[0].equals(name)) {
                    return window;
                }
            }
            return super.lookupWindow(name);
        }

        public boolean isMonotonic(SqlNode expr)
        {
            if (children.size() == 1) {
                final SqlNodeList monotonicExprs =
                    ((Namespace) children.get(0)).getMonotonicExprs();
                for (int i = 0; i < monotonicExprs.size(); i++) {
                    SqlNode monotonicExpr = monotonicExprs.get(i);
                    if (expr.equalsDeep(monotonicExpr)) {
                        return true;
                    }
                }
            }
            return super.isMonotonic(expr);
        }
    }

    /**
     * Namespace offered by a subquery.
     */
    public class SelectNamespace extends AbstractNamespace
    {
        private final SqlSelect select;

        SelectNamespace(SqlSelect select)
        {
            this.select = select;
        }

        public SqlNode getNode()
        {
            return select;
        }

        public RelDataType validateImpl() {
            validateSelect(select, unknownType);
            setValidatedNodeType(select, rowType);
            return rowType;
        }

        public String[] lookupHints(SqlParserPos pp) {
            return lookupSelectHints(select, pp);
        }
    }

    /**
     * Scope for resolving identifers within a SELECT statement which has a
     * GROUP BY clause.
     *
     * <p>The same set of identifiers are in scope, but it won't allow
     * access to identifiers or expressions which are not group-expressions.
     */
    class AggregatingSelectScope
        extends SelectScope
        implements AggregatingScope
    {
        private final AggChecker aggChecker;

        AggregatingSelectScope(
            Scope parent,
            SqlSelect select)
        {
            super(parent, select);
            SqlNodeList groupExprs = select.getGroup();
            if (groupExprs == null) {
                groupExprs = SqlNodeList.Empty;
            }
            // We deep-copy the group-list in case subsequent validation
            // modifies it and makes it no longer equivalent.
            groupExprs = deepCopy(groupExprs);
            aggChecker = new AggChecker(
                this,
                groupExprs);
        }

        public Scope getScopeAboveAggregation() {
            return parent;
        }

        public boolean checkAggregateExpr(SqlNode expr) {
            // Make sure expression is valid, throws if not.
            expr.accept(aggChecker);
            return aggChecker.isGroupExpr(expr);
        }
    }

    Namespace lookupFieldNamespace(RelDataType rowType, String name,
        Scope[] ancestorOut, int[] offsetOut)
    {
        final RelDataType dataType = lookupField(rowType, name);
        return new Namespace() {
            public Table getTable() {
                return null;
            }

            public RelDataType getRowType() {
                return dataType;
            }

            public void validate() {
            }

            public String[] lookupHints(SqlParserPos pp) {
                return Util.emptyStringArray;
            }

            public SqlNode getNode() {
                return null;
            }

            public Namespace lookupChild(String name, Scope[] ancestorOut, int[] offsetOut) {
                return null;
            }

            public boolean fieldExists(String name) {
                return false;
            }

            public Object getExtra() {
                return null;
            }

            public void setExtra(Object o) {
            }

            public SqlNodeList getMonotonicExprs() {
                return null;
            }
        };
    }

    /**
     * Validates the right-hand side of an OVER expression. It might be
     * either an {@link SqlIdentifier identifier} referencing a window, or
     * an {@link SqlWindow inline window specification}.
     */
    public void validateWindow(SqlNode windowOrId, Scope scope) {
        switch (windowOrId.getKind().ordinal) {
        case SqlKind.IdentifierORDINAL:
            SqlIdentifier id = (SqlIdentifier) windowOrId;
            final SqlWindow window;
            if (id.isSimple()) {
                final String name = id.names[0];
                window = scope.lookupWindow(name);
            } else {
                window = null;
            }
            if (window == null) {
                throw newValidationError(
                    id,
                    EigenbaseResource.instance().newWindowNotFound(
                        id.toString()));
            }
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
    public void validateCall(SqlCall call, Scope scope)
    {
        Scope operandScope = scope;
        if (scope instanceof SqlValidator.AggregatingScope) {
            SqlValidator.AggregatingScope aggScope =
                (SqlValidator.AggregatingScope) scope;
            if (call.operator.isAggregator()) {
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
        call.operator.validateCall(call, this, scope, operandScope);
   }

    /**
     * Namespace whose contents are defined by the type of an
     * {@link SqlIdentifier identifier}.
     */
    public class IdentifierNamespace extends AbstractNamespace
    {
        public final SqlIdentifier id;

        /** The underlying table. Set on validate. */
        private Table table;

        /** List of monotonic expressions. */
        private final SqlNodeList monotonicExprs =
            new SqlNodeList(SqlParserPos.ZERO);

        IdentifierNamespace(SqlIdentifier id)
        {
            this.id = id;
        }

        public RelDataType validateImpl()
        {
            table = catalogReader.getTable(id.names);
            if (table == null) {
                throw newValidationError(id,
                    EigenbaseResource.instance().newTableNameNotFound(
                        id.toString()));
            }
            if (shouldExpandIdentifiers()) {
                // TODO:  expand qualifiers for column references also
                String [] qualifiedNames = table.getQualifiedName();
                if (qualifiedNames != null) {
                    id.names = qualifiedNames;
                }
            }
            return table.getRowType();
        }

        public SqlNode getNode()
        {
            return id;
        }

        public Table getTable()
        {
            return table;
        }

        public Namespace resolve(
            String name,
            Scope[] ancestorOut,
            int[] offsetOut)
        {
            return null;
        }

        public Namespace lookupChild(String name, Scope[] ancestorOut,
            int[] offsetOut)
        {
            return null;
        }

        public SqlNodeList getMonotonicExprs()
        {
            return monotonicExprs;
        }
    }

    /**
     * Namespace based upon a set operation (UNION, INTERSECT, EXCEPT).
     */
    protected class SetopNamespace extends AbstractNamespace
    {
        private final SqlCall call;

        SetopNamespace(SqlCall call)
        {
            this.call = call;
        }

        public SqlNode getNode()
        {
            return call;
        }

        public RelDataType validateImpl()
        {
            switch (call.getKind().getOrdinal()) {
            case SqlKind.UnionORDINAL:
            case SqlKind.IntersectORDINAL:
            case SqlKind.ExceptORDINAL:
                for (int i = 0; i < call.operands.length; i++) {
                    SqlNode operand = call.operands[i];
                    if (!operand.getKind().isA(SqlKind.Query)) {
                        throw newValidationError(operand,
                            EigenbaseResource.instance().newNeedQueryOp(
                                operand.toString()));
                    }
                    validateQuery(operand);
                }
                final Scope scope = (Scope) scopes.get(call);
                return call.operator.getType(SqlValidator.this, scope, call);
            default:
                throw Util.newInternal("Not a query: " + call.getKind());
            }
        }
    }

    /**
     * Namespace for a table constructor <code>VALUES (expr, expr, ...)</code>.
     */
    public class TableConstructorNamespace extends AbstractNamespace
    {
        private final SqlNode values;
        private final Scope scope;

        TableConstructorNamespace(SqlNode values, Scope scope)
        {
            this.values = values;
            this.scope = scope;
        }

        protected RelDataType validateImpl()
        {
            return getTableConstructorRowType((SqlCall) values, scope);
        }

        public SqlNode getNode()
        {
            return values;
        }

        public Scope getScope()
        {
            return scope;
        }
    }

    /**
     * Namespace for UNNEST
     */
    class UnnestNamespace extends AbstractNamespace
    {
        private final SqlNode child;
        private final Scope scope;

        UnnestNamespace(SqlNode child, Scope scope)
        {
            this.child = child;
            this.scope = scope;
        }

        protected RelDataType validateImpl()
        {
            RelDataType type = scope.getValidator().deriveType(scope, child);
            if (type.isStruct()) {
                return type;
            }
            return typeFactory.createStructType(
                new RelDataType[]{type}, new String[]{ deriveAlias(child, 0) });
        }

        public SqlNode getNode()
        {
            return child;
        }
    }

    /**
     * Namespace for COLLECT/TABLE
     */
    public class CollectNamespace extends AbstractNamespace
    {
        private final SqlNode child;
        private final Scope scope;

        CollectNamespace(SqlNode child, Scope scope)
        {
            this.child = child;
            this.scope = scope;
        }

        protected RelDataType validateImpl()
        {
            RelDataType type = scope.getValidator().deriveTypeImpl(scope, child);
            boolean isNullable = type.isNullable();
            type = typeFactory.createStructType(
                new RelDataType[]{type}, new String[]{ deriveAlias(child, 0) });
            return typeFactory.createTypeWithNullability(type, isNullable);
        }

        public SqlNode getNode()
        {
            return child;
        }

        public Scope getScope()
        {
            return scope;
        }
    }

    /**
     * The name-resolution context for expression inside a JOIN clause.
     * The objects visible are the joined table expressions, and those
     * inherited from the parent scope.
     *
     * <p>Consider "SELECT * FROM (A JOIN B ON {exp1}) JOIN C ON {exp2}".
     * {exp1} is resolved in the join scope for "A JOIN B", which contains A
     * and B but not C.</p>
     */
    class JoinScope extends ListScope
    {
        private final Scope usingScope;
        private final SqlJoin join;

        JoinScope(Scope parent, Scope usingScope, SqlJoin join)
        {
            super(parent);
            this.usingScope = usingScope;
            this.join = join;
        }

        public SqlNode getNode()
        {
            return join;
        }

        public void addChild(Namespace ns, String alias) {
            super.addChild(ns, alias);
            if (usingScope != null && usingScope != parent) {
                // We're looking at a join within a join. Recursively add this
                // child to its parent scope too. Example:
                //
                //   select *
                //   from (a join b on expr1)
                //   join c on expr2
                //   where expr3
                //
                // 'a' is a child namespace of 'a join b' and also of
                // 'a join b join c'.
                usingScope.addChild(ns, alias);
            }
        }
    }

    /**
     *todo
     */
    class CollectScope extends ListScope
    {
        private final Scope usingScope;
        private final SqlCall child;

        CollectScope(Scope parent, Scope usingScope, SqlCall child)
        {
            super(parent);
            this.usingScope = usingScope;
            this.child= child;
        }

        public SqlNode getNode()
        {
            return child;
        }
    }

    class JoinNamespace extends AbstractNamespace {
        private final SqlJoin join;

        JoinNamespace(SqlJoin join) {
            this.join = join;
        }

        protected RelDataType validateImpl() {
            final RelDataType leftType = getNamespace(join.getLeft()).getRowType();
            final RelDataType rightType = getNamespace(join.getRight()).getRowType();
            final RelDataType[] types = {leftType, rightType};
            return typeFactory.createJoinType(types);
        }

        public SqlNode getNode() {
            return join;
        }
    }

    /**
     * A scope which delegates all requests to its parent scope.
     * Use this as a base class for defining nested scopes.
     */
    abstract class DelegatingScope implements Scope {
        /**
         * Parent scope. This is where to look next to resolve an identifier;
         * it is not always the parent object in the parse tree.
         *
         * <p>This is never null: at the top of the tree, it is an
         * {@link EmptyScope}.
         */
        protected final Scope parent;

        DelegatingScope(Scope parent)
        {
            super();
            Util.pre(parent != null, "parent != null");
            this.parent = parent;
        }

        /**
         * Registers a relation in this scope.
         * @param ns Namespace representing the result-columns of the relation
         * @param alias Alias with which to reference the relation, must not
         *   be null
         */
        public void addChild(Namespace ns, String alias)
        {
            // By default, you cannot add to a scope. Derived classes can
            // override.
            throw new UnsupportedOperationException();
        }

        public Namespace resolve(
            String name,
            Scope[] ancestorOut,
            int[] offsetOut)
        {
            return parent.resolve(name, ancestorOut, offsetOut);
        }

        protected void addColumnNames(SqlValidator.Namespace ns,
                                    List colNames) {
            final RelDataType rowType;
            try {
                rowType = ns.getRowType();
            } catch (Error e) {
                // namespace is not good - bail out.
                return;
            }

            final RelDataTypeField [] fields = rowType.getFields();
            for (int i = 0; i < fields.length; i++) {
                RelDataTypeField field = fields[i];
                colNames.add(field.getName());
            }
        }

        protected void addTableNames(SqlValidator.Namespace ns,
                                    List tableNames) {
            Table table = ns.getTable();
            if (table == null) return;
            String [] qnames = table.getQualifiedName();
            String fullname = "";
            if (qnames != null) {
                for (int i = 0; i < qnames.length; i++) {
                    fullname += qnames[i];
                    if (i < qnames.length - 1)
                        fullname += ".";
                }
            }
            tableNames.add(fullname);
        }

        public void findAllColumnNames(String parentObjName, List result)
        {
            parent.findAllColumnNames(parentObjName, result);
        }

        public void findAllTableNames(List result)
        {
            parent.findAllTableNames(result);
        }

        public String findQualifyingTableName(String columnName, SqlNode ctx)
        {
            return parent.findQualifyingTableName(columnName, ctx);
        }

        public SqlValidator getValidator() {
            return SqlValidator.this;
        }

        /**
         * Converts an identifier into a fully-qualified identifier. For
         * example, the "empno" in "select empno from emp natural join dept"
         * becomes "emp.empno".
         *
         * If the identifier cannot be resolved, throws. Never returns null.
         */
        public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
            if (identifier.isStar()) {
                return identifier;
            }
            switch (identifier.names.length) {
            case 1:
                {
                    final String columnName = identifier.names[0];
                    final String tableName =
                        findQualifyingTableName(columnName, identifier);

                    //todo: do implicit collation here
                    return new SqlIdentifier(
                        new String[]{tableName, columnName},
                        SqlParserPos.ZERO);
                }

            case 2:
                {
                    final String tableName = identifier.names[0];
                    final Namespace fromNs = resolve(tableName, null, null);
                    if (fromNs == null) {
                        throw newValidationError(identifier,
                            EigenbaseResource.instance().newTableNameNotFound(
                                tableName));
                    }
                    final String columnName = identifier.names[1];
                    final RelDataType fromRowType = fromNs.getRowType();
                    if (lookupField(fromRowType, columnName) != null) {
                        return identifier; // it was fine already
                    } else {
                        throw EigenbaseResource.instance()
                            .newColumnNotFoundInTable(columnName, tableName);
                    }
                }

            default:
                // NOTE jvs 26-May-2004:  lengths greater than 2 are possible
                // for row and structured types
                assert identifier.names.length > 0;
                return identifier;
            }
        }

        public SqlWindow lookupWindow(String name) {
            return parent.lookupWindow(name);
        }

        public boolean isMonotonic(SqlNode expr) {
            return parent.isMonotonic(expr);
        }
    }

    /**
     * Represents the name-resolution context for expressions in an ORDER BY
     * clause.
     *
     * <p>In some dialects of SQL, the ORDER BY clause can reference column
     * aliases in the SELECT clause. For example, the query
     *
     * <blockquote><code>SELECT empno AS x<br/>
     * FROM emp<br/>
     * ORDER BY x</code></blockquote>
     *
     * is valid.
     */
    class OrderByScope extends DelegatingScope {
        private final SqlNodeList orderList;
        private final SqlSelect select;

        OrderByScope(Scope parent, SqlNodeList orderList, SqlSelect select)
        {
            super(parent);
            this.orderList = orderList;
            this.select = select;
        }

        protected RelDataType validateInternal() {
            throw new UnsupportedOperationException();
        }

        public SqlNode getNode() {
            return orderList;
        }

        public void findAllColumnNames(String parentObjName, List result) {
            final Namespace ns = getNamespace(select);
            addColumnNames(ns, result);
        }

        public SqlIdentifier fullyQualify(SqlIdentifier identifier) {
            if (identifier.isSimple()) {
                String name = identifier.names[0];
                final Namespace selectNs = getNamespace(select);
                final RelDataType rowType = selectNs.getRowType();
                final RelDataType dataType = lookupField(rowType, name);
                if (dataType != null) {
                    return identifier;
                }
            }
            return super.fullyQualify(identifier);
        }
    }

    /**
     * Finds an aggregate function.
     *
     * TODO: use aggs registered in the fun table; we currently look for
     *   SUM and COUNT
     */
    static class AggFinder extends SqlBasicVisitor {
        AggFinder() {}

        public SqlNode findAgg(SqlNode node) {
            try {
                node.accept(this);
                return null;
            } catch (Util.FoundOne e) {
                Util.swallow(e, null);
                return (SqlNode) e.getNode();
            }
        }

        public void visit(SqlCall call) {
            if (call.operator.isAggregator()) {
                throw new Util.FoundOne(call);
            }
            if (call.isA(SqlKind.Query)) {
                // don't traverse into queries
                return;
            }
            if (call.isA(SqlKind.Over)) {
                // an aggregate function over a window is not an aggregate!
                return;
            }
            super.visit(call);
        }
    }

    /**
     * Visitor which throws an exception if any component of the expression is
     * not a group expression.
     */
    class AggChecker extends SqlBasicVisitor
    {
        private final AggregatingScope scope;
        private final SqlNodeList groupExprs;

        /**
         * Creates an AggChecker
         */
        AggChecker(AggregatingScope scope, SqlNodeList groupExprs) {
            this.groupExprs = groupExprs;
            this.scope = scope;
        }

        boolean isGroupExpr(SqlNode expr) {
            for (int i = 0; i < groupExprs.size(); i++) {
                SqlNode groupExpr = groupExprs.get(i);
                if (groupExpr.equalsDeep(expr)) {
                    return true;
                }
            }
            return false;
        }

        public void visit(SqlIdentifier id) {
            if (isGroupExpr(id)) {
                return;
            }
            // Is it a call to a parentheses-free function?
            SqlCall call = makeCall(id);
            if (call != null) {
                call.accept(this);
                return;
            }
            // Didn't find the identifer in the group-by list as is, now find
            // it fully-qualified.
            // TODO: It would be better if we always compared fully-qualified
            // to fully-qualified.
            final SqlIdentifier fqId = scope.fullyQualify(id);
            if (isGroupExpr(fqId)) {
                return;
            }
            final String exprString = id.toString();
            throw scope.getValidator().newValidationError(id,
                EigenbaseResource.instance().newNotGroupExpr(exprString));
        }

        public void visit(SqlCall call) {
            if (call.operator.isAggregator()) {
                // For example, 'sum(sal)' in 'SELECT sum(sal) FROM emp GROUP
                // BY deptno'
                return;
            }
            if (isGroupExpr(call)) {
                // This call matches an expression in the GROUP BY clause.
                return;
            }
            if (call.isA(SqlKind.Query)) {
                // Allow queries for now, even though they may contain
                // references to forbidden columns.
                return;
            }
            // Visit the operands.
            super.visit(call);
        }
    }

}
// End SqlValidator.java

