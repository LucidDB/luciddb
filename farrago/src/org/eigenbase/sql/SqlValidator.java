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

import java.nio.charset.Charset;
import java.util.*;

import junit.framework.TestCase;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.fun.SqlRowOperator;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.util.Util;
import net.sf.farrago.util.FarragoException;


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

    /** Maps {@link SqlNode query nodes} to {@link Scope the scope created from
     * them}. */
    private HashMap scopes = new HashMap();
    private SqlNode outermostNode;
    private int nextGeneratedId;
    public final RelDataTypeFactory typeFactory;
    public final RelDataType unknownType;
    public final RelDataType anyType;

    /**
     * Map of derived RelDataType for each node.  This is an IdentityHashMap
     * since in some cases (such as null literals) we need to discriminate by
     * instance.
     */
    private Map nodeToTypeMap = new IdentityHashMap();

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
    }

    //~ Methods ---------------------------------------------------------------

    public SelectScope getScope(SqlSelect node)
    {
        return (SelectScope) scopes.get(node);
    }

    public void check(String s)
    {
    }

    public void checkFails(
        String s,
        String message)
    {
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
        return new SqlNodeList(list, ParserPosition.ZERO);
    }

    /**
     * Returns whether a select item is "*" or "TABLE.*".
     */
    private boolean isStar(SqlNode selectItem)
    {
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            return ((identifier.names.length == 1)
                && identifier.names[0].equals("*"))
                || ((identifier.names.length == 2)
                && identifier.names[1].equals("*"));
        }
        return false;
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
        final SelectScope scope = getScope(select);
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            if ((identifier.names.length == 1)
                    && identifier.names[0].equals("*")) {
                List tableNames = scope.childrenNames;
                for (Iterator tableIter = tableNames.iterator();
                        tableIter.hasNext();) {
                    String tableName = (String) tableIter.next();
                    final SqlNode from = getChild(select, tableName);
                    final Scope fromScope = getScope(from);
                    assert fromScope != null;
                    final RelDataType rowType = fromScope.getRowType();
                    final RelDataTypeField [] fields = rowType.getFields();
                    for (int i = 0; i < fields.length; i++) {
                        RelDataTypeField field = fields[i];
                        String columnName = field.getName();

                        //todo: do real implicit collation here
                        final SqlNode exp =
                            new SqlIdentifier(new String [] {
                                    tableName, columnName
                                },
                                ParserPosition.ZERO);
                        addToSelectList(selectItems, aliases, types, exp, scope);
                    }
                }
                return true;
            } else if ((identifier.names.length == 2)
                    && identifier.names[1].equals("*")) {
                final String tableName = identifier.names[0];
                final SqlNode from = getChild(select, tableName);
                final Scope fromScope = getScope(from);
                assert fromScope != null;
                final RelDataType rowType = fromScope.getRowType();
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

    public void testDoubleNoAlias()
    {
        check("select * from emp join dept");
    }

    public void testDuplicateColumnAliasFails()
    {
        checkFails("select 1 as a, 2 as b, 3 as a from emp", "xyz");
    }

    // NOTE jvs 20-May-2003 -- this is just here as a reminder that GROUP BY
    // validation isn't implemented yet
    public void testInvalidGroupBy(TestCase test)
    {
        try {
            check("select empno, deptno from emp group by deptno");
        } catch (RuntimeException ex) {
            return;
        }
        test.fail("Expected validation error");
    }

    public void testSingleNoAlias()
    {
        check("select * from emp");
    }

    public void testObscuredAliasFails()
    {
        // It is an error to refer to a table which has been given another
        // alias.
        checkFails("select * from emp as e where exists ("
            + "  select 1 from dept where dept.deptno = emp.deptno)", "xyz");
    }

    public void testFromReferenceFails()
    {
        // You cannot refer to a table ('e2') in the parent scope of a query in
        // the from clause.
        checkFails("select * from emp as e1 where exists ("
            + "  select * from emp as e2, "
            + "    (select * from dept where dept.deptno = e2.deptno))", "xyz");
    }

    public void testWhereReference()
    {
        // You can refer to a table ('e1') in the parent scope of a query in
        // the from clause.
        check("select * from emp as e1 where exists ("
            + "  select * from emp as e2, "
            + "    (select * from dept where dept.deptno = e1.deptno))");
    }

    public void testIncompatibleUnionFails()
    {
        checkFails("select 1,2 from emp union select 3 from dept", "xyz");
    }

    public void testUnionOfNonQueryFails()
    {
        checkFails("select 1 from emp union 2", "xyz");
    }

    public void testInTooManyColumnsFails()
    {
        checkFails("select * from emp where deptno in (select deptno,deptno from dept)",
            "xyz");
    }

    public void testNaturalCrossJoinFails()
    {
        checkFails("select * from emp natural cross join dept", "xyz");
    }

    public void testCrossJoinUsingFails()
    {
        checkFails("select * from emp cross join dept using (deptno)",
            "cross join cannot have a using clause");
    }

    public void testCrossJoinOnFails()
    {
        checkFails("select * from emp cross join dept on emp.deptno = dept.deptno",
            "cross join cannot have an on clause");
    }

    public void testInnerJoinWithoutUsingOrOnFails()
    {
        checkFails("select * from emp inner join dept "
            + "where emp.deptno = dept.deptno",
            "INNER, LEFT, RIGHT or FULL join requires ON or USING condition");
    }

    public void testJoinUsingInvalidColsFails()
    {
        checkFails("select * from emp left join dept using (gender)",
            "dept does not have a gender column");
    }

    /**
     * Validates a tree. You can call this method multiple times,
     * but not reentrantly.
     *
     * @pre outermostNode == null
     * @pre topNode.getKind().isA(SqlKind.TopLevel)
     */
    public SqlNode validate(SqlNode topNode)
    {
        Util.pre(outermostNode == null, "outermostNode == null");
        Util.pre(
            topNode.getKind().isA(SqlKind.TopLevel),
            "topNode.getKind().isA(SqlKind.TopLevel)");
        try {
            outermostNode = createInternalSelect(topNode);
            registerQuery(null, outermostNode, null, false);
            validateExpression(outermostNode);
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
        final Scope scope = getScope(node);
        if (scope == null) {
            throw Util.newInternal("Not a query: " + node);
        }
        scope.validate();
    }

    public Scope getScope(SqlNode node)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            return getScope(((SqlCall) node).operands[0]);
        default:
            return (Scope) scopes.get(node);
        }
    }

    private SqlNode getChild(
        SqlSelect select,
        String alias)
    {
        SelectScope selectScope = getScope(select);
        final Scope childScope = selectScope.getChild(alias);
        return childScope.getNode();
    }

    // REVIEW jvs 2-Feb-2004: any reason not to introduce the visitor
    // pattern for createInternalSelect, subquery registration, expression
    // validation, etc?
    private SqlNode createInternalSelect(SqlNode node)
    {
        if (node == null) {
            return node;
        }

        // first transform operands
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            final SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                SqlNode newOperand = createInternalSelect(operand);
                if (newOperand != null) {
                    call.setOperand(i, newOperand);
                }
            }
        } else if (node instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) node;
            for (int i = 0, count = list.size(); i < count; i++) {
                SqlNode operand = list.get(i);
                SqlNode newOperand = createInternalSelect(operand);
                if (newOperand != null) {
                    list.getList().set(i, newOperand);
                }
            }
        }

        // now transform node itself
        if (node.isA(SqlKind.SetQuery) || node.isA(SqlKind.Values)) {
            final SqlNodeList selectList =
                new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*", ParserPosition.ZERO));
            SqlSelect wrapperNode =
                SqlOperatorTable.std().selectOperator.createCall(null,
                    selectList, node, null, null, null, null, null,
                        ParserPosition.ZERO);
            return wrapperNode;
        } else if (node.isA(SqlKind.OrderBy)) {
            SqlCall orderBy = (SqlCall) node;
            SqlNode query =
                orderBy.getOperands()[SqlOrderByOperator.QUERY_OPERAND];
            SqlNodeList orderList =
                (SqlNodeList) orderBy.getOperands()[SqlOrderByOperator.ORDER_OPERAND];
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
                new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*", null));
            SqlSelect wrapperNode =
                SqlOperatorTable.std().selectOperator.createCall(null,
                    selectList, query, null, null, null, null, orderList,
                        ParserPosition.ZERO);
            return wrapperNode;
        } else if (node.isA(SqlKind.ExplicitTable)) {
            // (TABLE t) is equivalent to (SELECT * FROM t)
            SqlCall call = (SqlCall) node;
            final SqlNodeList selectList =
                new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*", null));
            SqlSelect wrapperNode =
                SqlOperatorTable.std().selectOperator.createCall(null,
                    selectList, call.getOperands()[0], null, null, null, null,
                    null, ParserPosition.ZERO);
            return wrapperNode;
        } else if (node.isA(SqlKind.Insert)) {
            SqlInsert call = (SqlInsert) node;
            call.setOperand(
                SqlInsert.SOURCE_SELECT_OPERAND,
                call.getSource());
        } else if (node.isA(SqlKind.Delete)) {
            SqlDelete call = (SqlDelete) node;
            final SqlNodeList selectList =
                new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*", null));
            SqlSelect select =
                SqlOperatorTable.std().selectOperator.createCall(null,
                    selectList, call.getTargetTable(), call.getCondition(),
                    null, null, null, null, ParserPosition.ZERO);
            call.setOperand(SqlDelete.SOURCE_SELECT_OPERAND, select);
        } else if (node.isA(SqlKind.Update)) {
            SqlUpdate call = (SqlUpdate) node;
            final SqlNodeList selectList =
                new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*", null));
            Iterator iter =
                call.getSourceExpressionList().getList().iterator();
            int ordinal = 0;
            while (iter.hasNext()) {
                SqlNode exp = (SqlNode) iter.next();

                // Force unique aliases to avoid a duplicate for Y with
                // SET X=Y
                String alias = deriveAliasFromOrdinal(ordinal);
                selectList.add(
                    SqlOperatorTable.std().asOperator.createCall(
                        exp,
                        new SqlIdentifier(alias, null),
                        null));
                ++ordinal;
            }
            SqlSelect select =
                SqlOperatorTable.std().selectOperator.createCall(null,
                    selectList, call.getTargetTable(), call.getCondition(),
                    null, null, null, null, ParserPosition.ZERO);
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
        assert operand.isA(SqlKind.Row);
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
        return typeFactory.createProjectType((RelDataType []) types.toArray(
                emptyTypes), (String []) aliases.toArray(emptyStrings));
    }

    /**
     * Get the type assigned to a node by validation.
     *
     * @param node the node of interest
     *
     * @return validated type
     */
    public RelDataType getValidatedNodeType(SqlNode node)
    {
        final RelDataType type = (RelDataType) nodeToTypeMap.get(node);
        if (type == null) {
            throw Util.needToImplement("Type derivation for " + node);
        }
        return type;
    }

    //REVIEW wael 07/27/04: changed from private to public access
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
        if (type.isCharType()) {
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

    public RelDataType deriveType(
        Scope scope,
        SqlNode operand)
    {
        // if we already know the type, no need to re-derive
        RelDataType type = (RelDataType) nodeToTypeMap.get(operand);
        if (type != null) {
            return type;
        }
        if (operand instanceof SqlIdentifier) {
            // REVIEW klo 1-Jan-2004:
            // We should have assert scope != null here. The idea is we should
            // figure out the right scope for the SqlIdentifier before we call
            // this method. Therefore, scope can't be null.
            SqlIdentifier id = (SqlIdentifier) operand;

            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            SqlCall call = makeCall(id);
            if (call != null) {
                return call.operator.getType(this, scope, call);
            }

            for (int i = 0; i < id.names.length; i++) {
                String name = id.names[i];
                if (i == 0) {
                    // REVIEW jvs 23-Dec-2003:  what if a table and column have
                    // the same name?
                    final Scope resolvedScope =
                        scope.resolve(name, null, null);
                    if (resolvedScope != null) {
                        // There's a table with the name we seek.
                        type = resolvedScope.getRowType();
                    } else if (scope instanceof SelectScope
                            && ((type =
                                ((SelectScope) scope).resolveColumn(name)) != null)) {
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
                        throw Util.newInternal("Could not find field '" + name
                            + "' in '" + type + "'");
                    }
                    type = fieldType;
                }
            }
            if (type.isCharType()) {
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

        if (operand instanceof SqlDynamicParam) {
            return unknownType;
        }

        // SqlDataType may occur in an expression as the 2nd arg to the CAST
        // function.
        if (operand instanceof SqlDataType) {
            SqlDataType dataType = (SqlDataType) operand;
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

            if ((call.operator instanceof SqlFunction)
                    || (call.operator instanceof SqlSpecialOperator)
                    || (call.operator instanceof SqlRowOperator)) {
                RelDataType[] argTypes = new RelDataType[operands.length];
                for (int i = 0; i < operands.length; ++i) {
                    // We can't derive a type for some operands.
                    if (operands[i] instanceof SqlSymbol) {
                        continue; // operand is a symbol e.g. LEADING
                    }
                    RelDataType nodeType = deriveType(scope, operands[i]);
                    setValidatedNodeType(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }

                if (!(call.operator instanceof SqlJdbcFunctionCall)
                        && call.operator instanceof SqlFunction) {
                    SqlFunction function;
                    if (operands.length == 0 &&
                        syntax == SqlSyntax.FunctionId) {
                        // For example, "LOCALTIME()" is illegal. (It should be
                        // "LOCALTIME", which would have been handled as a
                        // SqlIdentifier.)
                        function = null;
                    } else {
                        function = opTab.lookupFunction(call.operator.name,
                                argTypes);
                    }
                    if (function == null) {
                        // todo: localize "Function"
                        List overloads =
                            opTab.lookupFunctionsByName(call.operator.name);
                        if ((null == overloads)
                                || (Collections.EMPTY_LIST == overloads)) {
                            throw newValidationError(call,
                                EigenbaseResource.instance()
                                .newValidatorUnknownFunction(
                                    call.operator.name));
                        }
                        SqlFunction fun = (SqlFunction) overloads.get(0);
                        final Integer expectedArgCount = (Integer)
                            fun.getOperandsCountDescriptor()
                            .getPossibleNumOfOperands().get(0);
                        throw newValidationError(call,
                            EigenbaseResource.instance().newInvalidArgCount(
                                call.operator.name,
                                expectedArgCount));
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
                    if ((null == operandType1) || (null == operandType2)) {
                        throw Util.newInternal(
                            "operands' types should have been derived");
                    }
                    if (operandType1.isCharType() && operandType2.isCharType()) {
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

                        if (type.isCharType()) {
                            type =
                                typeFactory.createTypeWithCharsetAndCollation(
                                    type,
                                    type.getCharset(),
                                    resultCol);
                        }
                    }
                }
                //determine coercibility and resulting collation name of unary operator if needed
                else if (type.isCharType()) {
                    RelDataType operandType =
                        getValidatedNodeType(call.operands[0]);
                    if (null == operandType) {
                        throw Util.newInternal(
                            "operand's type should have been derived");
                    }
                    if (operandType.isCharType()) {
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
        throw Util.needToImplement(operand);
    }

    /**
     * If an identifier is a legitimate call to a function which has no
     * arguments and requires no parentheses (for example "CURRENT_USER"),
     * returns a call to that function, otherwise returns null.
     */
    public SqlCall makeCall(SqlIdentifier id) {
        if (id.names.length == 1) {
            String name = id.names[0];
            List list = opTab.lookupFunctionsByName(name);
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
        if ((node instanceof SqlDynamicParam)
                || SqlUtil.isNullLiteral(node, false)) {
            if (inferredType.equals(unknownType)) {
                // TODO: scrape up some positional context information
                throw EigenbaseResource.instance().newNullIllegal();
            }

            // REVIEW:  should dynamic parameter types always be nullable?
            RelDataType newInferredType =
                typeFactory.createTypeWithNullability(inferredType, true);
            if (inferredType.isCharType()) {
                newInferredType =
                    typeFactory.createTypeWithCharsetAndCollation(
                        newInferredType,
                        inferredType.getCharset(),
                        inferredType.getCollation());
            }
            setValidatedNodeType(node, newInferredType);
        } else if (node instanceof SqlNodeList) {
            SqlNodeList nodeList = (SqlNodeList) node;
            if (inferredType.isProject()) {
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
                if (inferredType.isProject()) {
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

            List whenList = caseCall.getWhenOperands();
            for (int i = 0; i < whenList.size(); i++) {
                SqlNode sqlNode = (SqlNode) whenList.get(i);
                inferUnknownTypes(unknownType, scope, sqlNode);
            }
            List thenList = caseCall.getThenOperands();
            for (int i = 0; i < thenList.size(); i++) {
                SqlNode sqlNode = (SqlNode) thenList.get(i);
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
                call.operator.getParamTypeInference();
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

    private void checkForIllegalNull(SqlCall call)
    {
        if (!(call.isA(SqlKind.Case) || call.isA(SqlKind.Cast)
                || call.isA(SqlKind.Row) || call.isA(SqlKind.Select))) {
            for (int i = 0; i < call.operands.length; i++) {
                //todo use pp when not null
                if (SqlUtil.isNullLiteral(call.operands[i], false)) {
                    //                    assert(null==node.getParserPosition()) : "todo use pp";
                    throw EigenbaseResource.instance().newNullIllegal();
                }
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
                    exp = SqlOperatorTable.std().asOperator.createCall(
                            exp,
                            new SqlIdentifier(alias, null),
                            null);
                    break;
                }
            }
        }
        aliases.add(alias);
        types.add(deriveType(scope, exp));
        list.add(exp);
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
     * Registers a new scope. We assume that its parent pointer has already
     * been set.
     * @param scope Scope to register
     * @param inFrom Whether the scope is in the FROM list of its parent. Only
     *   scopes in the FROM list can be returned when resolving identifiers.
     */
    private void register(
        Scope scope,
        boolean inFrom)
    {
        scopes.put(
            scope.getNode(),
            scope);
        if (inFrom) {
            assert scope.parent instanceof SelectScope;
            if (!(scope instanceof JoinScope)) {
                SelectScope parentScope = (SelectScope) scope.parent;
                parentScope.children.add(scope);
                parentScope.childrenNames.add(scope.alias);
            }
        }
    }

    private void registerFrom(
        Scope scope,
        SqlNode node,
        String alias)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            if (alias == null) {
                alias = ((SqlCall) node).operands[1].toString();
            }
            registerFrom(scope, ((SqlCall) node).operands[0], alias);
            return;
        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) node;
            final SqlNode left = join.getLeft();
            registerFrom(scope, left, null);
            final SqlNode right = join.getRight();
            registerFrom(scope, right, null);
            final JoinScope joinScope = new JoinScope(scope, join);
            register(joinScope, true);
            return;
        case SqlKind.IdentifierORDINAL:
            if (alias == null) {
                alias = deriveAlias(node, -1);
            }
            final IdentifierScope newScope =
                new IdentifierScope(scope, (SqlIdentifier) node, alias);
            register(newScope, true);
            return;
        case SqlKind.SelectORDINAL:
        case SqlKind.UnionORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.ValuesORDINAL:
            if (alias == null) {
                // give this anonymous construct a name since later
                // query processing stages rely on it
                alias = deriveAlias(node, nextGeneratedId++);
            }
            registerQuery(scope, node, alias, true);
            return;
        default:
            throw node.getKind().unexpected();
        }
    }

    private void registerQuery(
        Scope scope,
        SqlNode node,
        String alias,
        boolean inFrom)
    {
        SqlCall call;

        switch (node.getKind().getOrdinal()) {
        case SqlKind.SelectORDINAL:
            final SqlSelect select = (SqlSelect) node;
            final SelectScope newSelectScope =
                new SelectScope(scope, select, alias);
            register(newSelectScope, inFrom);
            registerFrom(
                newSelectScope,
                select.getFrom(),
                null);
            registerSubqueries(
                newSelectScope,
                select.getSelectList());
            registerSubqueries(
                newSelectScope,
                select.getWhere());
            registerSubqueries(
                newSelectScope,
                select.getGroup());
            registerSubqueries(
                newSelectScope,
                select.getHaving());
            break;
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final Scope newScope =
                new SetopScope(scope, (SqlCall) node, alias);
            register(newScope, inFrom);
            call = (SqlCall) node;
            registerQuery(newScope, call.operands[0], null, false);
            registerQuery(newScope, call.operands[1], null, false);
            break;
        case SqlKind.ValuesORDINAL:
            assert (inFrom);
            assert (scope instanceof SelectScope);
            final TableConstructorScope tableConstructorScope =
                new TableConstructorScope(scope, node, alias);
            register(tableConstructorScope, true);
            call = (SqlCall) node;
            SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; ++i) {
                assert (operands[i].isA(SqlKind.Row));

                // FIXME jvs 9-Feb-2004:  Correlation should
                // be illegal in these subqueries.  Same goes for
                // any SELECT in the FROM list.
                registerSubqueries(tableConstructorScope, operands[i]);
            }
            break;
        case SqlKind.InsertORDINAL:
            assert (!inFrom);
            SqlInsert insertCall = (SqlInsert) node;
            Scope insertScope =
                new IdentifierScope(scope,
                    insertCall.getTargetTable(), alias);
            register(insertScope, false);
            registerQuery(
                scope,
                insertCall.getSourceSelect(),
                null,
                false);
            break;
        case SqlKind.DeleteORDINAL:
            assert (!inFrom);
            SqlDelete deleteCall = (SqlDelete) node;
            registerQuery(
                scope,
                deleteCall.getSourceSelect(),
                null,
                false);
            break;
        case SqlKind.UpdateORDINAL:
            assert (!inFrom);
            SqlUpdate updateCall = (SqlUpdate) node;
            registerQuery(
                scope,
                updateCall.getSourceSelect(),
                null,
                false);
            break;
        default:
            throw node.getKind().unexpected();
        }
    }

    private void registerSubqueries(
        Scope scope,
        SqlNode node)
    {
        if (node == null) {
            return;
        } else if (node.isA(SqlKind.Query)) {
            registerQuery(scope, node, null, false);
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            final SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode operand = operands[i];
                registerSubqueries(scope, operand);
            }
        } else if (node instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) node;
            for (int i = 0, count = list.size(); i < count; i++) {
                registerSubqueries(
                    scope,
                    list.get(i));
            }
        } else {
            ; // atomic node -- can be ignored
        }
    }

    private void validateExpression(SqlNode node)
    {
        if (node == null) {
            return;
        }
        switch (node.getKind().getOrdinal()) {
        case SqlKind.UnionORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.SelectORDINAL:
            validateQuery(node);
            break;
        case SqlKind.InsertORDINAL:
            validateInsert((SqlInsert) node);
            break;
        case SqlKind.DeleteORDINAL:
            validateDelete((SqlDelete) node);
            break;
        case SqlKind.UpdateORDINAL:
            validateUpdate((SqlUpdate) node);
            break;
        case SqlKind.LiteralORDINAL:
            validateLiteral((SqlLiteral) node);
            break;
        default:
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                final SqlNode [] operands = call.getOperands();
                for (int i = 0; i < operands.length; i++) {
                    validateExpression(operands[i]);
                }
                call.operator.validateCall(call, this);
            } else if (node instanceof SqlNodeList) {
                SqlNodeList nodeList = (SqlNodeList) node;
                Iterator iter = nodeList.getList().iterator();
                while (iter.hasNext()) {
                    validateExpression((SqlNode) iter.next());
                }
            }
        }
    }

    protected void validateLiteral(SqlLiteral literal)
    {
        // default is to do nothing
    }

    private void validateFrom(
        SqlNode node,
        RelDataType targetRowType)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            validateFrom(((SqlCall) node).getOperands()[0], targetRowType);
            return; // don't break -- AS doesn't have a scope to validate
        case SqlKind.ValuesORDINAL:
            validateValues((SqlCall) node, targetRowType);
            return;
        default:
            final Scope scope = getScope(node);
            assert scope != null : node;
            scope.validate();
        }
    }

    private void validateSelect(
        SqlSelect select,
        RelDataType targetRowType)
    {
        final SelectScope scope = getScope(select);
        assert scope.rowType == null;

        final SqlNodeList selectItems = select.getSelectList();

        RelDataType fromType = unknownType;
        if ((selectItems.size() == 1) && isStar(selectItems.get(0))) {
            SqlIdentifier id = (SqlIdentifier) selectItems.get(0);
            if (id.names.length == 1) {
                // Special case: for INSERT ... VALUES(?,?), the SQL standard
                // says we're supposed to propagate the target types down.  So
                // iff the select list is an unqualified star (as it will be
                // after an INSERT ... VALUES has been expanded), then
                // propagate.
                fromType = targetRowType;
            }
        }
        validateFrom(
            select.getFrom(),
            fromType);

        validateWhereClause(select, scope);
        validateGroupClause(select, scope);
        validateOrderList(select);
        validateWindowClause(select);

        // validate the select list at the end because the select item might depends on the group by list, or
        // the window function might reference window name in the window clause etc.
        validateSelectList(selectItems, select, targetRowType, scope);

    }

    private void validateWindowClause(SqlSelect select)
    {
        final SqlNodeList windowList = select.getWindowList();
        if(windowList != null) {
            // todo: validate window clause
            // validateExpression(windowList);
        }
    }

    private void validateOrderList(SqlSelect select)
    {
        SqlNodeList orderList = select.getOrderList();
        if (orderList != null) {
            if (!shouldAllowIntermediateOrderBy()) {
                if (select != outermostNode) {
                    throw newValidationError(select,
                        EigenbaseResource.instance().newInvalidOrderByPos());
                }
            }
            validateExpression(orderList);
        }
    }

    private void validateGroupClause(SqlSelect select, final SelectScope scope)
    {
        SqlNodeList group = select.getGroup();
        if (group != null) {
            // TODO jvs 20-May-2003 -- I enabled this for testing Fennel
            // DISTINCT, but there's specific GROUP BY
            // validation that needs to be added, which is why jhyde had a
            // throw here before.
            validateExpression(group);
            inferUnknownTypes(unknownType, scope, group);
        }
    }

    private void validateWhereClause(SqlSelect select, final SelectScope scope)
    {
        // validate WHERE clause
        final SqlNode where = select.getWhere();
        if (where != null) {
            validateExpression(where);
            inferUnknownTypes(
                typeFactory.createSqlType(SqlTypeName.Boolean),
                scope,
                where);
            deriveType(scope, where);
        }
    }

    private void validateSelectList(final SqlNodeList selectItems, SqlSelect select, RelDataType targetRowType, final SelectScope scope)
    {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.
        final ArrayList aliases = new ArrayList();

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        final ArrayList expandedSelectItems = new ArrayList();
        final ArrayList types = new ArrayList();
        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode selectItem = selectItems.get(i);
            validateExpression(selectItem);
            expandSelectItem(selectItem, select, expandedSelectItems, aliases,
                types);
        }

        SqlNodeList newSelectList =
            new SqlNodeList(expandedSelectItems, ParserPosition.ZERO);
        if (shouldExpandIdentifiers()) {
            select.setOperand(SqlSelect.SELECT_OPERAND, newSelectList);
        }

        // TODO: when SELECT appears as a value subquery, should be using
        // something other than unknownType for targetRowType
        inferUnknownTypes(targetRowType, scope, newSelectList);

        assert types.size() == aliases.size();
        scope.rowType =
            typeFactory.createProjectType((RelDataType []) types.toArray(
                    emptyTypes), (String []) aliases.toArray(emptyStrings));
    }

    private RelDataType createTargetRowType(
        RelDataType baseRowType,
        SqlNodeList targetColumnList,
        boolean append)
    {
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
        return typeFactory.createProjectType(types, fieldNames);
    }

    private void validateInsert(SqlInsert call)
    {
        IdentifierScope targetScope =
            (IdentifierScope) getScope(call.getTargetTable());
        targetScope.validate();
        Table table = targetScope.getTable();

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
        RelDataType sourceRowType = getScope(sqlSelect).getRowType();

        if (targetRowType.getFieldCount() != sourceRowType.getFieldCount()) {
            throw EigenbaseResource.instance().newUnmatchInsertColumn(""
                + targetRowType.getFieldCount(),
                "" + sourceRowType.getFieldCount());
        }

        // TODO:  validate updatability, type compatibility, etc.
    }

    private void validateDelete(SqlDelete call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect, unknownType);

        // TODO:  validate updatability, etc.
    }

    private void validateUpdate(SqlUpdate call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();

        SqlIdentifier id = call.getTargetTable();
        IdentifierScope targetScope =
            (IdentifierScope) getScope(sqlSelect).getChild(id.names[id.names.length
                - 1]);
        targetScope.validate();
        Table table = targetScope.getTable();

        RelDataType targetRowType =
            createTargetRowType(
                table.getRowType(),
                call.getTargetColumnList(),
                true);

        validateSelect(sqlSelect, targetRowType);

        // TODO:  validate updatability, type compatibility
    }

    /**
     * Validates a VALUES clause
     */
    private void validateValues(
        SqlCall node,
        RelDataType targetRowType)
    {
        assert node.isA(SqlKind.Values);

        final SqlNode [] operands = node.getOperands();
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            validateExpression(operand);
        }
        for (int i = 0; i < operands.length; i++) {
            if (!operands[i].isA(SqlKind.Row)) {
                throw Util.needToImplement(
                    "Values function where operands are scalars");
            }
            SqlCall rowConstructor = (SqlCall) operands[i];
            if (targetRowType.isProject()
                    && (rowConstructor.getOperands().length != targetRowType
                    .getFieldCount())) {
                return;
            }

            inferUnknownTypes(
                targetRowType,
                getScope(node),
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
                    types[row] =
                        deriveType(
                            getScope(node),
                            thisRow.operands[col]);
                }

                final RelDataType type =
                    ReturnTypeInference.useNullableBiggest.getType(typeFactory,
                        types);

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
     * Converts the <code>ordinal</code>th argument of a call to a positive
     * integer, otherwise throws.
     */
    public static int getOperandAsPositiveInteger(SqlCall call, int ordinal) {
        if (call.operands.length >= ordinal) {
            SqlNode exp = call.operands[ordinal];
            if (exp instanceof SqlLiteral) {
                SqlLiteral literal = (SqlLiteral) exp;
                switch (literal.typeName.ordinal) {
                case SqlTypeName.Decimal_ordinal:
                    int precision = literal.intValue();
                    if (precision >= 0) {
                        return precision;
                    }
                }
            }
        }
        throw EigenbaseResource.instance().newArgumentMustBePositiveInteger(
                call.operator.name);
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
        final ParserPosition pos = node.getParserPosition();
        return EigenbaseResource.instance().newValidatorContext(
            new Integer(pos.getBeginLine()),
            new Integer(pos.getBeginColumn()),
            e);
    }

    //~ Inner Interfaces ------------------------------------------------------

    /**
     * A <code>CatalogReader</code> supplies catalog information for a {@link
     * SqlValidator}.
     */
    public interface CatalogReader
    {
        /**
         * Finds a table with the given name or names. Returns null if not
         * found.
         */
        Table getTable(String [] names);
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
     * Name-resolution scope. Represents any position in a parse tree than an
     * expression can be, or anything in the parse tree which has columns.
     */
    public abstract class Scope
    {
        public final Scope parent;
        private final String alias;

        Scope(
            Scope parent,
            String alias)
        {
            this.parent = parent;
            this.alias = alias;
        }

        public String getAlias()
        {
            return alias;
        }

        public Scope getScopeFromNode(SqlNode node)
        {
            return getScope(node);
        }

        // REVIEW jvs 10-Sept-2003:  I put this in to make sure generated names
        // for constructed columns were consistent, but it doesn't really
        // belong on Scope.
        public String deriveAlias(
            SqlNode node,
            int ordinal)
        {
            return SqlValidator.this.deriveAlias(node, ordinal);
        }

        /**
         * Converts an identifier into a fully-qualified identifier. For
         * example, the "empno" in "select empno from emp natural join dept"
         * becomes "emp.empno".
         */
        public SqlIdentifier fullyQualify(SqlIdentifier identifier)
        {
            switch (identifier.names.length) {
            case 1: {
                final String columnName = identifier.names[0];
                final String tableName = findQualifyingTableName(columnName);

                //todo: do implicit collation here
                return new SqlIdentifier(
                    new String [] { tableName, columnName },
                    ParserPosition.ZERO);
            }
            case 2: {
                final String tableName = identifier.names[0];
                final Scope fromScope = resolve(tableName, null, null);
                if (fromScope == null) {
                    throw newValidationError(identifier,
                        EigenbaseResource.instance().newTableNameNotFound(
                            tableName));
                }
                final String columnName = identifier.names[1];
                if (lookupField(
                            fromScope.getRowType(),
                            columnName) != null) {
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

        public void validate()
        {
        }

        abstract SqlNode getNode();

        /**
         * Looks up a node with a given name. Returns null if none is found.
         *
         * @param name Name of node to find
         * @param ancestorOut If not null, writes the ancestor scope here
         * @param offsetOut If not null, writes the offset within the ancestor
         *   here
         */
        public Scope resolve(
            String name,
            Scope [] ancestorOut,
            int [] offsetOut)
        {
            return null;
        }

        /**
         * Returns the underlying table, or null if there is none.
         */
        public Table getTable()
        {
            return null;
        }

        protected String findQualifyingTableName(String columnName)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Returns a list of names of output columns.
         */
        public abstract RelDataType getRowType();
    }

    /**
     * The name-resolution scope of a SELECT clause. The objects visible are
     * those in the FROM clause, and objects inherited from the parent scope.
     */
    public class SelectScope extends Scope
    {
        private int validateCount = 0;
        public final SqlSelect select;

        /** List of {@link SqlValidator.Scope child scopes}, set on validate. */
        private final ArrayList children = new ArrayList();

        /** Aliases of the {@link SqlValidator.Scope child scopes}, set on
         * validate. */
        private final ArrayList childrenNames = new ArrayList();

        /** Type of the output row, which comprises the name and type of each
         * output column. Set on validate. */
        private RelDataType rowType = null;

        SelectScope(
            Scope parent,
            SqlSelect select,
            String alias)
        {
            super(parent, alias);
            this.select = select;
        }

        protected String findQualifyingTableName(final String columnName)
        {
            int count = 0;
            String tableName = null;
            for (int i = 0; i < children.size(); i++) {
                Scope scope = (Scope) children.get(i);
                if (lookupField(
                            scope.getRowType(),
                            columnName) != null) {
                    tableName = scope.alias;
                    count++;
                }
            }
            switch (count) {
            case 0:
                if (parent == null) {
                    throw EigenbaseResource.instance().newColNotFound(columnName);
                }
                return parent.findQualifyingTableName(columnName);
            case 1:
                return tableName;
            default:
                throw EigenbaseResource.instance().newColumnAmbiguous(columnName);
            }
        }

        public RelDataType getRowType()
        {
            return rowType;
        }

        public void validate()
        {
            assert validateCount++ == 0 : validateCount;
            validateSelect(select, unknownType);
        }

        SqlNode getNode()
        {
            return select;
        }

        private Scope getChild(String alias)
        {
            if (alias == null) {
                if (children.size() != 1) {
                    throw Util.newInternal(
                        "no alias specified, but more than one table in from list");
                }
                return (Scope) children.get(0);
            } else {
                for (int i = 0; i < children.size(); i++) {
                    final Scope child = (Scope) children.get(i);
                    if (child.alias.equals(alias)) {
                        return child;
                    }
                }
                return null;
            }
        }

        public Scope resolve(
            String name,
            Scope [] ancestorOut,
            int [] offsetOut)
        {
            final int i = childrenNames.indexOf(name);
            if (i >= 0) {
                if (ancestorOut != null) {
                    ancestorOut[0] = this;
                }
                if (offsetOut != null) {
                    offsetOut[0] = i;
                }
                return (Scope) children.get(i);
            }
            if (parent != null) {
                return parent.resolve(name, ancestorOut, offsetOut);
            }
            return null;
        }

        public RelDataType resolveColumn(String name)
        {
            int found = 0;
            RelDataType theType = null;
            for (int i = 0; i < children.size(); i++) {
                Scope childScope = (Scope) children.get(i);
                final RelDataType type =
                    lookupField(
                        childScope.getRowType(),
                        name);
                if (type != null) {
                    found++;
                    theType = type;
                }
            }
            if (found == 0) {
                return null;
            } else if (found > 1) {
                throw Util.newInternal("More than one column '" + name
                    + "' in scope");
            } else {
                return theType;
            }
        }
    }

    public class IdentifierScope extends Scope
    {
        public final SqlIdentifier id;

        /** The underlying table. Set on validate. */
        private Table table;

        IdentifierScope(
            Scope parent,
            SqlIdentifier id,
            String alias)
        {
            super(parent, alias);
            this.id = id;
        }

        public RelDataType getRowType()
        {
            return table.getRowType();
        }

        public void validate()
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
        }

        SqlNode getNode()
        {
            return id;
        }

        public Table getTable()
        {
            return table;
        }
    }

    class SetopScope extends Scope
    {
        private final SqlCall call;

        SetopScope(
            Scope parent,
            SqlCall call,
            String alias)
        {
            super(parent, alias);
            this.call = call;
        }

        SqlNode getNode()
        {
            return call;
        }

        public RelDataType getRowType()
        {
            final Scope childScope = getScope(call.operands[0]);
            return childScope.getRowType();
        }

        public void validate()
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
                break;
            default:
                throw Util.newInternal("Not a query: " + call.getKind());
            }
        }
    }

    class TableConstructorScope extends Scope
    {
        private RelDataType rowType;
        private SqlNode values;

        TableConstructorScope(
            Scope parent,
            SqlNode values,
            String alias)
        {
            super(parent, alias);
            this.values = values;
        }

        public RelDataType getRowType()
        {
            if (rowType == null) {
                rowType = getTableConstructorRowType((SqlCall) values, this);
            }
            return rowType;
        }

        SqlNode getNode()
        {
            return values;
        }
    }

    /**
     * The name-resolution scope of a JOIN operator. The objects visible are
     * the joined table expressions, and those inherited from the parent scope.
     *
     * <p>Consider "SELECT * FROM (A JOIN B ON {exp1}) JOIN C ON {exp2}".
     * {exp1} is resolved in the join scope for "A JOIN B", which contains A
     * and B but not C.</p>
     */
    class JoinScope extends Scope
    {
        private final SqlJoin join;

        JoinScope(
            Scope parent,
            SqlJoin join)
        {
            super(parent, null);
            this.join = join;
        }

        SqlNode getNode()
        {
            return join;
        }

        public RelDataType getRowType()
        {
            Scope leftScope = getScope(join.getLeft());
            final RelDataTypeField [] leftFields =
                leftScope.getRowType().getFields();
            Scope rightScope = getScope(join.getRight());
            final RelDataTypeField [] rightFields =
                rightScope.getRowType().getFields();
            final RelDataTypeFactory.FieldInfo fieldInfo =
                new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return leftFields.length + rightFields.length;
                    }

                    private RelDataTypeField getField(int index)
                    {
                        if (index < leftFields.length) {
                            return leftFields[index];
                        } else {
                            return rightFields[index - leftFields.length];
                        }
                    }

                    public String getFieldName(int index)
                    {
                        return getField(index).getName();
                    }

                    public RelDataType getFieldType(int index)
                    {
                        RelDataType type = getField(index).getType();
                        boolean makeNullable = false;
                        switch (join.getJoinType().getOrdinal()) {
                        case SqlJoinOperator.JoinType.Full_ORDINAL:
                            makeNullable = true;
                            break;
                        case SqlJoinOperator.JoinType.Left_ORDINAL:
                            makeNullable = (index >= leftFields.length);
                            break;
                        case SqlJoinOperator.JoinType.Right_ORDINAL:
                            makeNullable = (index < leftFields.length);
                            break;
                        }
                        if (makeNullable) {
                            type =
                                typeFactory.createTypeWithNullability(type,
                                    true);
                        }
                        return type;
                    }
                };
            return typeFactory.createProjectType(fieldInfo);
        }

        public Scope resolve(
            String name,
            Scope [] ancestorOut,
            int [] offsetOut)
        {
            Scope leftScope = getScope(join.getLeft());
            if ((leftScope.alias != null) && leftScope.alias.equals(name)) {
                if (ancestorOut != null) {
                    ancestorOut[0] = this;
                }
                if (offsetOut != null) {
                    offsetOut[0] = 0;
                }
                return leftScope;
            }
            Scope scope = leftScope.resolve(name, ancestorOut, offsetOut);
            if (scope != null) {
                return scope;
            }
            Scope rightScope = getScope(join.getRight());
            if ((rightScope.alias != null) && rightScope.alias.equals(name)) {
                if (ancestorOut != null) {
                    ancestorOut[0] = this;
                }
                if (offsetOut != null) {
                    offsetOut[0] = 0;
                }
                return rightScope;
            }
            scope = rightScope.resolve(name, ancestorOut, offsetOut);
            if (scope != null) {
                return scope;
            }
            if (parent != null) {
                return parent.resolve(name, ancestorOut, offsetOut);
            }
            return null;
        }

        public void validate()
        {
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            SqlNode condition = join.getCondition();
            boolean isNatural = join.isNatural();
            SqlJoinOperator.JoinType joinType = join.getJoinType();
            SqlJoinOperator.ConditionType conditionType =
                join.getConditionType();
            validateFrom(left, unknownType);
            validateFrom(right, unknownType);
        }
    }
}


// End SqlValidator.java
