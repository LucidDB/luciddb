/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.sql;

import junit.framework.TestCase;
import net.sf.saffron.util.Util;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.type.SqlTypeName;

import java.util.*;
import java.sql.Timestamp;
import java.sql.Time;
import java.math.BigInteger;


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
    //~ Instance fields -------------------------------------------------------

    public final SqlOperatorTable opTab;
    public final SqlContextVariableTable contextVariableTable;
    private final CatalogReader catalogReader;
    /** Maps {@link SqlNode query nodes} to {@link Scope the scope created from
     * them}. */
    private HashMap scopes = new HashMap();
    private SqlNode outermostNode;
    private int nextGeneratedId;
    final SaffronTypeFactory typeFactory;
    public static final SaffronType[] emptyTypes = new SaffronType[0];
    public static final String[] emptyStrings = new String[0];

    final SaffronType unknownType;
    // We may need this to report the exact poistion in the sql for function valiation error
    private SqlNode errorTypeNode;

    /**
     * Map of derived SaffronType for each node.  This is an IdentityHashMap
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
    public SqlValidator(SqlOperatorTable opTab,CatalogReader catalogReader,
            SaffronTypeFactory typeFactory)
    {
        Util.pre(opTab != null, "opTab != null");
        Util.pre(catalogReader != null, "catalogReader != null");
        Util.pre(typeFactory != null, "typeFactory != null");
        this.opTab = opTab;
        this.catalogReader = catalogReader;
        this.typeFactory = typeFactory;
        contextVariableTable = new SqlContextVariableTable(typeFactory);

        // NOTE jvs 23-Dec-2003:  This is used as the type for dynamic
        // parameters and null literals until a real type is imposed for them.
        unknownType = typeFactory.createSqlType(SqlTypeName.Null);
    }

    //~ Methods ---------------------------------------------------------------

    public SelectScope getScope(SqlSelect node)
    {
        return (SelectScope) scopes.get(node);
    }

    public void check(String s)
    {
    }

    public void checkFails(String s,String message)
    {
    }

    /**
     * Returns a list of expressions, with every occurrence of "&#42;" or
     * "TABLE.&#42;" expanded.
     */
    public SqlNodeList expandStar(SqlNodeList selectList,SqlSelect query)
    {
        ArrayList list = new ArrayList();
        ArrayList aliases = new ArrayList();
        ArrayList types = new ArrayList();
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode selectItem = selectList.get(i);
            expandSelectItem(selectItem, query, list, aliases, types);
            continue;
        }
        return new SqlNodeList(list);
    }

    /**
     * Returns whether a select item is "*" or "TABLE.*".
     */
    private boolean isStar(SqlNode selectItem) {
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            return (identifier.names.length == 1) &&
                    identifier.names[0].equals("*") ||
                    (identifier.names.length == 2) &&
                    identifier.names[1].equals("*");
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
    private boolean expandSelectItem(final SqlNode selectItem, SqlSelect select,
            ArrayList selectItems, List aliases, List types) {
        final SelectScope scope = getScope(select);
        if (selectItem instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) selectItem;
            if (
                (identifier.names.length == 1)
                    && identifier.names[0].equals("*")) {
                List tableNames = scope.childrenNames;
                for (
                    Iterator tableIter = tableNames.iterator();
                        tableIter.hasNext();) {
                    String tableName = (String) tableIter.next();
                    final SqlNode from = getChild(select,tableName);
                    final Scope fromScope = getScope(from);
                    assert fromScope != null;
                    final SaffronType rowType = fromScope.getRowType();
                    final SaffronField [] fields = rowType.getFields();
                    for (int i = 0; i < fields.length; i++) {
                        SaffronField field = fields[i];
                        String columnName = field.getName();
                        final SqlNode exp =
                            new SqlIdentifier(
                                new String [] { tableName,columnName },
                                null);
                        addToSelectList(selectItems,aliases, types, exp, scope);
                    }
                }
                return true;
            } else if (
                (identifier.names.length == 2)
                    && identifier.names[1].equals("*")) {
                final String tableName = identifier.names[0];
                final SqlNode from = getChild(select,tableName);
                final Scope fromScope = getScope(from);
                assert fromScope != null;
                final SaffronType rowType = fromScope.getRowType();
                final SaffronField [] fields = rowType.getFields();
                for (int i = 0; i < fields.length; i++) {
                    SaffronField field = fields[i];
                    String columnName = field.getName();
                    final SqlIdentifier exp =
                        new SqlIdentifier(
                            new String [] { tableName,columnName },
                            null);
                    addToSelectList(selectItems,aliases, types, exp, scope);
                }
                return true;
            }
        }
        selectItems.add(selectItem);
        final String alias = deriveAlias(selectItem, aliases.size());
        aliases.add(alias);

        final SaffronType type = deriveType(scope, selectItem);
        setValidatedNodeType(selectItem, type);
        types.add(type);
        return false;
    }

    public void testDoubleNoAlias()
    {
        check("select * from emp join dept");
    }

    public void testDuplicateColumnAliasFails() {
        checkFails("select 1 as a, 2 as b, 3 as a from emp",
                "xyz");
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

    public void testObscuredAliasFails() {
        // It is an error to refer to a table which has been given another
        // alias.
        checkFails("select * from emp as e where exists (" +
                "  select 1 from dept where dept.deptno = emp.deptno)",
                "xyz");
    }

    public void testFromReferenceFails() {
        // You cannot refer to a table ('e2') in the parent scope of a query in
        // the from clause.
        checkFails("select * from emp as e1 where exists (" +
                "  select * from emp as e2, " +
                "    (select * from dept where dept.deptno = e2.deptno))",
                "xyz");
    }

    public void testWhereReference() {
        // You can refer to a table ('e1') in the parent scope of a query in
        // the from clause.
        check("select * from emp as e1 where exists (" +
                "  select * from emp as e2, " +
                "    (select * from dept where dept.deptno = e1.deptno))");
    }

    public void testIncompatibleUnionFails() {
        checkFails("select 1,2 from emp union select 3 from dept",
                "xyz");
    }

    public void testUnionOfNonQueryFails() {
        checkFails("select 1 from emp union 2",
                "xyz");
    }

    public void testInTooManyColumnsFails() {
        checkFails("select * from emp where deptno in (select deptno,deptno from dept)",
                "xyz");
    }

	public void testNaturalCrossJoinFails()
	{
		checkFails("select * from emp natural cross join dept",
                "xyz");
	}

	public void testCrossJoinUsingFails()
	{
		checkFails(
			"select * from emp cross join dept using (deptno)",
			"cross join cannot have a using clause");
	}

	public void testCrossJoinOnFails()
	{
		checkFails(
			"select * from emp cross join dept on emp.deptno = dept.deptno",
			"cross join cannot have an on clause");
	}

	public void testInnerJoinWithoutUsingOrOnFails()
	{
		checkFails(
			"select * from emp inner join dept " +
			"where emp.deptno = dept.deptno",
			"INNER, LEFT, RIGHT or FULL join requires ON or USING condition");
	}

	public void testJoinUsingInvalidColsFails()
	{
		checkFails(
			"select * from emp left join dept using (gender)",
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
        Util.pre(topNode.getKind().isA(SqlKind.TopLevel),
                "topNode.getKind().isA(SqlKind.TopLevel)");
        try {
            outermostNode = createInternalSelect(topNode);
            registerQuery(null,outermostNode,null,false);
            validateExpression(outermostNode);
            if(errorTypeNode != null) {
                // todo: Need a better way to handle this
                Util.needToImplement("Type checking fail :"+errorTypeNode.toString());
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
        final Scope scope = getScope(node);
        if (scope == null) {
            throw Util.newInternal("Not a query: " + node);
        }
        scope.validate();
    }

    public Scope getScope(SqlNode node)
    {
        switch (node.getKind().ordinal_) {
        case SqlKind.AsORDINAL:
            return getScope(((SqlCall) node).operands[0]);
        default:
            return (Scope) scopes.get(node);
        }
    }

    private SqlNode getChild(SqlSelect select,String alias)
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
                    call.setOperand(i,newOperand);
                }
            }
        } else if (node instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) node;
            for (int i = 0, count = list.size(); i < count; i++) {
                SqlNode operand = list.get(i);
                SqlNode newOperand = createInternalSelect(operand);
                if (newOperand != null) {
                    list.getList().set(i,newOperand);
                }
            }
        }

        // now transform node itself
        if (node.isA(SqlKind.SetQuery) || node.isA(SqlKind.Values)) {
            final SqlNodeList selectList = new SqlNodeList();
            selectList.add(new SqlIdentifier("*"));
            SqlSelect wrapperNode =
                opTab.selectOperator.createCall(
                    false,
                    selectList,
                    node,
                    null,
                    null,
                    null,
                    null);
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
                    select.setOperand(SqlSelect.ORDER_OPERAND,orderList);
                    return select;
                }
            }
            final SqlNodeList selectList = new SqlNodeList();
            selectList.add(new SqlIdentifier("*"));
            SqlSelect wrapperNode =
                opTab.selectOperator.createCall(
                    false,
                    selectList,
                    query,
                    null,
                    null,
                    null,
                    orderList);
            return wrapperNode;
        } else if (node.isA(SqlKind.ExplicitTable)) {
            // (TABLE t) is equivalent to (SELECT * FROM t)
            SqlCall call = (SqlCall) node;
            final SqlNodeList selectList = new SqlNodeList();
            selectList.add(new SqlIdentifier("*"));
            SqlSelect wrapperNode =
                opTab.selectOperator.createCall(
                    false,
                    selectList,
                    call.getOperands()[0],
                    null,
                    null,
                    null,
                    null);
            return wrapperNode;
        } else if (node.isA(SqlKind.Insert)) {
            SqlInsert call = (SqlInsert) node;
            call.setOperand(SqlInsert.SOURCE_SELECT_OPERAND,call.getSource());
        } else if (node.isA(SqlKind.Delete)) {
            SqlDelete call = (SqlDelete) node;
            final SqlNodeList selectList = new SqlNodeList();
            selectList.add(new SqlIdentifier("*"));
            SqlSelect select =
                opTab.selectOperator.createCall(
                    false,
                    selectList,
                    call.getTargetTable(),
                    call.getCondition(),
                    null,
                    null,
                    null);
            call.setOperand(SqlDelete.SOURCE_SELECT_OPERAND,select);
        } else if (node.isA(SqlKind.Update)) {
            SqlUpdate call = (SqlUpdate) node;
            final SqlNodeList selectList = new SqlNodeList();
            selectList.add(new SqlIdentifier("*"));
            Iterator iter = call.getSourceExpressionList().getList().iterator();
            int ordinal = 0;
            while (iter.hasNext()) {
                SqlNode exp = (SqlNode) iter.next();
                // Force unique aliases to avoid a duplicate for Y with
                // SET X=Y
                String alias = deriveAliasFromOrdinal(ordinal);
                selectList.add(
                    opTab.asOperator.createCall(
                        exp,new SqlIdentifier(alias)));
                ++ordinal;
            }
            SqlSelect select =
                opTab.selectOperator.createCall(
                    false,
                    selectList,
                    call.getTargetTable(),
                    call.getCondition(),
                    null,
                    null,
                    null);
            call.setOperand(SqlUpdate.SOURCE_SELECT_OPERAND,select);
        }
        return node;
    }

    private SaffronType getTableConstructorRowType(SqlCall values,Scope scope)
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
            final String alias = deriveAlias(operands[i],i);
            aliases.add(alias);
            final SaffronType type = deriveType(scope, operands[i]);
            types.add(type);
        }
        return typeFactory.createProjectType(
                (SaffronType[]) types.toArray(emptyTypes),
                (String[]) aliases.toArray(emptyStrings));
    }

    /**
     * Get the type assigned to a node by validation.
     *
     * @param node the node of interest
     *
     * @return validated type
     */
    public SaffronType getValidatedNodeType(SqlNode node)
    {
        final SaffronType type = (SaffronType) nodeToTypeMap.get(node);
        if (type == null) {
            throw Util.needToImplement("Type derivation for " + node);
        }
        return type;
    }

    private void setValidatedNodeType(SqlNode node,SaffronType type)
    {
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return;
        }
        nodeToTypeMap.put(node,type);
    }

    SaffronType deriveType(Scope scope, SqlNode operand)
    {
        // if we already know the type, no need to re-derive
        SaffronType type = (SaffronType) nodeToTypeMap.get(operand);
        if (type != null) {
            return type;
        }

        if (operand instanceof SqlIdentifier) {
            // REVIEW klo 1-Jan-2004:
            // We should have assert scope != null here. The idea is we should figure
            // out the right scope for the SqlIdentifier before we call this method. Therefore, scope can't be
            // null.

            SqlIdentifier id = (SqlIdentifier) operand;

            // first check for reserved identifiers like CURRENT_USER
            type = contextVariableTable.deriveType(id);
            if (type != null) {
                return type;
            }

            for (int i = 0; i < id.names.length; i++) {
                String name = id.names[i];
                if (i == 0) {
                    // REVIEW jvs 23-Dec-2003:  what if a table and column have
                    // the same name?
                    final Scope resolvedScope = scope.resolve(name,null,null);
                    if (resolvedScope != null) {
                        // There's a table with the name we seek.
                        type = resolvedScope.getRowType();
                    } else if (scope instanceof SelectScope &&
                            (type = ((SelectScope) scope).resolveColumn(name)) != null) {
                        // There's a column with the name we seek in precisely
                        // one of the tables.
                        ;
                    } else {
                        // REVIEW jvs 9-Feb-2004: I changed this into a
                        // validation error instead of an internal error
                        // because validateExpression isn't currently throwing
                        // any exception for unknown identifiers.  Someone
                        // should probably arrange for that instead.  Also,
                        // This is a case where SqlNode parse position is
                        // definitely needed.  I took off the "in scope" part
                        // because the internal information is not wanted
                        // after I made this a non-internal error.
                        throw newValidationError(
                            "Unknown identifier '" + name + "'");
                    }
                } else {
                    type = lookupField(type,name);
                    if (type == null) {
                        throw Util.newInternal("Could not find field '" +
                                name + "' in '" + type + "'");
                    }
                }
            }
            return type;
        }
        if (operand instanceof SqlLiteral.Numeric) {
            SqlLiteral.Numeric numLiteral = (SqlLiteral.Numeric) operand;
            if (numLiteral.isExact()){
                int scale = numLiteral.getScale().intValue();
                if (0==scale){
                    return typeFactory.createSqlType(SqlTypeName.Integer);
                }
                //else we have a decimal
                return typeFactory.createSqlType(SqlTypeName.Decimal,numLiteral.getPrec().intValue(), scale);
            }
            //else we have a float, real or double. Make them all double for now
            return typeFactory.createSqlType(SqlTypeName.Double);
        }

        if (operand instanceof SqlLiteral)
        {
            SqlLiteral literal = (SqlLiteral) operand;
            final Object value = literal.getValue();
            if (value == null) {
                return typeFactory.createSqlType(SqlTypeName.Null);
            } else if (value instanceof Boolean) {
                return typeFactory.createSqlType(SqlTypeName.Boolean);
            } else if (value instanceof SqlLiteral.BitString){
                SqlLiteral.BitString bitLiteral = (SqlLiteral.BitString) value;
                return typeFactory.createSqlType(SqlTypeName.Bit, bitLiteral.getBitCount());
            } else if (value instanceof SqlLiteral.StringLiteral){
                SqlLiteral.StringLiteral strLiteral = (SqlLiteral.StringLiteral) value;
                //REVIEW 1-march-2004 wael: is varchar correct in general?
                return typeFactory.createSqlType(SqlTypeName.Varchar, strLiteral.getValue().length());
            } else if (value instanceof String) {
                return typeFactory.createSqlType(SqlTypeName.Varchar, ((String) value).length());
            } else if (value instanceof byte[]) {
                return typeFactory.createSqlType(SqlTypeName.Varbinary, ((byte[]) value).length);
            } else if (value instanceof BigInteger) {
                //REVIEW 29-feb-2004 wael: can this else if clause safely be removed?
                return typeFactory.createSqlType(SqlTypeName.Integer);
            } else if (value instanceof Date) {
                return typeFactory.createSqlType(SqlTypeName.Date);
            } else if (value instanceof Time) {
                return typeFactory.createSqlType(SqlTypeName.Time);
            } else if (value instanceof Timestamp) {
                return typeFactory.createSqlType(SqlTypeName.Timestamp);
            } else if (value instanceof java.math.BigDecimal) {
                //REVIEW 29-feb-2004 wael: can this else if clause safely be removed?
                return typeFactory.createSqlType(SqlTypeName.Double);
            }
            else {
                throw Util.needToImplement(this.toString() + ", operand=" + operand);
            }
        }
        if (operand instanceof SqlDynamicParam) {
            return unknownType;
        }
        if (operand instanceof SqlCall) {
            SqlCall call = (SqlCall) operand;
            if (call.operator instanceof SqlCaseOperator) {
                SqlCase caseCall = (SqlCase) call;
                List whenList = caseCall.getWhenOperands();
                List thenList = caseCall.getThenOperands();
                for(int i=0;i<whenList.size();i++){
                    SaffronType nodeType = deriveType(scope, (SqlNode)whenList.get(i));
                    setValidatedNodeType((SqlNode)whenList.get(i), nodeType);
                    nodeType = deriveType(scope, (SqlNode)thenList.get(i));
                    setValidatedNodeType((SqlNode)thenList.get(i), nodeType);
                }
                SaffronType nodeType = deriveType(scope, caseCall.getElseOperand());
                setValidatedNodeType(caseCall.getElseOperand(), nodeType);
                return call.operator.getType(this, scope, call);
            }
            if(call.operator instanceof SqlFunction ||
               call.operator instanceof SqlBinaryOperator ||
               call.operator instanceof SqlPostfixOperator ||
               call.operator instanceof SqlPrefixOperator ) {
                // REVIEW: do we need to get the type for special operator?
                // Special operators are "update", "delete" etc.
                SqlCall node = (SqlCall) operand;
                SqlNode[] operands = node.getOperands();
                SaffronType[] argTypes = new SaffronType[operands.length];
                int n = operands.length;
                // special case for AS:  never try to derive type
                // for alias
                if (operand.isA(SqlKind.As)) {
                    n = 1;
                }
                for (int i = 0; i < n; ++i) {
                    SaffronType nodeType = deriveType(scope, operands[i]);
                    setValidatedNodeType(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }
                if (call.operator instanceof SqlFunction) {
                    call.operator = SqlFunctionTable.instance().lookup(call.operator.name, argTypes);
                }
                return call.operator.getType(this, scope, call);
            } else {
                // TODO: if function can take record type (select statement) as parameter,
                // we need to derive type of SqlSelectOperator, SqlJoinOperator etc here.
                return unknownType;
            }
        }
        if (operand instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) operand;
            for (int i = 0; i < list.size(); i++) {
                 deriveType(scope,(SqlNode) list.get(i));
            }
        }
        throw Util.needToImplement(this.toString() + ", operand=" + operand);
    }



    private void inferUnknownTypes(
        SaffronType inferredType,Scope scope,SqlNode node)
    {
        if ((node instanceof SqlDynamicParam) || SqlLiteral.
                isNullLiteral(node)) {
            if (inferredType.equals(unknownType)) {
                // TODO: scrape up some positional context information
                throw newValidationError(
                    "Unable to infer type for " + node);
            }
            // REVIEW:  should dynamic parameter types always be nullable?
            setValidatedNodeType(
                node,
                typeFactory.createTypeWithNullability(
                    inferredType,true));
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
                SaffronType type;
                if (inferredType.isProject()) {
                    type = inferredType.getFields()[i].getType();
                    ++i;
                } else {
                    type = inferredType;
                }
                inferUnknownTypes(type,scope,child);
            }
        } else if (node instanceof SqlCase) {
            SqlCase caseCall = (SqlCase) node;
            List whenList = caseCall.getWhenOperands();
            for (int i = 0; i < whenList.size(); i++) {
                SqlCall sqlNode = (SqlCall) whenList.get(i);
                inferUnknownTypes(unknownType,scope,sqlNode);
            }

            SaffronType returnType = getValidatedNodeType(node);
            List thenList = caseCall.getThenOperands();
            for (int i = 0; i < thenList.size(); i++) {
                SqlCall sqlNode = (SqlCall) whenList.get(i);
                inferUnknownTypes(returnType,scope,sqlNode);
            }

            if (!SqlLiteral.isNullLiteral(caseCall.getElseOperand())){
                inferUnknownTypes(returnType,scope,caseCall.getElseOperand());
            }
            else {
                setValidatedNodeType(caseCall.getElseOperand(), returnType);
            }


        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            SqlOperator.ParamTypeInference paramTypeInference =
                call.operator.getParamTypeInference();
            SqlNode [] operands = call.getOperands();
            SaffronType [] operandTypes = new SaffronType[operands.length];
            if (paramTypeInference == null) {
                // TODO:  eventually should assert(paramTypeInference != null)
                // instead; for now just eat it
                Arrays.fill(operandTypes,unknownType);
            } else {
                paramTypeInference.inferOperandTypes(
                    this,
                    scope,
                    call,
                    inferredType,
                    operandTypes);
            }
            for (int i = 0; i < operands.length; ++i) {
                inferUnknownTypes(operandTypes[i],scope,operands[i]);
            }
        }
    }

    /**
     * Adds an expression to a select list, ensuring that its alias does not
     * clash with any existing expressions on the list.
     */
    private void addToSelectList(ArrayList list,List aliases, List types,
            SqlNode exp, Scope scope) {
        String alias = deriveAlias(exp,-1);
        if (alias == null) {
            alias = "EXPR$";
        }
        if (aliases.contains(alias)) {
            String aliasBase = alias;
            for (int j = 0;; j++) {
                alias = aliasBase + j;
                if (!aliases.contains(alias)) {
                    exp = opTab.asOperator.createCall(
                            exp,
                            new SqlIdentifier(alias));
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
    String deriveAlias(SqlNode node,int ordinal)
    {
        switch (node.getKind().ordinal_) {
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

    private String deriveAliasFromOrdinal(int ordinal)
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

    protected RuntimeException newValidationError(String s)
    {
        return new RuntimeException(s);
    }

    /**
     * Registers a new scope. We assume that its parent pointer has already
     * been set.
     * @param scope Scope to register
     * @param inFrom Whether the scope is in the FROM list of its parent. Only
     *   scopes in the FROM list can be returned when resolving identifiers.
     */
    private void register(Scope scope, boolean inFrom)
    {
        scopes.put(scope.getNode(),scope);
        if (inFrom) {
            assert scope.parent instanceof SelectScope;
            if (!(scope instanceof JoinScope)) {
                SelectScope parentScope = (SelectScope) scope.parent;
                parentScope.children.add(scope);
                parentScope.childrenNames.add(scope.alias);
            }
        }
    }

    private void registerFrom(Scope scope,SqlNode node,String alias)
    {
        switch (node.getKind().ordinal_) {
        case SqlKind.AsORDINAL:
            if (alias == null) {
                alias = ((SqlCall) node).operands[1].toString();
            }
            registerFrom(scope,((SqlCall) node).operands[0],alias);
            return;
        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) node;
            final SqlNode left = join.getLeft();
            registerFrom(scope,left,null);
            final SqlNode right = join.getRight();
            registerFrom(scope,right,null);
            final JoinScope joinScope = new JoinScope(scope,join);
            register(joinScope, true);
            return;
        case SqlKind.IdentifierORDINAL:
            if (alias == null) {
                alias = deriveAlias(node,-1);
            }
            final IdentifierScope newScope =
                new IdentifierScope(scope,(SqlIdentifier) node,alias);
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
                alias = deriveAlias(node,nextGeneratedId++);
            }
            registerQuery(scope,node,alias, true);
            return;
        default:
            throw node.getKind().unexpected();
        }
    }

    private void registerQuery(
        Scope scope,SqlNode node,String alias, boolean inFrom)
    {
        SqlCall call;

        switch (node.getKind().ordinal_) {
        case SqlKind.SelectORDINAL:
            final SqlSelect select = (SqlSelect) node;
            final SelectScope newSelectScope =
                new SelectScope(scope,select,alias);
            register(newSelectScope, inFrom);
            registerFrom(newSelectScope,select.getFrom(),null);
            registerSubqueries(newSelectScope,select.getSelectList());
            registerSubqueries(newSelectScope,select.getWhere());
            registerSubqueries(newSelectScope,select.getGroup());
            registerSubqueries(newSelectScope,select.getHaving());
            break;
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final Scope newScope = new SetopScope(scope,(SqlCall) node,alias);
            register(newScope, inFrom);
            call = (SqlCall) node;
            registerQuery(newScope,call.operands[0],null, false);
            registerQuery(newScope,call.operands[1],null, false);
            break;
        case SqlKind.ValuesORDINAL:
            assert(inFrom);
            assert(scope instanceof SelectScope);
            final TableConstructorScope tableConstructorScope =
                new TableConstructorScope(scope,node,alias);
            register(tableConstructorScope, true);
            call = (SqlCall) node;
            SqlNode [] operands = call.getOperands();
            for (int i = 0; i < operands.length; ++i) {
                assert (operands[i].isA(SqlKind.Row));
                // FIXME jvs 9-Feb-2004:  Correlation should
                // be illegal in these subqueries.  Same goes for
                // any SELECT in the FROM list.
                registerSubqueries(
                    tableConstructorScope,operands[i]);
            }
            break;
        case SqlKind.InsertORDINAL:
            assert(!inFrom);
            SqlInsert insertCall = (SqlInsert) node;
            Scope insertScope = new IdentifierScope(
                scope,
                insertCall.getTargetTable(),
                alias);
            register(insertScope,false);
            registerQuery(
                scope,
                insertCall.getSourceSelect(),
                null,
                false);
            break;
        case SqlKind.DeleteORDINAL:
            assert(!inFrom);
            SqlDelete deleteCall = (SqlDelete) node;
            registerQuery(
                scope,
                deleteCall.getSourceSelect(),
                null,
                false);
            break;
        case SqlKind.UpdateORDINAL:
            assert(!inFrom);
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

    private void registerSubqueries(Scope scope, SqlNode node) {
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
                registerSubqueries(scope, list.get(i));
            }
        } else {
            ; // atomic node -- can be ignored
        }
    }

    private void validateExpression(SqlNode node)
    {
        switch (node.getKind().ordinal_) {
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
        default:
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                final SqlNode [] operands = call.getOperands();
                for (int i = 0; i < operands.length; i++) {
                    validateExpression(operands[i]);
                }
            } else if (node instanceof SqlNodeList) {
                SqlNodeList nodeList = (SqlNodeList) node;
                Iterator iter = nodeList.getList().iterator();
                while (iter.hasNext()) {
                    validateExpression((SqlNode) iter.next());
                }
            }
        }

    }

    private void validateFrom(SqlNode node,SaffronType targetRowType)
    {
        switch (node.getKind().ordinal_) {
        case SqlKind.AsORDINAL:
            validateFrom(((SqlCall) node).getOperands()[0],targetRowType);
            return; // don't break -- AS doesn't have a scope to validate
        case SqlKind.ValuesORDINAL:
            validateValues((SqlCall) node,targetRowType);
            return;
        default:
            final Scope scope = getScope(node);
            assert scope != null : node;
            scope.validate();
        }
    }

    private void validateSelect(SqlSelect select,SaffronType targetRowType)
    {
        final SelectScope scope = getScope(select);
        assert scope.rowType == null;

        final SqlNodeList selectItems = select.getSelectList();

        SaffronType fromType = unknownType;
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
        validateFrom(select.getFrom(),fromType);

        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.
        checkForDuplicateAliases(selectItems);

        final ArrayList aliases = new ArrayList();
        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        final ArrayList expandedSelectItems = new ArrayList();
        final ArrayList types = new ArrayList();
        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode selectItem = selectItems.get(i);
            validateExpression(selectItem);
            expandSelectItem(selectItem, select, expandedSelectItems,
                    aliases, types);
        }


        SqlNodeList newSelectList = new SqlNodeList(expandedSelectItems);
        if (shouldExpandIdentifiers()) {
            select.setOperand(SqlSelect.SELECT_OPERAND,newSelectList);
        }

        // TODO: when SELECT appears as a value subquery, should be using
        // something other than unknownType for targetRowType
        inferUnknownTypes(targetRowType,scope,newSelectList);

        assert types.size() == aliases.size();
        scope.rowType = typeFactory.createProjectType(
                (SaffronType[]) types.toArray(emptyTypes),
                (String[]) aliases.toArray(emptyStrings));
        // validate WHERE clause
        final SqlNode where = select.getWhere();
        if (where != null) {
            validateExpression(where);
            inferUnknownTypes(
                typeFactory.createSqlType(SqlTypeName.Boolean),
                scope,where);
        }
        SqlNodeList group = select.getGroup();
        if (group != null) {
            // TODO jvs 20-May-2003 -- I enabled this for testing Fennel
            // DISTINCT, but there's specific GROUP BY
            // validation that needs to be added, which is why jhyde had a
            // throw here before.
            validateExpression(group);
            inferUnknownTypes(unknownType,scope,group);
        }
        SqlNodeList orderList = select.getOrderList();
        if (orderList != null) {
            if (!shouldAllowIntermediateOrderBy()) {
                if (select != outermostNode) {
                    throw newValidationError(
                        "ORDER BY is only allowed on top-level SELECT");
                }
            }
            validateExpression(orderList);
        }
    }

    private void checkForDuplicateAliases(final SqlNodeList selectItems)
    {
        final ArrayList aliases = new ArrayList();
        for (int i = 0; i < selectItems.size(); i++)
        {
            SqlNode selectItem = selectItems.get(i);
            if (isStar(selectItem)) {
                continue;
            }
            final String alias = deriveAlias(selectItem, i);
            if (aliases.contains(alias)) {
                throw newValidationError("More than one column has alias '" +
                        alias + "'");
            }
            aliases.add(alias);
        }
    }


    private SaffronType createTargetRowType(
        SaffronType baseRowType,
        SqlNodeList targetColumnList,
        boolean append)
    {
        SaffronField [] targetFields = baseRowType.getFields();
        int targetColumnCount = targetColumnList.size();
        if (append) {
            targetColumnCount += baseRowType.getFieldCount();
        }
        SaffronType [] types = new SaffronType[targetColumnCount];
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
                throw newValidationError(
                    "Unknown target column "+id.getSimple());
            }
            fieldNames[iTarget] = targetFields[iColumn].getName();
            types[iTarget] = targetFields[iColumn].getType();
            ++iTarget;
        }
        return typeFactory.createProjectType(types,fieldNames);
    }

    private void validateInsert(SqlInsert call)
    {
        IdentifierScope targetScope = (IdentifierScope)
            getScope(call.getTargetTable());
        targetScope.validate();
        Table table = targetScope.getTable();

        SaffronType targetRowType = table.getRowType();
        if (call.getTargetColumnList() != null) {
            targetRowType = createTargetRowType(
                targetRowType,
                call.getTargetColumnList(),
                false);
        }

        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect,targetRowType);
        SaffronType sourceRowType = getScope(sqlSelect).getRowType();

        if (targetRowType.getFieldCount() != sourceRowType.getFieldCount()) {
            throw newValidationError(
                "Number of INSERT target columns ("
                + targetRowType.getFieldCount()
                + ") does not equal number of source items ("
                + sourceRowType.getFieldCount() + ")");
        }
        // TODO:  validate updatability, type compatibility, etc.
    }

    private void validateDelete(SqlDelete call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect(sqlSelect,unknownType);
        // TODO:  validate updatability, etc.
    }

    private void validateUpdate(SqlUpdate call)
    {
        SqlSelect sqlSelect = call.getSourceSelect();

        SqlIdentifier id = call.getTargetTable();
        IdentifierScope targetScope = (IdentifierScope)
            getScope(sqlSelect).getChild(
                id.names[id.names.length - 1]);
        targetScope.validate();
        Table table = targetScope.getTable();

        SaffronType targetRowType = createTargetRowType(
            table.getRowType(),
            call.getTargetColumnList(),
            true);

        validateSelect(sqlSelect,targetRowType);
        // TODO:  validate updatability, type compatibility
    }

    private void validateValues(SqlCall node,SaffronType targetRowType)
    {
        // TODO: validate that all rows have the same number of columns
        //   and that expressions in each column are union-compatible.
        // jhyde: Having the same number of columns is a pre-requisite for
        //   being union-compatible. So just check that they're
        //   union-compatible.
        final SqlNode [] operands = node.getOperands();
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            validateExpression(operand);
        }
        for (int i = 0; i < operands.length; i++) {
            if (!operands[i].isA(SqlKind.Row)) {
                throw Util.needToImplement("Values function where operands are scalars");
            }
            SqlCall rowConstructor = (SqlCall) operands[i];
            if (targetRowType.isProject() &&
                (rowConstructor.getOperands().length
                 != targetRowType.getFieldCount()))
            {
                return;
            }
            inferUnknownTypes(targetRowType,getScope(node),rowConstructor);
        }
    }

    private static SaffronType lookupField(final SaffronType rowType, String columnName) {
        final SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            SaffronField field = fields[i];
            if (field.getName().equals(columnName)) {
                return field.getType();
            }
        }
        return null;
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
        SaffronType getRowType();

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

        Scope(Scope parent,String alias)
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
        public String deriveAlias(SqlNode node,int ordinal)
        {
            return SqlValidator.this.deriveAlias(node,ordinal);
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
                return new SqlIdentifier(
                    new String [] { tableName,columnName },
                    null);
            }
            case 2: {
                final String tableName = identifier.names[0];
                final Scope fromScope = resolve(tableName, null, null);
                if (fromScope == null) {
                    throw newValidationError(
                        "Table '" + tableName + "' not found");
                }
                final String columnName = identifier.names[1];
                if (lookupField(fromScope.getRowType(), columnName) != null) {
                    return identifier; // it was fine already
                } else {
                    throw newValidationError(
                        "Column '" + columnName + "' not found in table '"
                        + tableName + "'");
                }
            }
            default:
                throw newValidationError(
                    "Invalid identifier '" + identifier + "'");
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
        public Scope resolve(String name, Scope[] ancestorOut, int[] offsetOut)
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
        public abstract SaffronType getRowType();
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
        private SaffronType rowType = null;

        SelectScope(Scope parent,SqlSelect select,String alias)
        {
            super(parent,alias);
            this.select = select;
        }

        protected String findQualifyingTableName(final String columnName) {
            int count = 0;
            String tableName = null;
            for (int i = 0; i < children.size(); i++) {
                Scope scope = (Scope) children.get(i);
                if (lookupField(scope.getRowType(), columnName) != null) {
                    tableName = scope.alias;
                    count++;
                }
            }
            switch (count) {
            case 0:
                if (parent == null) {
                    throw newValidationError(
                        "Column '" + columnName + "' not found in any table");
                }
                return parent.findQualifyingTableName(columnName);
            case 1:
                return tableName;
            default:
                throw newValidationError(
                    "Column '" + columnName + "' is ambiguous");
            }
        }

        public SaffronType getRowType() {
            return rowType;
        }

        public void validate()
        {
            assert validateCount++ == 0 : validateCount;
            validateSelect(select,unknownType);
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

        public Scope resolve(String name, Scope[] ancestorOut, int[] offsetOut) {
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

        public SaffronType resolveColumn(String name) {
            int found = 0;
            SaffronType theType = null;
            for (int i = 0; i < children.size(); i++) {
                Scope childScope = (Scope) children.get(i);
                final SaffronType type = lookupField(childScope.getRowType(), name);
                if (type != null) {
                    found++;
                    theType = type;
                }
            }
            if (found == 0) {
                return null;
            } else if (found > 1) {
                throw Util.newInternal("More than one column '" +
                        name + "' in scope");
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

        IdentifierScope(Scope parent,SqlIdentifier id,String alias)
        {
            super(parent,alias);
            this.id = id;
        }

        public SaffronType getRowType() {
            return table.getRowType();
        }

        public void validate()
        {
            table = catalogReader.getTable(id.names);
            if (table == null) {
                throw newValidationError("Table '" + id + "' not found");
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

        SetopScope(Scope parent,SqlCall call,String alias)
        {
            super(parent,alias);
            this.call = call;
        }

        SqlNode getNode()
        {
            return call;
        }

        public SaffronType getRowType() {
            final Scope childScope = getScope(call.operands[0]);
            return childScope.getRowType();
        }

        public void validate() {
            switch (call.getKind().ordinal_) {
            case SqlKind.UnionORDINAL:
            case SqlKind.IntersectORDINAL:
            case SqlKind.MinusORDINAL:
                for (int i = 0; i < call.operands.length; i++) {
                    SqlNode operand = call.operands[i];
                    if (!operand.getKind().isA(SqlKind.Query)) {
                        throw newValidationError("Operand of UNION/INTERSECT/MINUS must be a query: " + operand);
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
        private SaffronType rowType;
        private SqlNode values;

        TableConstructorScope(Scope parent,SqlNode values,String alias)
        {
            super(parent,alias);
            this.values = values;
        }

        public SaffronType getRowType() {
            if (rowType == null) {
                rowType = getTableConstructorRowType((SqlCall) values,this);
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

        JoinScope(Scope parent,SqlJoin join) {
            super(parent,null);
            this.join = join;
        }

        SqlNode getNode() {
            return join;
        }

        public SaffronType getRowType() {
            Scope leftScope = getScope(join.getLeft());
            final SaffronField [] leftFields = leftScope.getRowType().getFields();
            Scope rightScope = getScope(join.getRight());
            final SaffronField [] rightFields = rightScope.getRowType().getFields();
            final SaffronTypeFactory.FieldInfo fieldInfo = new SaffronTypeFactory.FieldInfo() {
                public int getFieldCount() {
                    return leftFields.length + rightFields.length;
                }

                private SaffronField getField(int index) {
                    if (index < leftFields.length) {
                        return leftFields[index];
                    } else {
                        return rightFields[index - leftFields.length];
                    }
                }

                public String getFieldName(int index) {
                    return getField(index).getName();
                }

                public SaffronType getFieldType(int index) {
                    SaffronType type = getField(index).getType();
                    boolean makeNullable = false;
                    switch(join.getJoinType().getOrdinal()) {
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
                        type = typeFactory.createTypeWithNullability(
                            type,true);
                    }
                    return type;
                }
            };
            return typeFactory.createProjectType(fieldInfo);
        }

        public Scope resolve(String name, Scope[] ancestorOut, int[] offsetOut) {
            Scope leftScope = getScope(join.getLeft());
            if (leftScope.alias != null &&
                    leftScope.alias.equals(name)) {
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
            if (rightScope.alias != null &&
                    rightScope.alias.equals(name)) {
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

        public void validate() {
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            SqlNode condition = join.getCondition();
            boolean isNatural = join.isNatural();
            SqlJoinOperator.JoinType joinType = join.getJoinType();
            SqlJoinOperator.ConditionType conditionType = join.getConditionType();
            validateFrom(left,unknownType);
            validateFrom(right,unknownType);
        }
    }
}


// End SqlValidator.java
