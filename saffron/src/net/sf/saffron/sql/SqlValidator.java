/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Tech
// (C) Copyright 2003-2004 John V. Sichi
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
import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.fun.SqlRowOperator;
import net.sf.saffron.sql.parser.ParserPosition;
import net.sf.saffron.util.Util;

import java.nio.charset.Charset;
import java.util.*;


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
    public final SaffronTypeFactory typeFactory;
    public static final SaffronType[] emptyTypes = new SaffronType[0];
    public static final String[] emptyStrings = new String[0];

    final SaffronType unknownType;

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
    public SqlValidator(SqlOperatorTable opTab,
            CatalogReader catalogReader, SaffronTypeFactory typeFactory)
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
        }
        return new SqlNodeList(list, ParserPosition.ZERO);
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
                for (Iterator tableIter = tableNames.iterator();
                      tableIter.hasNext(); /* empty */) {
                    String tableName = (String) tableIter.next();
                    final SqlNode from = getChild(select,tableName);
                    final Scope fromScope = getScope(from);
                    assert fromScope != null;
                    final SaffronType rowType = fromScope.getRowType();
                    final SaffronField [] fields = rowType.getFields();
                    for (int i = 0; i < fields.length; i++) {
                        SaffronField field = fields[i];
                        String columnName = field.getName();
                        //todo: do real implicit collation here
                        final SqlNode exp = new SqlIdentifier(
                                new String [] { tableName,columnName },
                                ParserPosition.ZERO);
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
                    //todo: do real implicit collation here
                    final SqlIdentifier exp = new SqlIdentifier(new String [] { tableName,columnName },null);
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
            final SqlNodeList selectList = new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*",ParserPosition.ZERO));
            SqlSelect wrapperNode =
                SqlOperatorTable.std().selectOperator.createCall(
                    false,
                    selectList,
                    node,
                    null,
                    null,
                    null,
                    null,
                    ParserPosition.ZERO);
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
            final SqlNodeList selectList = new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*",null));
            SqlSelect wrapperNode =
                SqlOperatorTable.std().selectOperator.createCall(
                    false,
                    selectList,
                    query,
                    null,
                    null,
                    null,
                    orderList,null);
            return wrapperNode;
        } else if (node.isA(SqlKind.ExplicitTable)) {
            // (TABLE t) is equivalent to (SELECT * FROM t)
            SqlCall call = (SqlCall) node;
            final SqlNodeList selectList = new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*",null));
            SqlSelect wrapperNode =
                SqlOperatorTable.std().selectOperator.createCall(
                    false,
                    selectList,
                    call.getOperands()[0],
                    null,
                    null,
                    null,
                    null,null);
            return wrapperNode;
        } else if (node.isA(SqlKind.Insert)) {
            SqlInsert call = (SqlInsert) node;
            call.setOperand(SqlInsert.SOURCE_SELECT_OPERAND,call.getSource());
        } else if (node.isA(SqlKind.Delete)) {
            SqlDelete call = (SqlDelete) node;
            final SqlNodeList selectList = new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*",null));
            SqlSelect select =
                SqlOperatorTable.std().selectOperator.createCall(
                    false,
                    selectList,
                    call.getTargetTable(),
                    call.getCondition(),
                    null,
                    null,
                    null,null);
            call.setOperand(SqlDelete.SOURCE_SELECT_OPERAND,select);
        } else if (node.isA(SqlKind.Update)) {
            SqlUpdate call = (SqlUpdate) node;
            final SqlNodeList selectList = new SqlNodeList(ParserPosition.ZERO);
            selectList.add(new SqlIdentifier("*",null));
            Iterator iter = call.getSourceExpressionList().getList().iterator();
            int ordinal = 0;
            while (iter.hasNext()) {
                SqlNode exp = (SqlNode) iter.next();
                // Force unique aliases to avoid a duplicate for Y with
                // SET X=Y
                String alias = deriveAliasFromOrdinal(ordinal);
                selectList.add(
                    SqlOperatorTable.std().asOperator.createCall(
                        exp,new SqlIdentifier(alias,null),null));
                ++ordinal;
            }
            SqlSelect select =
                SqlOperatorTable.std().selectOperator.createCall(
                    false,
                    selectList,
                    call.getTargetTable(),
                    call.getCondition(),
                    null,
                    null,
                    null,null);
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

    //REVIEW wael 07/27/04: changed from private to public access
    public void setValidatedNodeType(SqlNode node,SaffronType type)
    {
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return;
        }
        nodeToTypeMap.put(node,type);
    }

    public static void checkCharsetAndCollateConsistentIfCharType(SaffronType type) {
        //(every charset must have a default collation)
        if (type.isCharType()) {
            Charset strCharset = type.getCharset();
            Charset colCharset = type.getCollation().getCharset();
            assert(null!=strCharset);
            assert(null!=colCharset);
            if (!strCharset.equals(colCharset)) {
                //todo: enable this checking when we have a charset to collation mapping
//                throw newValidationError(type.toString()+" was found to have charset="+strCharset.name()+
//                        " and a mismatched collation charset="+colCharset.name());
            }
        }
    }

    public SaffronType deriveType(Scope scope, SqlNode operand)
    {
        // if we already know the type, no need to re-derive
        SaffronType type = (SaffronType) nodeToTypeMap.get(operand);
        if (type != null) {
            return type;
        }
        //~ SqlIdentifier -----------------------------------------------------------
        if (operand instanceof SqlIdentifier) {
            // REVIEW klo 1-Jan-2004:
            // We should have assert scope != null here. The idea is we should figure
            // out the right scope for the SqlIdentifier before we call this method. Therefore, scope can't be
            // null.

            SqlIdentifier id = (SqlIdentifier) operand;

            // first check for reserved identifiers like CURRENT_USER
            type = contextVariableTable.deriveType(id);
            if (type != null) {

                // TODO jvs 26-May-2004: share code with other exit path
                // below.

                if (type.isCharType()) {
                    Charset charset = type.getCharset()==null?
                            Util.getDefaultCharset() :
                            type.getCharset();
                    SqlCollation collation = type.getCollation()==null?
                            new SqlCollation(SqlCollation.Coercibility.Implicit) :
                            type.getCollation();
                    //todo: should get the implicit collation from repository instead of null
                    type = typeFactory.createTypeWithCharsetAndCollation(type,
                            charset, collation);
                    checkCharsetAndCollateConsistentIfCharType(type);
                }
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
                        // this is a case where SqlNode parse position is
                        // definitely needed.  I took off the "in scope" part
                        // because the internal information is not wanted
                        // after I made this a non-internal error.
                        throw SaffronResource.instance().newUnknownIdentifier(name, id.getParserPosition().toString());
                    }
                } else {
                    SaffronType fieldType = lookupField(type,name);
                    if (fieldType == null) {
                        throw Util.newInternal("Could not find field '" +
                                name + "' in '" + type + "'");
                    }
                    type = fieldType;
                }
            }
            if (type.isCharType()) {
                Charset charset = type.getCharset()==null?
                        Util.getDefaultCharset() :
                        type.getCharset();
                SqlCollation collation = type.getCollation()==null?
                        new SqlCollation(SqlCollation.Coercibility.Implicit) :
                        type.getCollation();
                //todo: should get the implicit collation from repository instead of null
                type = typeFactory.createTypeWithCharsetAndCollation(type,
                        charset, collation);
                checkCharsetAndCollateConsistentIfCharType(type);
            }
            return type;
        }
        //~ SqlLiteral ----------------------------------------------------------------------
        if (operand instanceof SqlLiteral)
        {
            SqlLiteral literal = (SqlLiteral) operand;
            return literal.createSqlType(typeFactory);

        }
        //~ SqlDynamicParam ---------------------------------------------------
        if (operand instanceof SqlDynamicParam) {
            return unknownType;
        }
        // ~ SqlDataType - currently, the 2nd arg to a cast.
        if (operand instanceof SqlDataType) {
            SqlDataType dataType = (SqlDataType) operand;
            return dataType.deriveType(this);
        }
        //~ SqlCall -----------------------------------------------------------
        if (operand instanceof SqlCall) {
            SqlCall call = (SqlCall) operand;
            checkForIllegalNull(call);
            //~ SqlCaseOperator ---------
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
            //~ FunctionCall ---------
            if ((call.operator instanceof SqlFunction)
                || (call.operator instanceof SqlSpecialOperator)
                || (call.operator instanceof SqlRowOperator))
            {
                SqlCall node = (SqlCall) operand;
                SqlNode[] operands = node.getOperands();
                SaffronType[] argTypes = new SaffronType[operands.length];
                for (int i = 0; i < operands.length; ++i) {
                    // We can't derive a type for some operands.
                    if (operands[i] instanceof SqlSymbol) {
                            continue; // operand is a symbol e.g. LEADING
                    }
                    SaffronType nodeType = deriveType(scope, operands[i]);
                    setValidatedNodeType(operands[i], nodeType);
                    argTypes[i] = nodeType;
                }

                if (!(call.operator instanceof SqlJdbcFunctionCall)
                        && call.operator instanceof SqlFunction) {
                    SqlFunction function = opTab.lookupFunction(
                        call.operator.name, argTypes);
                    if (function == null) {
                        // todo: localize "Function"
                        List overloads =
                            opTab.lookupFunctionsByName(call.operator.name);
                        if (null == overloads ||
                            Collections.EMPTY_LIST == overloads)
                        {
                            throw SaffronResource.instance().newValidatorUnknownFunction(
                                call.operator.name,
                                call.getParserPosition().toString());
                        }
                        SqlFunction fun = (SqlFunction) overloads.get(0);
                        throw SaffronResource.instance().newInvalidNbrOfArgument(
                                call.operator.name,
                                call.getParserPosition().toString(),
                                (Integer) fun.getOperandsCountDescriptor().
                                    getPossibleNumOfOperands().get(0));
                    }
                    call.operator = function;
                }
                return call.operator.getType(this, scope, call);
            }
            //~ Unary and Binary Operators ---------
            if (call.operator instanceof SqlBinaryOperator ||
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
                type = call.operator.getType(this, scope, call);
                if (operand.isA(SqlKind.As)) {
                    return type;
                }

                //validate and determine coercibility and resulting collation name of binary operator if needed
                if (call.operator instanceof SqlBinaryOperator) {
                    SaffronType operandType1 = getValidatedNodeType(call.operands[0]);
                    SaffronType operandType2 = getValidatedNodeType(call.operands[1]);
                    if (null==operandType1 || null==operandType2) {
                        throw Util.newInternal("operands' types should have been derived");
                    }
                    if (operandType1.isCharType() && operandType2.isCharType()){
                        Charset cs1 = operandType1.getCharset();
                        Charset cs2 = operandType2.getCharset();
                        assert(null!=cs1 && null!=cs2) : "An implicit or explicit charset should have been set";
                        if (!cs1.equals(cs2)) {
                            throw SaffronResource.instance().newIncompatibleCharset(
                                    call.operator.name, cs1.name(),cs2.name());
                        }

                        SqlCollation col1 = operandType1.getCollation();
                        SqlCollation col2 = operandType2.getCollation();
                        assert(null!=col1 && null!=col2) :
                                "An implicit or explicit collation should have been set";
                        //validation will occur inside getCoercibilityDyadicOperator...


                        SqlCollation resultCol = SqlCollation.getCoercibilityDyadicOperator(col1,  col2);

                        if (type.isCharType()) {
                            type =
                             typeFactory.createTypeWithCharsetAndCollation(type
                                            ,type.getCharset()
                                            ,resultCol);
                        }
                    }
                }
                //determine coercibility and resulting collation name of unary operator if needed
                else if (type.isCharType()) {
                    SaffronType operandType = getValidatedNodeType(call.operands[0]);
                    if (null==operandType) {
                        throw Util.newInternal("operand's type should have been derived");
                    }
                    if (operandType.isCharType()){
                        SqlCollation collation = operandType.getCollation();
                        assert(null!=collation) : "An implicit or explicit collation should have been set";
                        type=typeFactory.createTypeWithCharsetAndCollation(type,
                                type.getCharset(),
                                new SqlCollation(collation.getCollationName(),
                                        collation.getCoercibility())
                        );
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

    private void inferUnknownTypes(
        SaffronType inferredType,Scope scope,SqlNode node)
    {

        if ((node instanceof SqlDynamicParam) ||
            SqlUtil.isNullLiteral(node, false)) {
            if (inferredType.equals(unknownType)
                ) {
                // TODO: scrape up some positional context information
                throw SaffronResource.instance().newNullIllegal();
            }
            // REVIEW:  should dynamic parameter types always be nullable?
            SaffronType newInferredType= typeFactory.createTypeWithNullability(inferredType,true);
            if (inferredType.isCharType()) {
                newInferredType=typeFactory.createTypeWithCharsetAndCollation(newInferredType,
                        inferredType.getCharset(),
                        inferredType.getCollation()
                );
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
            SaffronType returnType = deriveType(scope,node);

            List whenList = caseCall.getWhenOperands();
            for (int i = 0; i < whenList.size(); i++) {
                SqlNode sqlNode = (SqlNode) whenList.get(i);
                inferUnknownTypes(unknownType,scope,sqlNode);
            }
            List thenList = caseCall.getThenOperands();
            for (int i = 0; i < thenList.size(); i++) {
                SqlNode sqlNode = (SqlNode) thenList.get(i);
                inferUnknownTypes(returnType,scope,sqlNode);
            }

            if (!SqlUtil.isNullLiteral(caseCall.getElseOperand(), false)){
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

            checkForIllegalNull(call);
        }


    }

    private void checkForIllegalNull(SqlCall call) {
        if (!(call.isA(SqlKind.Case) ||
            call.isA(SqlKind.Cast) ||
            call.isA(SqlKind.Row) ||
            call.isA(SqlKind.Select))) {
            for (int i = 0; i < call.operands.length; i++) {
                //todo use pp when not null
                if (SqlUtil.isNullLiteral(call.operands[i],false)) {
//                    assert(null==node.getParserPosition()) : "todo use pp";
                    throw SaffronResource.instance().newNullIllegal();
                }
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
                    exp = SqlOperatorTable.std().asOperator.createCall(
                            exp,
                            new SqlIdentifier(alias,null),null);
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
    public String deriveAlias(SqlNode node,int ordinal)
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
        switch (node.getKind().getOrdinal()) {
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

        switch (node.getKind().getOrdinal()) {
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

    private void validateFrom(SqlNode node,SaffronType targetRowType)
    {
        switch (node.getKind().getOrdinal()) {
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


        SqlNodeList newSelectList = new SqlNodeList(expandedSelectItems,
                ParserPosition.ZERO);
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
            deriveType(scope, where);
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
                    throw SaffronResource.instance().newInvalidOrderByPos(select.getParserPosition().toString());
                }
            }
            validateExpression(orderList);
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
                throw SaffronResource.instance().newUnknownTargetColumn(id.getSimple(),id.getParserPosition().toString());
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
            throw SaffronResource.instance().newUnmatchInsertColumn(
                    ""+targetRowType.getFieldCount(),
                    ""+sourceRowType.getFieldCount());
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
        assert(node.isA(SqlKind.Values));
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

        // validate that all rows have the same number of columns
        //  and that expressions in each column are compatible.
        final int numOfRows = operands.length;
        if (numOfRows >= 2) {
            SqlCall firstRow = (SqlCall) operands[0];
            final int numOfColumns = firstRow.getOperands().length;

            // 1. check that all rows have the same cols length
            for (int row = 0; row < numOfRows; row++) {
                SqlCall thisRow = (SqlCall) operands[row];
                if (numOfColumns != thisRow.operands.length) {
                    throw SaffronResource.instance().newIncompatibleValueType(node.getParserPosition().toString());
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for (int col = 0; col < numOfColumns; col++) {
                SaffronType[] types = new SaffronType[numOfRows];
                for (int row = 0; row < numOfRows; row++) {
                    SqlCall thisRow = (SqlCall) operands[row];
                    types[row] =deriveType(
                        getScope(node), thisRow.operands[col]);
                }

                final SaffronType type = SqlOperatorTable.useNullableBiggest.
                                                    getType(typeFactory, types);

                if (null == type) {
                    throw SaffronResource.instance().newIncompatibleValueType(node.getParserPosition().toString());
                }
            }
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
                //todo: do implicit collation here
                return new SqlIdentifier(
                        new String [] { tableName,columnName },
                        ParserPosition.ZERO);
            }
            case 2: {
                final String tableName = identifier.names[0];
                final Scope fromScope = resolve(tableName, null, null);
                if (fromScope == null) {
                    throw SaffronResource.instance().newTableNameNotFound(
                            tableName,
                            identifier.getParserPosition().toString());
                }
                final String columnName = identifier.names[1];
                if (lookupField(fromScope.getRowType(), columnName) != null) {
                    return identifier; // it was fine already
                } else {
                    throw SaffronResource.instance().newColumnNotFoundInTable(
                            columnName, tableName);
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
                    throw SaffronResource.instance().newColNotFound(columnName);
                }
                return parent.findQualifyingTableName(columnName);
            case 1:
                return tableName;
            default:
                throw SaffronResource.instance().newColumnAmbiguous(columnName);
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
                throw SaffronResource.instance().newTableNameNotFound(""+id, id.getParserPosition().toString());
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
            switch (call.getKind().getOrdinal()) {
            case SqlKind.UnionORDINAL:
            case SqlKind.IntersectORDINAL:
            case SqlKind.ExceptORDINAL:
                for (int i = 0; i < call.operands.length; i++) {
                    SqlNode operand = call.operands[i];
                    if (!operand.getKind().isA(SqlKind.Query)) {
                        throw SaffronResource.instance().newNeedQueryOp(
                                ""+operand, operand.getParserPosition().toString());
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
