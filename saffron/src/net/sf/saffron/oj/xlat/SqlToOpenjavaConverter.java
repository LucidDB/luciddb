/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.oj.xlat;

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;
import net.sf.saffron.jdbc.SaffronJdbcConnection;
import net.sf.saffron.oj.stmt.OJStatement;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;
import openjava.ptree.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * A <code>SqlToOpenjavaConverter</code> converts a tree of {@link SqlNode}
 * objects a {@link openjava.ptree.ParseTree} tree.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 19, 2003
 */
public class SqlToOpenjavaConverter
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Maps names of {@link SqlBinaryOperator} to codes of
     * {@link BinaryExpression}, wrapped as {@link Integer}. For example,
     * <code>mapBinarySqlToOj.get("/")</code> returns
     * <code>Integer({@link BinaryExpression#DIVIDE}</code>.
     */
    static final HashMap mapBinarySqlToOj = new HashMap();
    /**
     * Inverse of {@link #mapBinarySqlToOj}.
     */
    static final HashMap mapBinaryOjToSql = new HashMap();
    static {
        initMaps();
    }

    //~ Instance fields -------------------------------------------------------

    private final SqlValidator validator;

    //~ Constructors ----------------------------------------------------------

    public SqlToOpenjavaConverter(SqlValidator validator)
    {
        this.validator = validator;
    }

    //~ Methods ---------------------------------------------------------------

    public Expression convertQuery(SqlNode query)
    {
        query = validator.validate(query);
        return convertQueryRecursive(query);
    }

    public Expression convertSelect(SqlSelect query)
    {
        final SqlNode from = query.getFrom();
        final SqlNodeList groupList = query.getGroup();
        final SqlNodeList orderList = query.getOrderList();
        final SqlNodeList selectList = query.getSelectList();
        final SqlNode where = query.getWhere();
        final SqlValidator.SelectScope selectScope = validator.getScope(query);
        final ExpressionList convertedSelectList =
            convertSelectList(selectScope,selectList);
        final ExpressionList convertedGroup =
            convertGroup(selectScope,groupList);
        final Expression convertedFrom = convertFrom(selectScope,from);
        final Expression convertedWhere =
            (where == null) ? null : convertExpression(selectScope,where);
        final ExpressionList convertedOrder =
            convertOrder(selectScope,orderList);
        QueryExpression queryExpression =
            new QueryExpression(
                convertedSelectList,
                true,
                convertedGroup,
                convertedFrom,
                convertedWhere,
                convertedOrder);
        if (query.isDistinct()) {
            throw new UnsupportedOperationException(
                "SELECT DISTINCT is not implemented"); // todo:
        }
        final SqlNode having = query.getHaving();
        if (having != null) {
            throw new UnsupportedOperationException(
                "HAVING is not implemented"); // todo:
        }
        return queryExpression;
    }

    /**
     * Standard method recognised by JUnit.
     */
    public static Test suite()
    {
        return new TestSuite(ConverterTest.class);
    }

    private Expression convertExpression(
        SqlValidator.Scope scope,
        SqlNode node)
    {
        final SqlNode [] operands;
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            operands = ((SqlCall) node).getOperands();
            return new AliasedExpression(
                convertExpression(scope,operands[0]),
                operands[1].toString());
        case SqlKind.IdentifierORDINAL:
            return convertIdentifier(scope,(SqlIdentifier) node);
        case SqlKind.LiteralORDINAL:
            return convertLiteral((SqlLiteral) node);
        default:
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                operands = call.getOperands();
                if (call.operator instanceof SqlBinaryOperator) {
                    final SqlBinaryOperator binop =
                        (SqlBinaryOperator) call.operator;
                    final Integer integer =
                        (Integer) mapBinarySqlToOj.get(binop.name);
                    if (integer == null) {
                        throw new UnsupportedOperationException(
                            "unknown binary operator " + binop);
                    }
                    int op = integer.intValue();
                    return new BinaryExpression(
                        convertExpression(scope,operands[0]),
                        op,
                        convertExpression(scope,operands[1]));
                } else if (call.operator instanceof SqlFunction) {
                    throw new UnsupportedOperationException("todo:" + node);
                } else if (call.operator instanceof SqlPrefixOperator) {
                    throw new UnsupportedOperationException("todo:" + node);
                } else if (call.operator instanceof SqlPostfixOperator) {
                    throw new UnsupportedOperationException("todo:" + node);
                } else {
                    throw new UnsupportedOperationException("todo:" + node);
                }
            } else {
                throw new UnsupportedOperationException("todo:" + node);
            }
        }
    }

    private Expression convertFrom(SqlValidator.Scope scope,SqlNode from)
    {
        switch (from.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            final SqlNode [] operands = ((SqlCall) from).getOperands();
            return new AliasedExpression(
                convertFrom(scope,operands[0]),
                operands[1].toString());
        case SqlKind.IdentifierORDINAL:
            Expression e = new Variable(OJStatement.connectionVariable);
            final SqlIdentifier id = (SqlIdentifier) from;
            String schemaName = null;
            String tableName;
            if (id.names.length == 1) {
                tableName = id.names[0];
            } else if (id.names.length == 2) {
                schemaName = id.names[0];
                tableName = id.names[1];
            } else {
                throw Util.newInternal("improperly qualified id:  " + id);
            }
            return new TableReference(e,schemaName,tableName);
        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            SqlNode condition = join.getCondition();
            boolean isNatural = join.isNatural();
            SqlJoinOperator.JoinType joinType = join.getJoinType();
            SqlJoinOperator.ConditionType conditionType = join.getConditionType();
            Expression leftExp = convertFrom(scope,left);
            Expression rightExp = convertFrom(scope,right);
            Expression conditionExp = null;
            int convertedJoinType = convertJoinType(joinType);
            if (isNatural) {
                throw new UnsupportedOperationException(
                    "todo: implement natural join");
            }
            if (condition != null) {
                switch (conditionType.getOrdinal()) {
                case SqlJoinOperator.ConditionType.On_ORDINAL:
                    conditionExp = convertExpression(scope,condition);
                    break;
                case SqlJoinOperator.ConditionType.Using_ORDINAL:
                    SqlNodeList list = (SqlNodeList) condition;
                    for (int i = 0; i < list.size(); i++) {
                        final SqlNode columnName = list.get(i);
                        assert(columnName instanceof SqlIdentifier);
                        Expression exp = convertExpression(scope,columnName);
                        if (i == 0) {
                            conditionExp = exp;
                        } else {
                            conditionExp =
                                new BinaryExpression(
                                    conditionExp,
                                    BinaryExpression.LOGICAL_AND,
                                    exp);
                        }
                    }
                default:
                    throw conditionType.unexpected();
                }
            }
            if (conditionExp == null) {
                conditionExp = Literal.makeLiteral(true);
            }
            if (convertedJoinType == JoinRel.JoinType.RIGHT) {
                // "class Join" does not support RIGHT, so swap...
                return new JoinExpression(
                    rightExp,
                    leftExp,
                    JoinRel.JoinType.LEFT,
                    conditionExp);
            } else {
                return new JoinExpression(
                    leftExp,
                    rightExp,
                    convertedJoinType,
                    conditionExp);
            }
        case SqlKind.SelectORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            return convertQueryRecursive(from);
        case SqlKind.ValuesORDINAL:
            return convertValues(scope,(SqlCall) from);
        default:
            throw Util.newInternal("not a join operator " + from);
        }
    }

    private static int convertJoinType(SqlJoinOperator.JoinType joinType)
    {
        switch (joinType.getOrdinal()) {
        case SqlJoinOperator.JoinType.Comma_ORDINAL:
        case SqlJoinOperator.JoinType.Inner_ORDINAL:
        case SqlJoinOperator.JoinType.Cross_ORDINAL:
            return JoinRel.JoinType.INNER;
        case SqlJoinOperator.JoinType.Full_ORDINAL:
            return JoinRel.JoinType.FULL;
        case SqlJoinOperator.JoinType.Left_ORDINAL:
            return JoinRel.JoinType.LEFT;
        case SqlJoinOperator.JoinType.Right_ORDINAL:
            return JoinRel.JoinType.RIGHT;
        default:
            throw joinType.unexpected();
        }
    }

    private ExpressionList convertGroup(
        SqlValidator.Scope scope,
        SqlNodeList groupList)
    {
        if (groupList == null) {
            return null;
        }
        ExpressionList list = new ExpressionList();
        for (int i = 0; i < groupList.size(); i++) {
            Expression expression = convertExpression(scope,groupList.get(i));
            list.add(expression);
        }
        return list;
    }

    private Expression convertLiteral(final SqlLiteral literal)
    {
        final Object value = literal.getValue();
        switch (literal._typeName.ordinal_) {
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            BigDecimal bd = (BigDecimal) value;
            // Convert to integer if possible.
            if (bd.scale() == 0) {
                int i = bd.intValue();
                return Literal.makeLiteral(i);
            } else {
                // TODO:  preserve fixed-point precision and large integers
                return Literal.makeLiteral(bd.doubleValue());
            }
        case SqlTypeName.Char_ordinal:
            NlsString nlsStr = (NlsString) value;
            return Literal.makeLiteral(nlsStr.getValue());
        case SqlTypeName.Boolean_ordinal:
            if (value != null) {
                return Literal.makeLiteral((Boolean) value);
            }
            // fall through to handle UNKNOWN (the boolean NULL value)
        case SqlTypeName.Null_ordinal:
            return Literal.constantNull();
        default:
            throw literal._typeName.unexpected();
        }
    }

    private ExpressionList convertOrder(
        SqlValidator.Scope scope,
        SqlNodeList orderList)
    {
        if (orderList == null) {
            return null;
        }
        ExpressionList result = new ExpressionList();
        for (int i = 0; i < orderList.size(); i++) {
            SqlNode order = orderList.get(i);
            result.add(convertExpression(scope,order));
        }
        return result;
    }

    private Expression convertQueryRecursive(SqlNode query)
    {
        if (query instanceof SqlSelect) {
            return convertSelect((SqlSelect) query);
        } else if (query instanceof SqlCall) {
            final SqlCall call = (SqlCall) query;
            int op;
            final SqlKind kind = call.getKind();
            switch (kind.getOrdinal()) {
            case SqlKind.UnionORDINAL:
                op = BinaryExpression.UNION;
                break;
            case SqlKind.IntersectORDINAL:
                op = BinaryExpression.INTERSECT;
                break;
            case SqlKind.ExceptORDINAL:
                op = BinaryExpression.EXCEPT;
                break;
            default:
                throw kind.unexpected();
            }
            final SqlNode [] operands = call.getOperands();
            final Expression left = convertQueryRecursive(operands[0]);
            final Expression right = convertQueryRecursive(operands[1]);
            return new BinaryExpression(left,op,right);
        } else {
            throw Util.newInternal("not a query: " + query);
        }
    }

    private static void initMaps()
    {
        addBinary("/", BinaryExpression.DIVIDE);
        addBinary("=", BinaryExpression.EQUAL);
        addBinary(">", BinaryExpression.GREATER);
        addBinary(">=", BinaryExpression.GREATEREQUAL);
        addBinary("<", BinaryExpression.LESS);
        addBinary("<=", BinaryExpression.LESSEQUAL);
        addBinary("AND", BinaryExpression.LOGICAL_AND);
        addBinary("OR", BinaryExpression.LOGICAL_OR);
        addBinary("-", BinaryExpression.MINUS);
        addBinary("*", BinaryExpression.NOTEQUAL);
        addBinary("+", BinaryExpression.PLUS);
        addBinary("*", BinaryExpression.TIMES);
    }

    private static void addBinary(String name, int code) {
        mapBinarySqlToOj.put(name,new Integer(code));
        mapBinaryOjToSql.put(new Integer(code),name);
    }

    private static HashMap createPrefixMap()
    {
        HashMap map = new HashMap();
        map.put("NOT",new Integer(UnaryExpression.NOT));
        map.put("-",new Integer(UnaryExpression.MINUS));
        map.put("+",new Integer(UnaryExpression.PLUS));
        return map;
    }

    private static HashMap createPostfixMap()
    {
        HashMap map = new HashMap();
        return map;
    }

    private static HashMap createFunctionMap()
    {
        HashMap map = new HashMap();
        return map;
    }

    /**
     * Converts an identifier into an expression in a given scope. For
     * example, the "empno" in "select empno from emp join dept" becomes
     * "emp.empno".
     */
    private Expression convertIdentifier(
        SqlValidator.Scope scope,
        SqlIdentifier identifier)
    {
        identifier = scope.fullyQualify(identifier);
        Expression e = new Variable(identifier.names[0]);
        for (int i = 1; i < identifier.names.length; i++) {
            String name = identifier.names[i];
            e = new FieldAccess(e,name);
        }
        return e;
    }

    private QueryExpression convertRowConstructor(
        SqlValidator.Scope scope,
        SqlCall rowConstructor)
    {
        final SqlNode [] operands = rowConstructor.getOperands();
        ExpressionList selectList = new ExpressionList();
        for (int i = 0; i < operands.length; ++i) {
            // TODO:  when column-aliases are supported and provided, use them
            Expression value = convertExpression(scope,operands[i]);
            Expression alias =
                new AliasedExpression(value,scope.deriveAlias(operands[i],i));
            selectList.add(alias);
        }

        // SELECT value-list FROM onerow
        return new QueryExpression(selectList,true,null,null,null,null);
    }

    private ExpressionList convertSelectList(
        SqlValidator.SelectScope scope,
        SqlNodeList selectList)
    {
        ExpressionList list = new ExpressionList();
        selectList = validator.expandStar(selectList,scope.select);
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode node = selectList.get(i);
            Expression expression = convertExpression(scope,node);
            list.add(expression);
        }
        return list;
    }

    private Expression convertValues(SqlValidator.Scope scope,SqlCall values)
    {
        SqlNodeList rowConstructorList =
            (SqlNodeList) (values.getOperands()[0]);
        Expression expr = null;

        // NOTE jvs 10-Sept-2003: If there are multiple rows, we will build a
        // left-deep UNION ALL tree with the first row left-most.  It might be
        // a little more natural to generate a right-deep tree instead.
        // FIXME jvs 10-Sept-2003:  The multi-row stuff doesn't actually work
        // yet, so it's disabled in the validator.  For one thing, the
        // expression below should be a UNION ALL, not a UNION.  But even this
        // way, Saffron validation or codegen fails depending on how it's
        // used.  Single-row is fine.
        Iterator iter = rowConstructorList.getList().iterator();
        while (iter.hasNext()) {
            SqlCall rowConstructor = (SqlCall) iter.next();
            QueryExpression queryExpr =
                convertRowConstructor(scope,rowConstructor);
            if (expr == null) {
                expr = queryExpr;
            } else {
                expr =
                    new BinaryExpression(
                        expr,
                        BinaryExpression.UNION,
                        queryExpr);
            }
        }

        SqlValidator.Scope valuesScope = scope.getScopeFromNode(values);

        // TODO:  don't do this if query already supplied a non-generated alias
        return new AliasedExpression(expr,valuesScope.getAlias());
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Unit test for <code>SqlToOpenjavaConverter</code>.
     */
    public static class ConverterTest extends TestCase
    {
        public void testConvert()
        {
            check("select 1 from \"emps\"");
        }

        public void testOrder(TestCase test)
        {
            check("select * from \"emps\" order by \"empno\" asc, \"salary\", \"deptno\" desc");
        }

        private void check(String s)
        {
            TestContext testContext = TestContext.instance();
            final SqlNode sqlQuery;
            try {
                sqlQuery = new SqlParser(s).parseQuery();
            } catch (ParseException e) {
                throw new AssertionFailedError(e.toString());
            }
            final SqlValidator validator =
                    new SqlValidator(SqlOperatorTable.instance(),
                            testContext.seeker,
                            testContext.schema.getTypeFactory());
            final SqlToOpenjavaConverter converter =
                new SqlToOpenjavaConverter(validator);
            final Expression expression = converter.convertQuery(sqlQuery);
            assertTrue(expression != null);
        }
    }

    /**
     * A <code>SchemaCatalogReader</code> looks up catalog information from a
     * {@link RelOptSchema saffron schema object}.
     */
    public static class SchemaCatalogReader
        implements SqlValidator.CatalogReader
    {
        private final RelOptSchema schema;
        private final boolean upperCase;

        public SchemaCatalogReader(RelOptSchema schema,boolean upperCase)
        {
            this.schema = schema;
            this.upperCase = upperCase;
        }

        public SqlValidator.Table getTable(String [] names)
        {
            if (names.length != 1) {
                return null;
            }
            final RelOptTable table =
                schema.getTableForMember(new String[]{maybeUpper(names[0])});
            if (table != null) {
                return new SqlValidator.Table() {
                    public RelDataType getRowType() {
                        return table.getRowType();
                    }
                    public String [] getQualifiedName() {
                        return null;
                    }

                        public List getColumnNames()
                        {
                            final RelDataType rowType = table.getRowType();
                            final RelDataTypeField [] fields = rowType.getFields();
                            ArrayList list = new ArrayList();
                            for (int i = 0; i < fields.length; i++) {
                                RelDataTypeField field = fields[i];
                                list.add(maybeUpper(field.getName()));
                            }
                            return list;
                        }
                    };
            }
            return null;
        }

        private String maybeUpper(String s)
        {
            return upperCase ? s.toUpperCase() : s;
        }
    }

    static class TestContext
    {
        private final SqlValidator.CatalogReader seeker;
        private final Connection jdbcConnection;
        private final RelOptConnection connection;
        private final RelOptSchema schema;
        private static TestContext instance;

        TestContext()
        {
            try {
                Class.forName("net.sf.saffron.jdbc.SaffronJdbcDriver");
            } catch (ClassNotFoundException e) {
                throw Util.newInternal(e, "Error loading JDBC driver");
            }
            try {
                jdbcConnection =
                    DriverManager.getConnection(
                        "jdbc:saffron:schema=sales.SalesInMemory");
            } catch (SQLException e) {
                throw Util.newInternal(e);
            }
            connection = ((SaffronJdbcConnection) jdbcConnection).saffronConnection;
            schema = connection.getRelOptSchema();
            seeker = new SchemaCatalogReader(schema,false);
            OJStatement.setupFactories();
        }

        static TestContext instance()
        {
            if (instance == null) {
                instance = new TestContext();
            }
            return instance;
        }
    }
}


// End SqlToOpenjavaConverter.java
