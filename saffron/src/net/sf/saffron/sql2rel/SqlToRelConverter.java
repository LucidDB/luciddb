/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.sql2rel;

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sf.saffron.core.*;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.opt.VolcanoQuery;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.type.*;
import net.sf.saffron.util.Util;
import openjava.mop.Environment;

import java.math.BigInteger;
import java.util.*;

/**
 * Converts a SQL parse tree (consisting of {@link net.sf.saffron.sql.SqlNode}
 * objects) into a relational algebra expression (consisting of
 * {@link net.sf.saffron.rel.SaffronRel} objects).
 *
 * <p>This class accomplishes in one step what you could accomplish in two steps
 * using a {@link net.sf.saffron.oj.xlat.SqlToOpenjavaConverter} followed by a
 * {@link net.sf.saffron.oj.xlat.OJQueryExpander}.</p>
 *
 * @author jhyde
 * @since Oct 10, 2003
 * @version $Id$
 **/
public class SqlToRelConverter {
    //~ Static fields/initializers --------------------------------------------

    /** Maps an {@link String operator name} to an {@link RexKind operator}.
     * For example, binaryMap.get("/") yields {@link RexKind#Divide}. */
    private static final HashMap binaryMap = createBinaryMap();

    /** Maps an {@link String operator name} to an {@link RexKind operator}.
     * For example, binaryMap.get("-") yields {@link RexKind#MinusPrefix}. */
    private static final HashMap prefixMap = createPrefixMap();

    /** Maps an {@link String operator name} to an {@link RexKind operator}. */
    private static final HashMap postfixMap = createPostfixMap();

    /** Maps a {@link String function name} to an {@link RexKind operator}. */
    private static final HashMap functionMap = createFunctionMap();

    //~ Instance fields -------------------------------------------------------

    private final SqlValidator validator;
    private final SqlOperatorTable opTab;
    private final SqlFunctionTable funTab;
    private RexBuilder rexBuilder;
    private SaffronConnection connection;
    private SaffronSchema schema;
    private VolcanoCluster cluster;
    private HashMap mapScopeToRel = new HashMap();
    private DefaultValueFactory defaultValueFactory;
    final ArrayList leaves = new ArrayList();

    private List dynamicParamSqlNodes;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a converter
     *
     * @pre connection != null
     * @param validator
     * @param schema
     * @param env
     * @param connection
     * @param rexBuilder
     */
    public SqlToRelConverter(
        SqlValidator validator,
        SaffronSchema schema,
        Environment env,
        SaffronConnection connection,
        RexBuilder rexBuilder)
    {
        Util.pre(connection != null, "connection != null");
        this.validator = validator;
        opTab = SqlOperatorTable.instance();
        funTab = SqlFunctionTable.instance();
        this.schema = schema;
        this.connection = connection;
        this.defaultValueFactory = new NullDefaultValueFactory();
        this.rexBuilder = rexBuilder;

        this.cluster = createCluster(env);

        dynamicParamSqlNodes = new ArrayList();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the row-expression builder.
     */
    public RexBuilder getRexBuilder()
    {
        return rexBuilder;
    }

    /**
     * Returns the number of dynamic parameters encountered during translation;
     * this must only be called after convertQuery.
     *
     * @return number of dynamic parameters
     */
    public int getDynamicParamCount()
    {
        return dynamicParamSqlNodes.size();
    }

    /**
     * Returns the type inferred for a dynamic parameter.
     *
     * @param index 0-based index of dynamic parameter
     *
     * @return inferred type
     */
    public SaffronType getDynamicParamType(int index)
    {
        SqlNode sqlNode = (SqlNode) dynamicParamSqlNodes.get(index);
        if (sqlNode == null) {
            throw Util.needToImplement("dynamic param type inference");
        }
        return validator.getValidatedNodeType(sqlNode);
    }

    /**
     * Set a new DefaultValueFactory.  To have any effect, this must be called
     * before any convert method.
     *
     * @param factory new DefaultValueFactory
     */
    public void setDefaultValueFactory(DefaultValueFactory factory)
    {
        defaultValueFactory = factory;
    }

    /**
     * Converts an unvalidated query's parse tree into a relational expression.
     */
    public SaffronRel convertQuery(SqlNode query)
    {
        query = validator.validate(query);
        return convertQueryRecursive(query);
    }

    /**
     * Converts a validated query's parse tree into a relational expression.
     */
    public SaffronRel convertValidatedQuery(SqlNode query)
    {
        return convertQueryRecursive(query);
    }

    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public SaffronRel convertSelect(SqlSelect query)
    {
        final SqlValidator.SelectScope selectScope = validator.getScope(query);
        final Blackboard bb = new Blackboard(selectScope);
        convertFrom(bb,query.getFrom());
        convertWhere(bb, query.getWhere());
        convertGroup(bb, query.getGroup());
        convertHaving(bb, query.getHaving());
        convertSelectList(bb,query.getSelectList());
        if (query.isDistinct()) {
            bb.setRoot(new DistinctRel(cluster,bb.root));
        }
        convertOrder(bb,query.getOrderList());
        leaves.add(bb.root);
        mapScopeToRel.put(selectScope,bb.root);
        return bb.root;
    }

    private void convertHaving(final Blackboard bb, SqlNode having) {
        if (having == null) {
            return;
        }
        throw Util.needToImplement("HAVING"); // todo:
    }

    private void convertWhere(final Blackboard bb,final SqlNode where) {
        if (where == null) {
            return;
        }
        replaceSubqueries(bb, where);
        final RexNode convertedWhere = convertExpression(bb,where);
        bb.setRoot(new FilterRel(cluster,bb.root,convertedWhere));
    }

    private void replaceSubqueries(final Blackboard bb, final SqlNode expr) {
        findSubqueries(bb,expr);
        substituteSubqueries(bb);
    }

    private void substituteSubqueries(Blackboard bb) {
        for (int i = 0; i < bb.subqueries.size(); i++) {
            SqlNode node = (SqlNode) bb.subqueries.get(i);
            final RexNode expr = (RexNode) bb.mapSubqueryToExpr.get(node);
            if (expr == null) {
                SaffronRel converted;
                switch (node.getKind().ordinal_) {
                case SqlKind.InORDINAL: {
                    // "select from emp where emp.deptno in (Q)"
                    //
                    // is equivalent to
                    //
                    // "select from emp where exists (
                    //   select from (Q) where emp.deptno = {row})"
                    // becomes
                    // "select from emp join (select *, TRUE from (Q)) q
                    //   on emp.deptno = q.col1
                    //   where q.t is not null
                    SqlCall call = (SqlCall) node;
                    final SqlNode [] operands = call.getOperands();
                    SqlNode condition = operands[0];
                    final SqlNode seek = operands[1];
                    converted = convertExists(
                        bb, seek, condition,
                        rexBuilder.makeLiteral(true), "$indicator");
                    break;
                }
                case SqlKind.ExistsORDINAL: {
                    // "select from emp where exists (Q)"
                    // becomes
                    // "select from emp join (select *, TRUE from (Q)) q
                    //   on emp.deptno = q.col1
                    //   where q.t is not null
                    SqlCall call = (SqlCall) node;
                    SqlSelect select = (SqlSelect) call.getOperands()[0];
                    converted = convertExists(
                            bb, select, null, rexBuilder.makeLiteral(true),
                            "$indicator");
                    break;
                }
                case SqlKind.SelectORDINAL:
                    // "select empno, (Q) from emp"
                    //   becomes
                    // "select empno, q.c1 from emp left join (Q) as q"
                    converted = convertExists(bb, node, null, null, "foo");
                    break;
                default:
                    throw Util.newInternal("unexpected kind of subquery :" +
                            node);
                }
                final RexNode expression = bb.register(converted);
                bb.mapSubqueryToExpr.put(node, expression);
            }
        }
    }

    /**
     * Converts a query into a join with an indicator variable. The result is
     * a relational expression which outer joins a boolean condition column
     * to the original query. After performing the outer join, the condition
     * will be TRUE if the EXISTS condition holds, NULL otherwise.
     *
     * @param bb
     * @param seek A query, for example 'select * from emp' or 'values (1,2,3)'
     *   or '('Foo', 34)'.
     * @param condition May be null, or a node, or a node list
     * @param extraExpr Column expression to add. "TRUE" for EXISTS and IN
     * @param extraName Name of expression to add.
     * @return relational expression which outer joins a boolean condition
     *   column
     * @pre extraExpr == null || extraName != null
     */
    private SaffronRel convertExists(Blackboard bb, SqlNode seek,
            SqlNode condition, RexLiteral extraExpr, String extraName) {
        assert extraExpr == null || extraName != null :
                "precondition: extraExpr == null || extraName != null";
        SaffronRel converted = convertQueryOrInList(bb, seek);
        if (condition != null) {
            // We are translating an IN clause, so add a condition.
            RexNode conditionExp = null;
            final RexNode ref = rexBuilder.makeRangeReference(bb.root.getRowType(), 0);
            if (condition instanceof SqlNodeList) {
                // If "seek" is "(emp,dept)", generate the condition "emp = Q.c1
                // and dept = Q.c2".
                SqlNodeList conditionList = (SqlNodeList) condition;
                for (int i = 0; i < conditionList.size(); i++) {
                    SqlNode conditionNode = conditionList.get(i);
                    RexNode e = rexBuilder.makeCall(rexBuilder.operatorTable.equalsOperator,
                            convertExpression(bb, conditionNode),
                            rexBuilder.makeFieldAccess(ref, i));
                    if (i == 0) {
                        conditionExp = e;
                    } else {
                        conditionExp = rexBuilder.makeCall(rexBuilder.operatorTable.andOperator, conditionExp,
                                e);
                    }
                }
            } else {
                // If "seek" is "emp", generate the condition "emp = Q.c1". The
                // query must have precisely one column.
                assert converted.getRowType().getFieldCount() == 1;
                conditionExp = rexBuilder.makeCall(rexBuilder.operatorTable.equalsOperator,
                        convertExpression(bb, condition),
                        rexBuilder.makeFieldAccess(ref, 0));
            }
            converted = new FilterRel(cluster, converted, conditionExp);
        }
        if (extraExpr != null) {
            final SaffronType rowType = converted.getRowType();
            final SaffronField [] fields = rowType.getFields();
            final RexNode[] expressions = new RexNode[fields.length + 1];
            String[] fieldNames = new String[fields.length + 1];
            final RexNode ref = rexBuilder.makeRangeReference(rowType, 0);
            for (int j = 0; j < fields.length; j++) {
                expressions[j] = rexBuilder.makeFieldAccess(ref, j);
                fieldNames[j] = fields[j].getName();
            }
            expressions[fields.length] = extraExpr;
            fieldNames[fields.length] = uniqueFieldName(fieldNames,
                    fields.length, extraName);
            converted = new ProjectRel(cluster, converted,
                    expressions, fieldNames, ProjectRelBase.Flags.Boxed);
        }
        return converted;
    }

    private SaffronRel convertQueryOrInList(Blackboard bb, SqlNode seek) {
        // NOTE: Once we start accepting single-row queries as row
        // constructors, there will be an ambiguity here for a case like X IN
        // ((SELECT Y FROM Z)).  The SQL standard resolves the ambiguity by
        // saying that a lone select should be interpreted as a table
        // expression, not a row expression.  The semantic difference is that a
        // table expression can return multiple rows.

        if (seek instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) seek;
            SaffronRel[] inputs = new SaffronRel[list.size()];
            for (int i = 0; i < list.size(); i++) {
                SqlNode node = list.get(i);
                SqlCall call;
                if (isRowConstructor(node)) {
                    call = (SqlCall) node;
                } else {
                    // convert "1" to "row(1)"
                    call = validator.opTab.rowConstructor.createCall(
                            new SqlNode[] {node});
                }
                inputs[i] = convertRowConstructor(bb, call);
            }
            UnionRel unionRel = new UnionRel(cluster, inputs, true);
            leaves.add(unionRel);
            return unionRel;
        } else {
            return convertQueryRecursive(seek);
        }
    }

    private boolean isRowConstructor(SqlNode node)
    {
        if (!node.isA(SqlKind.Row)) {
            return false;
        }
        SqlCall call = (SqlCall) node;
        return call.operator.name.equalsIgnoreCase("row");
    }

    /**
     * Generates a unique name
     *
     * @param names  Array of existing names
     * @param length Number of existing names
     * @param s Suggested name
     * @return Name which does not match any of the names in the first
     *   <code>length</code> positions of the <code>names</code> array.
     */
    private static String uniqueFieldName(String[] names, int length, String s)
    {
        if (!contains(names,length,s)) {
            return s;
        }
        int n = length;
        while (true) {
            s = "EXPR_" + n;
            if (!contains(names,length,s)) {
                return s;
            }
            // FIXME jvs 15-Nov-2003:  If we ever get here, it's an infinite
            // loop; should be ++n?
        }
    }

    private static boolean contains(String[] names, int length, String s) {
        for (int i = 0; i < length; i++) {
             if (names[i].equals(s)) {
                 return true;
             }
        }
        return false;
    }

    /**
     * Builds a list of all <code>IN</code> or <code>EXISTS</code> operators
     * inside SQL parse tree. Does not traverse inside queries.
     */
    private void findSubqueries(Blackboard bb, SqlNode node) {
        switch (node.getKind().ordinal_) {
        case SqlKind.InORDINAL:
        case SqlKind.ExistsORDINAL:
        case SqlKind.SelectORDINAL:
            bb.registerSubquery(node);
            return;
        default:
            if (node instanceof SqlCall) {
                final SqlNode [] operands = ((SqlCall) node).getOperands();
                for (int i = 0; i < operands.length; i++) {
                    SqlNode operand = operands[i];
                    findSubqueries(bb, operand);
                }
            } else if (node instanceof SqlNodeList) {
                final SqlNodeList nodes = (SqlNodeList) node;
                for (int i = 0; i < nodes.size(); i++) {
                    SqlNode child = nodes.get(i);
                    findSubqueries(bb, child);
                }
            }
        }
    }

    /**
     * Standard method recognised by JUnit.
     */
    public static Test suite()
    {
        return new TestSuite(ConverterTest.class);
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     * @param bb Workspace
     * @param node Expression to translate
     * @return Converted expression
     */
    public RexNode convertExpression(Blackboard bb, SqlNode node)
    {
        final SqlNode [] operands;
        switch (node.getKind().ordinal_) {
        case SqlKind.AsORDINAL:
            operands = ((SqlCall) node).getOperands();
            return convertExpression(bb,operands[0]);
        case SqlKind.IdentifierORDINAL:
            return convertIdentifier(bb,(SqlIdentifier) node);
        case SqlKind.LiteralORDINAL:
            return convertLiteral((SqlLiteral) node);
        case SqlKind.DynamicParamORDINAL:
            return convertDynamicParam((SqlDynamicParam) node);
        case SqlKind.SelectORDINAL:
        case SqlKind.InORDINAL:
        case SqlKind.ExistsORDINAL:
            final RexNode expr = (RexNode) bb.mapSubqueryToExpr.get(node);
            assert expr != null : "expr != null";
            // The indicator column is the last field of the subquery.
            final int fieldCount = expr.getType().getFieldCount();
            return rexBuilder.makeFieldAccess(expr, fieldCount - 1);
        default:
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                operands = call.getOperands();
                if (call.operator instanceof SqlBinaryOperator) {
                    final SqlBinaryOperator op = (SqlBinaryOperator) call.operator;
//                    final RexKind opCode = (RexKind) binaryMap.get(op.name);
//                    if (opCode == null) {
//                        assert !call.isA(SqlNode.Kind.In) :
//                                "IN should have been handled already";
//                        throw Util.needToImplement(
//                            "binary operator " + op.name);
//                    }
                    return rexBuilder.makeCall(
                            op, convertExpression(bb,operands[0]),
                            convertExpression(bb,operands[1]));
                } else if (call.operator instanceof SqlFunction) {
                    if (call.operator.equals(funTab.characterLengthFunc)) {
                        //todo: solve aliases in a more elegent way.
                        call.operator = funTab.charLengthFunc;
                    }
                    return rexBuilder.makeCall(call.operator, convertExpressionList(bb,operands));
                } else if (call.operator instanceof SqlPrefixOperator ||
                        call.operator instanceof SqlPostfixOperator) {
                    final RexNode exp = convertExpression(bb, operands[0]);
                    SqlOperator op;
                    op = call.operator;
                    if (op.name.equals("+")) {
                        // Unary "+" has no effect. There is no
                        // corresponding Rex operator.
                        return exp;
                    }
                    return rexBuilder.makeCall(op, new RexNode[]{exp});
                } else if (call.operator instanceof SqlCaseOperator) {
                    return convertCase(bb, (SqlCase) call);
                }

                else {
                    throw Util.needToImplement(node);
                }
            } else {
                throw Util.needToImplement(node);
            }
        }
    }

    private RexNode convertCase(Blackboard bb, SqlCase call) {
        List whenList = call.getWhenOperands();
        List thenList = call.getThenOperands();
        RexNode[] whenThenElseRex = new RexNode[whenList.size()*2+1];
        assert(whenList.size()==thenList.size());



        for (int i = 0; i < whenList.size(); i++) {
            whenThenElseRex[i*2] = convertExpression(bb, (SqlNode) whenList.get(i));
            whenThenElseRex[i*2+1] = convertExpression(bb, (SqlNode) thenList.get(i));
        }
        whenThenElseRex[whenThenElseRex.length-1] = convertExpression(bb, call.getElseOperand());
        return rexBuilder.makeCall(call.operator, whenThenElseRex); //REVIEW 16-March-2004 wael: is there a better way?
    }


    private RexNode[] convertExpressionList(Blackboard bb, SqlNode[] nodes)
    {
        final RexNode[] exps = new RexNode[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            SqlNode node = nodes[i];
            exps[i] = convertExpression(bb, node);
        }
        return exps;
    }

    /**
     * Converts a FROM clause into a relational expression.
     *
     * @param bb Scope within which to resolve identifiers
     * @param from  FROM clause of a query. Examples include:
     *    a single table ("SALES.EMP"),
     *    an aliased table ("EMP AS E"),
     *    a list of tables ("EMP, DEPT"),
     *    an ANSI Join expression ("EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"),
     *    a VALUES clause ("VALUES ('Fred', 20)"),
     *    a query ("(SELECT * FROM EMP WHERE GENDER = 'F')"),
     *    or any combination of the above.
     * @post return != null
     */
    private void convertFrom(Blackboard bb,SqlNode from)
    {
        switch (from.getKind().ordinal_) {
        case SqlKind.AsORDINAL:
            final SqlNode [] operands = ((SqlCall) from).getOperands();
            convertFrom(bb,operands[0]);
            return;
        case SqlKind.IdentifierORDINAL:
            final SqlValidator.Scope fromScope = validator.getScope(from);
            SaffronTable table = getSaffronTable(fromScope, schema);
            bb.setRoot(table.toRel(cluster, connection));
            // REVIEW jvs 22-Jan-2004: This is adding a SqlNode as a
            // mapScopeToRel key.  Shouldn't it be a scope instead?
            mapScopeToRel.put(from,bb.root);
            leaves.add(bb.root);
            return;
        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            boolean isNatural = join.isNatural();
            SqlJoinOperator.JoinType joinType = join.getJoinType();
            final Blackboard
                leftBlackboard = new Blackboard(validator.getScope(left)),
                rightBlackboard = new Blackboard(validator.getScope(right));
            convertFrom(leftBlackboard,left);
            SaffronRel leftRel = leftBlackboard.root;
            convertFrom(rightBlackboard,right);
            SaffronRel rightRel = rightBlackboard.root;
            if (isNatural) {
                throw Util.needToImplement("natural join");
            }
            int convertedJoinType = convertJoinType(joinType);
            if (convertedJoinType == JoinRel.JoinType.RIGHT) {
                // "class Join" does not support RIGHT, so swap...
                bb.setRoot(createJoin(bb, rightRel, leftRel,
                        join.getCondition(), join.getConditionType(),
                        JoinRel.JoinType.LEFT));
            } else {
                bb.setRoot(createJoin(bb, leftRel, rightRel,
                        join.getCondition(), join.getConditionType(),
                        convertedJoinType));
            }
            return;
        case SqlKind.SelectORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final SaffronRel rel = convertQueryRecursive(from);
            bb.setRoot(rel);
            return;
        case SqlKind.ValuesORDINAL:
            convertValues(bb,(SqlCall) from);
            return;
        default:
            throw Util.newInternal("not a join operator " + from);
        }
    }

    private JoinRel createJoin(
            Blackboard bb, SaffronRel leftRel, SaffronRel rightRel,
            SqlNode condition, SqlJoinOperator.ConditionType conditionType,
            int joinType) {
        // Deal with any forward-references.
        if (!cluster.query.mapDeferredToCorrel.isEmpty()) {
            Iterator lookups =
                cluster.query.mapDeferredToCorrel.keySet().iterator();
            while (lookups.hasNext()) {
                DeferredLookup lookup = (DeferredLookup) lookups.next();
                String correlName =
                    (String) cluster.query.mapDeferredToCorrel.get(lookup);

                // as a side-effect, this associates correlName with rel
                RexNode expression =
                    lookup.lookup(
                        new SaffronRel [] { leftRel,rightRel },
                        correlName);
                assert(expression != null);
            }
            cluster.query.mapDeferredToCorrel.clear();
        }

        // Make sure that left does not depend upon a correlating variable
        // coming from right. We'll swap them before we create a
        // JavaNestedLoopJoin.
        String [] variablesL2R =
            OptUtil.getVariablesSetAndUsed(rightRel,leftRel);
        String [] variablesR2L =
            OptUtil.getVariablesSetAndUsed(leftRel,rightRel);
        if ((variablesL2R.length > 0) && (variablesR2L.length > 0)) {
            throw Util.newInternal(
                "joined expressions must not be mutually dependent: "
                + condition);
        }
        HashSet variablesStopped = new HashSet();
        for (int i = 0; i < variablesL2R.length; i++) {
            variablesStopped.add(variablesL2R[i]);
        }
        for (int i = 0; i < variablesR2L.length; i++) {
            variablesStopped.add(variablesR2L[i]);
        }
        RexNode conditionExp = convertJoinCondition(bb,
                condition, conditionType, leftRel, rightRel);
        return new JoinRel(
            cluster,
            leftRel,
            rightRel,
            conditionExp,
            joinType,
            variablesStopped);
    }

    private RexNode convertJoinCondition(Blackboard bb, SqlNode condition,
            SqlJoinOperator.ConditionType conditionType, SaffronRel leftRel,
            SaffronRel rightRel) {
        if (condition == null) {
            return rexBuilder.makeLiteral(true);
        }
        switch (conditionType.getOrdinal()) {
        case SqlJoinOperator.ConditionType.On_ORDINAL:
            bb.setRoot(new SaffronRel [] { leftRel,rightRel });
            return convertExpression(bb,condition);
        case SqlJoinOperator.ConditionType.Using_ORDINAL:
            SqlNodeList list = (SqlNodeList) condition;
            RexNode conditionExp = null;
            for (int i = 0; i < list.size(); i++) {
                final SqlNode columnName = list.get(i);
                assert(columnName instanceof SqlIdentifier);
                RexNode exp = convertExpression(bb,columnName);
                if (i == 0) {
                    conditionExp = exp;
                } else {
                    SqlOperator andOp = rexBuilder.getOperator("AND", SqlOperator.Syntax.Binary);
                    conditionExp = rexBuilder.makeCall(
                            andOp, conditionExp, exp);
                }
            }
            assert conditionExp != null;
            return conditionExp;
        default:
            throw conditionType.unexpected();
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

    /**
     * Converts a GROUP BY clause.
     *
     * @param bb       Scope within which to resolve identifiers
     * @param groupList GROUP BY clause, or null
     * @pre child != null
     * @post return != null
     */
    private void convertGroup(Blackboard bb, SqlNodeList groupList)
    {
        assert bb.root != null : "precondition: child != null";
        if (groupList == null) {
            return;
        }
        ArrayList list = new ArrayList();
        for (int i = 0; i < groupList.size(); i++) {
            RexNode expression = convertExpression(bb,groupList.get(i));
            list.add(expression);
        }

        // FIXME
        final AggregateRel.Call [] aggCalls = null;
        bb.setRoot(new AggregateRel(cluster,bb.root,groupList.size(),
                aggCalls));
    }

    private RexNode convertLiteral(final SqlLiteral literal)
    {
        if (literal.value == null) {
            SaffronType type = validator.getValidatedNodeType(literal);
            return rexBuilder.makeCast(type,rexBuilder.constantNull());
        } else {
            return convertNonNullLiteral(literal);
        }
    }

    private RexLiteral convertNonNullLiteral(final SqlLiteral literal)
    {
        final Object value = literal.value;
        if (value instanceof Number) {
            final Number number = (Number) value;

            // Convert to integer if possible.
            if (number instanceof BigInteger) {
                long i = number.longValue();

                // NOTE:  Number.equals is insane because it requires the
                // comparands to be of the same exact type.  Be careful.
                if (number.equals(BigInteger.valueOf(i))) {
                    return rexBuilder.makeLiteral(i);
                }
            }

            // TODO:  preserve fixed-point precision and large integers
            return rexBuilder.makeLiteral(number.doubleValue());
        } else if (value instanceof String) {
            return rexBuilder.makeLiteral((String) value);
        } else if (value instanceof Boolean) {
            return rexBuilder.makeLiteral(((Boolean) value).booleanValue());
        } else if (value instanceof byte[]) {
            return rexBuilder.makeLiteral((byte[]) value);
        } else if (value instanceof SqlLiteral.BitString) {
            return rexBuilder.makeLiteral((SqlLiteral.BitString) value);
        } else if (value instanceof SqlLiteral.StringLiteral) {
            return rexBuilder.makeLiteral((SqlLiteral.StringLiteral) value);
        } else if (value instanceof SqlFunctionTable.FunctionFlagType) {
            return rexBuilder.makeLiteral((SqlFunctionTable.FunctionFlagType) value);
        } else if (value instanceof java.sql.Date) {
            return rexBuilder.makeLiteral((java.sql.Date) value);
        } else if (value instanceof java.sql.Time) {
            return rexBuilder.makeLiteral((java.sql.Time) value);
        } else if (value instanceof java.sql.Timestamp) {
            return rexBuilder.makeLiteral((java.sql.Timestamp) value);
        } else {
            throw Util.needToImplement(literal);
        }
    }

    public RexDynamicParam convertDynamicParam(
        final SqlDynamicParam dynamicParam)
    {
        // REVIEW jvs 8-Jan-2004:  dynamic params may be encountered out of
        // order.  Should probably cross-check with the count from the parser
        // at the end and make sure they all got filled in.  Why doesn't List
        // have a resize() method?!?  Make this a utility.
        while (dynamicParam.index >= dynamicParamSqlNodes.size()) {
            dynamicParamSqlNodes.add(null);
        }

        dynamicParamSqlNodes.set(dynamicParam.index,dynamicParam);
        return rexBuilder.makeDynamicParam(
            getDynamicParamType(dynamicParam.index),
            dynamicParam.index);
    }

    /**
     * Converts an ORDER BY clause.
     * @param bb       Scope within which to resolve identifiers
     * @param orderList Order by clause, or null
     * @pre bb.root != null
     * @post return != null
     */
    private void convertOrder(Blackboard bb,SqlNodeList orderList)
    {
        // TODO:  add validation rules to SqlValidator also

        assert bb.root != null : "precondition: child != null";
        if (orderList == null) {
            return;
        }
        RelFieldCollation [] collations =
            new RelFieldCollation[orderList.size()];
        for (int i = 0; i < collations.length; ++i) {
            SqlNode orderItem = orderList.get(i);
            int iOrdinal;
            if (orderItem.isA(SqlKind.Literal)) {
                SqlLiteral sqlLiteral = (SqlLiteral) orderItem;
                RexLiteral ordinalExp = convertNonNullLiteral(sqlLiteral);
                if (!(ordinalExp.getValue() instanceof Number)) {
                    throw Util.needToImplement(ordinalExp);
                }
                // SQL ordinals are 1-based, but SortRel's are 0-based
                iOrdinal = ((Number) sqlLiteral.getValue()).intValue() - 1;
            } else if (orderItem.isA(SqlKind.Identifier)) {
                SqlIdentifier id = (SqlIdentifier) orderItem;
                iOrdinal =
                    bb.root.getRowType().getFieldOrdinal(id.getSimple());
                assert(iOrdinal != -1);
            } else {
                // TODO:  handle descending, collation sequence,
                // (and expressions, but flagged as non-standard)
                throw Util.needToImplement(orderItem);
            }
            assert(iOrdinal < bb.root.getRowType().getFieldCount());
            collations[i] = new RelFieldCollation(iOrdinal);
        }
        bb.setRoot(new SortRel(cluster,bb.root,collations));
    }

    private SaffronRel convertQueryRecursive(SqlNode query)
    {
        final int kind = query.getKind().ordinal_;
        if (query instanceof SqlSelect) {
            return convertSelect((SqlSelect) query);
        } else if (query.isA(SqlKind.Insert)) {
            final SqlInsert call = (SqlInsert) query;
            return convertInsert(call);
        } else if (query.isA(SqlKind.Delete)) {
            final SqlDelete call = (SqlDelete) query;
            return convertDelete(call);
        } else if (query.isA(SqlKind.Update)) {
            final SqlUpdate call = (SqlUpdate) query;
            return convertUpdate(call);
        } else if (query instanceof SqlCall) {
            final SqlCall call = (SqlCall) query;
            final SqlNode [] operands = call.getOperands();
            final SaffronRel left = convertQueryRecursive(operands[0]);
            final SaffronRel right = convertQueryRecursive(operands[1]);
            boolean all = false;
            if (call.operator instanceof SqlSetOperator) {
                all = ((SqlSetOperator) (call.operator)).all;
            }
            switch (kind) {
            case SqlKind.UnionORDINAL:
                return new UnionRel(
                    cluster, new SaffronRel[] {left, right}, all);
            case SqlKind.IntersectORDINAL:
                // TODO:  all
                return new IntersectRel(cluster, left, right);
            case SqlKind.ExceptORDINAL:
                throw Util.needToImplement(this);
            default:
                throw Util.newInternal(
                    "not a set operator "
                    + SqlKind.enumeration.getName(kind));
            }
        } else {
            throw Util.newInternal("not a query: " + query);
        }
    }

    private SaffronRel convertInsert(SqlInsert call)
    {
        SqlValidator.Scope targetScope =
            validator.getScope(call.getTargetTable());
        SaffronRel sourceRel =
            convertQueryRecursive(call.getSourceSelect());
        SaffronTable targetTable =
            getSaffronTable(targetScope,schema);
        SaffronType lhsRowType = targetTable.getRowType();
        SqlNodeList targetColumnList = call.getTargetColumnList();

        SaffronType sourceRowType = sourceRel.getRowType();
        int nExps = lhsRowType.getFieldCount();
        RexNode [] rhsExps = new RexNode[nExps];

        final RexNode sourceRef = rexBuilder.makeRangeReference(
            sourceRowType, 0);
        if (targetColumnList == null) {
            // Source expressions match target columns in order.
            for (int i = 0; i < nExps; ++i) {
                rhsExps[i] = rexBuilder.makeFieldAccess(sourceRef, i);
            }
        } else {
            // Source expressions are mapped to target columns by name via
            // targetColumnList, and may not cover the entire target table.
            // So, we'll make up a full row, using a combination of default
            // values and the source expressions provided.

            int iSrc = 0;
            Iterator iter = targetColumnList.getList().iterator();
            for (; iter.hasNext(); ++iSrc) {
                SqlIdentifier id = (SqlIdentifier) iter.next();
                String targetColumnName = id.getSimple();
                int iTarget = lhsRowType.getFieldOrdinal(targetColumnName);
                assert(iTarget != -1);
                rhsExps[iTarget] = rexBuilder.makeFieldAccess(sourceRef, iSrc);
            }

            for (int i = 0; i < nExps; ++i) {
                if (rhsExps[i] != null) {
                    continue;
                }

                rhsExps[i] = defaultValueFactory.newDefaultValue(
                    targetTable,i);
            }

            sourceRel = new ProjectRel(
                cluster,
                sourceRel,
                rhsExps,
                null,
                ProjectRel.Flags.Boxed);
        }


        return new TableModificationRel(
            cluster,
            targetTable,
            connection,
            sourceRel,
            TableModificationRel.Operation.INSERT,
            null);
    }

    private SaffronRel convertDelete(SqlDelete call)
    {
        SqlIdentifier from = call.getTargetTable();
        SqlValidator.Scope targetScope = validator.getScope(from);
        SaffronTable targetTable =
            getSaffronTable(targetScope,schema);
        SaffronRel sourceRel = convertSelect(
            call.getSourceSelect());
        return new TableModificationRel(
            cluster,
            targetTable,
            connection,
            sourceRel,
            TableModificationRel.Operation.DELETE,
            null);
    }

    private SaffronRel convertUpdate(SqlUpdate call)
    {
        SqlIdentifier from = call.getTargetTable();
        SqlValidator.Scope targetScope = validator.getScope(from);
        SaffronTable targetTable =
            getSaffronTable(targetScope,schema);

        // convert update column list from SqlIdentifier to String
        List targetColumnNameList = new ArrayList();
        List targetColumnList = call.getTargetColumnList().getList();
        Iterator targetColumnIter = targetColumnList.iterator();
        while (targetColumnIter.hasNext()) {
            SqlIdentifier id = (SqlIdentifier) targetColumnIter.next();
            String name = id.getSimple();
            targetColumnNameList.add(name);
        }

        SaffronRel sourceRel = convertSelect(
            call.getSourceSelect());

        return new TableModificationRel(
            cluster,
            targetTable,
            connection,
            sourceRel,
            TableModificationRel.Operation.UPDATE,
            targetColumnNameList);
    }

    /**
     * Converts an identifier into an expression in a given scope. For
     * example, the "empno" in "select empno from emp join dept" becomes
     * "emp.empno".
     */
    private RexNode convertIdentifier(
        Blackboard bb, SqlIdentifier identifier)
    {
        // first check for reserved identifiers like CURRENT_USER
        SaffronType type = validator.contextVariableTable.deriveType(
            identifier);
        if (type != null) {
            return rexBuilder.makeContextVariable(identifier.getSimple(),type);
        }

        identifier = bb.scope.fullyQualify(identifier);
        RexNode e = bb.lookupExp(identifier.names[0]);
        for (int i = 1; i < identifier.names.length; i++) {
            String name = identifier.names[i];
            e = rexBuilder.makeFieldAccess(e,name);
        }
        if (e instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) e;
            // adjust the type to account for nulls introduced by outer joins
            SaffronField field = bb.getRootField(inputRef);
            if (field != null) {
                e = rexBuilder.makeInputRef(field.getType(),inputRef.index);
            }
        }
        return e;
    }

    /**
     * Converts a row constructor into a relational expression.
     * @pre rowConstructor
     * @param bb
     * @param rowConstructor
     * @return Relational expression which returns a single row.
     * @pre isRowConstructor(rowConstructor)
     */
    private SaffronRel convertRowConstructor(
        Blackboard bb,
        SqlCall rowConstructor)
    {
        assert isRowConstructor(rowConstructor) :
            "precondition: isRowConstructor(rowConstructor), was: " +
            rowConstructor;

        final SqlNode [] operands = rowConstructor.getOperands();
        RexNode[] selectList = new RexNode[operands.length];
        String[] fieldNames = new String[operands.length];
        for (int i = 0; i < operands.length; ++i) {
            RexNode value = convertExpression(bb,operands[i]);
            selectList[i] = value;
            fieldNames[i] = bb.scope.deriveAlias(operands[i],i);
        }

        // SELECT value-list FROM onerow
        final OneRowRel oneRow = new OneRowRel(cluster);
        return new ProjectRel(cluster,oneRow,selectList,fieldNames,
                ProjectRel.Flags.Boxed);
    }

    private void convertSelectList(Blackboard bb,SqlNodeList selectList)
    {
        assert bb.scope instanceof SqlValidator.SelectScope;
        selectList = validator.expandStar(selectList,
                ((SqlValidator.SelectScope) bb.scope).select);
        replaceSubqueries(bb, selectList);
        String[] fieldNames = new String[selectList.size()];
        RexNode[] exps = new RexNode[selectList.size()];
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode node = selectList.get(i);
            exps[i] = convertExpression(bb,node);
            fieldNames[i] = bb.scope.deriveAlias(node,i);
        }
        bb.setRoot(new ProjectRel(cluster, bb.root, exps, fieldNames,
                ProjectRel.Flags.Boxed));
    }

    /**
     * Converts a values clause (as in "INSERT INTO T(x,y) VALUES (1,2)")
     * into a relational expression.
     */
    private void convertValues(Blackboard bb,SqlCall values)
    {
        SqlNode[] rowConstructorList = values.getOperands();
        ArrayList unionRels = new ArrayList();
        for (int i = 0; i < rowConstructorList.length; i++) {
            SqlCall rowConstructor = (SqlCall) rowConstructorList[i];
            SaffronRel queryExpr = convertRowConstructor(bb,rowConstructor);
            unionRels.add(queryExpr);
        }

        if (unionRels.size() == 0) {
            throw Util.newInternal("empty values clause");
        } else if (unionRels.size() == 1) {
            bb.setRoot((SaffronRel) unionRels.get(0));
        } else {
            bb.setRoot(
                new UnionRel(
                    cluster,
                    (SaffronRel []) unionRels.toArray(new SaffronRel[0]),
                    true));
        }
        leaves.add(bb.root);
        // REVIEW jvs 22-Jan-2004:  should I add
        // mapScopeToRel.put(validator.getScope(values),bb.root);
        // ?
    }

    VolcanoCluster createCluster(Environment env) {
        VolcanoQuery query;
        if (cluster == null) {
            query = new VolcanoQuery();
        } else {
            query = cluster.query;
        }
        final SaffronTypeFactory typeFactory =
            SaffronTypeFactoryImpl.threadInstance();
        return query.createCluster(env, typeFactory, rexBuilder);
    }

    public static HashMap createBinaryMap()
    {
        HashMap map = new HashMap();
        map.put("/",RexKind.Divide);
        map.put("=",RexKind.Equals);
        map.put(">",RexKind.GreaterThan);
        map.put(">=",RexKind.GreaterThanOrEqual);
        map.put("<",RexKind.LessThan);
        map.put("<=",RexKind.LessThanOrEqual);
        map.put("AND",RexKind.And);
        map.put("OR",RexKind.Or);
        map.put("-",RexKind.Minus);
        // REVIEW jvs 22-Jan-2004:  shouldn't this be "<>"?
        map.put("*",RexKind.NotEquals);
        map.put("+",RexKind.Plus);
        map.put("*",RexKind.Times);
        return map;
    }

    public static HashMap createPrefixMap()
    {
        HashMap map = new HashMap();
        map.put("NOT",RexKind.Not);
        map.put("-",RexKind.MinusPrefix);
        return map;
    }

    public static HashMap createPostfixMap()
    {
        HashMap map = new HashMap();
        return map;
    }

    public static HashMap createFunctionMap()
    {
        HashMap map = new HashMap();
        map.put(RexKind.Substr.getName(), RexKind.Substr);
        return map;
    }

    /**
     * Converts a scope into a {@link SaffronTable}. This is only possible if
     * the scope represents an identifier, such as "sales.emp". Otherwise,
     * returns null.
     */
    public static SaffronTable getSaffronTable(SqlValidator.Scope scope,
            SaffronSchema schema) {
        if (scope instanceof SqlValidator.IdentifierScope) {
            SqlValidator.IdentifierScope identifierScope =
                    (SqlValidator.IdentifierScope) scope;
            final String [] names = identifierScope.id.names;
            return schema.getTableForMember(names);
        } else {
            return null;
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A <code>SchemaCatalogReader</code> looks up catalog information from a
     * {@link net.sf.saffron.core.SaffronSchema saffron schema object}.
     */
    public static class SchemaCatalogReader
        implements SqlValidator.CatalogReader
    {
        private final SaffronSchema schema;
        private final boolean upperCase;

        public SchemaCatalogReader(SaffronSchema schema,boolean upperCase)
        {
            this.schema = schema;
            this.upperCase = upperCase;
        }

        public SqlValidator.Table getTable(String [] names)
        {
            if (names.length != 1) {
                return null;
            }
            final SaffronTable table =
                schema.getTableForMember(new String[]{maybeUpper(names[0])});
            if (table != null) {
                return new SqlValidator.Table() {
                    public SaffronType getRowType() {
                        return table.getRowType();
                    }
                    public String [] getQualifiedName() {
                        return null;
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


    /**
     * Workspace for translating an individual SELECT statement (or sub-SELECT).
     */
    private class Blackboard {
        /** Collection of {@link SaffronRel} objects which correspond to a
         * SELECT statement. */
        final SqlValidator.Scope scope;
        private SaffronRel root;
        private SaffronRel[] inputs;
        /** List of <code>IN</code> and <code>EXISTS</code> nodes inside this
         * <code>SELECT</code> statement (but not inside sub-queries). */
        private final ArrayList subqueries = new ArrayList();
        /** Maps IN and EXISTS {@link SqlSelect sub-queries} to the expressions
         * which will be used to access them. */
        private final HashMap mapSubqueryToExpr = new HashMap();

        /**
         * Creates a Blackboard
         * @pre scope != null
         */
        Blackboard(SqlValidator.Scope scope) {
            assert scope != null : "precondition: scope != null";
            this.scope = scope;
        }

        /**
         * Registers a relational expression
         *
         * @param rel Relational expression
         * @return Expression with which to refer to the row (or partial row)
         *   coming from this relational expression's side of the join.
         */
        public RexNode register(SaffronRel rel) {
            if (root == null) {
                setRoot(rel);
                return rexBuilder.makeRangeReference(root.getRowType(), 0);
            } else {
                final JoinRel join = new JoinRel(rel.getCluster(), root, rel,
                        rexBuilder.makeLiteral(true), JoinRel.JoinType.LEFT,
                        Collections.EMPTY_SET);
                setRoot(join);
                return rexBuilder.makeRangeReference(rel.getRowType(),
                        join.getLeft().getRowType().getFieldCount());
            }
        }

        void setRoot(SaffronRel root) {
            this.root = root;
            this.inputs = new SaffronRel[] {root};
        }

        void setRoot(SaffronRel[] inputs) {
            this.inputs = inputs;
            this.root = null;
        }

        /**
         * Returns an expression with which to reference a from-list item.
         *
         * @param name the alias of the from item
         *
         * @return a {@link openjava.ptree.Variable} or {@link
         *         openjava.ptree.FieldAccess}
         */
        RexNode lookupExp(String name)
        {
            int [] offsets = new int [] { -1 };
            final SqlValidator.Scope[] ancestorScopes =
                new SqlValidator.Scope[1];
            SqlValidator.Scope foundScope = scope.resolve(
                name, ancestorScopes, offsets);
            if (foundScope == null) {
                return null;
            }
            // Found in current query's from list.  Find which from item.
            // We assume that the order of the from clause items has been
            // preserved.
            SqlValidator.Scope ancestorScope = ancestorScopes[0];
            boolean isParent = ancestorScope != scope;
            int offset = offsets[0];
            RexNode result;
            if (inputs != null && !isParent) {
                final SaffronRel[] rels = isParent ?
                        new SaffronRel[] {
                            (SaffronRel) mapScopeToRel.get(ancestorScope)} :
                        inputs;
                result = lookup(offset, rels,isParent,null);
            } else {
                // We're referencing a relational expression which has not
                // been converted yet. This occurs when from items are
                // correlated, e.g. "select from emp as emp join emp.getDepts()
                // as dept". Create a temporary expression.
                assert isParent;
                DeferredLookup lookup = new DeferredLookup(
                        this,offset,isParent);
                String correlName = cluster.query.createCorrelUnresolved(lookup);
                final SaffronType rowType = foundScope.getRowType();
                result = rexBuilder.makeCorrel(rowType, correlName);
            }
            return result;
            // Not found.
        }

        /**
         * Creates an expression with which to reference
         * <code>expression</code>, whose offset in its from-list is
         * <code>offset</code>.
         */
        RexNode lookup(
            int offset,
            SaffronRel [] inputs,
            boolean isParent,
            String varName)
        {
            final ArrayList relList = flatten(inputs);
            if (offset < 0 || offset >= relList.size()) {
                throw Util.newInternal("could not find input " + offset);
            }
            int fieldOffset = 0;
            for (int i = 0; i < offset; i++) {
                final SaffronRel rel = (SaffronRel) relList.get(i);
                fieldOffset += rel.getRowType().getFieldCount();
            }
            SaffronRel rel = (SaffronRel) relList.get(offset);
            if (isParent) {
                if (varName == null) {
                    varName = rel.getOrCreateCorrelVariable();
                } else {
                    // we are resolving a forward reference
                    rel.registerCorrelVariable(varName);
                }
                return rexBuilder.makeCorrel(rel.getRowType(), varName);
            } else {
                return rexBuilder.makeRangeReference(rel.getRowType(), fieldOffset);
            }
        }

        SaffronField getRootField(RexInputRef inputRef)
        {
            int fieldOffset = inputRef.index;
            for (int i = 0; i < inputs.length; ++i) {
                SaffronType rowType = inputs[i].getRowType();
                if (rowType == null) {
                    // TODO:  remove this once leastRestrictive
                    // is correctly implemented
                    return null;
                }
                if (fieldOffset < rowType.getFieldCount()) {
                    return rowType.getFields()[fieldOffset];
                }
                fieldOffset -= rowType.getFieldCount();
            }
            throw new AssertionError();
        }

        private ArrayList flatten(SaffronRel[] rels) {
            ArrayList list = new ArrayList();
            flatten(rels, list);
            return list;
        }

        private void flatten(SaffronRel[] rels, ArrayList list) {
            for (int i = 0; i < rels.length; i++) {
                SaffronRel rel = rels[i];
                if (leaves.contains(rel)) {
                    list.add(rel);
                } else {
                    flatten(rel.getInputs(), list);
                }
            }
        }

        public void registerSubquery(SqlNode node) {
            subqueries.add(node);
        }
    }

    /**
     * Contains the information necessary to repeat a call to
     * {@link Blackboard#lookup}.
     */
    static class DeferredLookup
    {
        //~ Instance fields ---------------------------------------------------

        Blackboard bb;
        boolean isParent;
        int offset;

        //~ Constructors ------------------------------------------------------

        DeferredLookup(Blackboard bb,int offset,boolean isParent)
        {
            this.bb = bb;
            this.offset = offset;
            this.isParent = isParent;
        }

        //~ Methods -----------------------------------------------------------

        RexNode lookup(SaffronRel [] inputs,String varName)
        {
            return bb.lookup(offset,inputs,isParent,varName);
        }
    }

    /**
     * An implementation of DefaultValueFactory which always supplies NULL.
     */
    class NullDefaultValueFactory implements DefaultValueFactory
    {
        public RexNode newDefaultValue(
            SaffronTable table,
            int iColumn)
        {
            return rexBuilder.constantNull();
        }
    }

}


// End SqlToRelConverter.java
