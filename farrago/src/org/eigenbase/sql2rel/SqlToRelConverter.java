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

package org.eigenbase.sql2rel;

import openjava.mop.Environment;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlLikeOperator;
import org.eigenbase.sql.fun.SqlRowOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.fun.SqlMultisetOperator;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.parser.ParserUtil;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.BitString;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;

import java.math.BigDecimal;
import java.util.*;


/**
 * Converts a SQL parse tree (consisting of {@link org.eigenbase.sql.SqlNode}
 * objects) into a relational algebra expression (consisting of
 * {@link org.eigenbase.rel.RelNode} objects).
 *
 * <p>The public entry points are:
 * {@link #convertQuery},
 * {@link #convertValidatedQuery},
 * {@link #convertExpression(SqlNode)}.
 *
 * @testcase {@link org.eigenbase.test.SqlToRelConverterTest}
 * @author jhyde
 * @since Oct 10, 2003
 * @version $Id$
 **/
public class SqlToRelConverter
{
    //~ Instance fields -------------------------------------------------------

    private final SqlValidator validator;
    private RexBuilder rexBuilder;
    private RelOptPlanner planner;
    private RelOptConnection connection;
    private RelOptSchema schema;
    private RelOptCluster cluster;
    private HashMap mapScopeToRel = new HashMap();
    private DefaultValueFactory defaultValueFactory;
    final ArrayList leaves = new ArrayList();
    private List dynamicParamSqlNodes;
    private final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

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
        RelOptSchema schema,
        Environment env,
        RelOptPlanner planner,
        RelOptConnection connection,
        RexBuilder rexBuilder)
    {
        Util.pre(connection != null, "connection != null");
        this.validator = validator;
        this.schema = schema;
        this.planner = planner;
        this.connection = connection;
        this.defaultValueFactory = new NullDefaultValueFactory();
        this.rexBuilder = rexBuilder;

        this.cluster = createCluster(env);

        dynamicParamSqlNodes = new ArrayList();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the RelOptCluster in use.
     */
    public RelOptCluster getCluster()
    {
        return cluster;
    }

    /**
     * Returns the row-expression builder.
     */
    public RexBuilder getRexBuilder()
    {
        return rexBuilder;
    }

    /**
     * Returns the number of dynamic parameters encountered during translation;
     * this must only be called after {@link #convertQuery}.
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
     * @return inferred type, never null
     */
    public RelDataType getDynamicParamType(int index)
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
    public RelNode convertQuery(SqlNode query)
    {
        query = validator.validate(query);
        return convertQueryRecursive(query);
    }

    /**
     * Converts a validated query's parse tree into a relational expression.
     */
    public RelNode convertValidatedQuery(SqlNode query)
    {
        return convertQueryRecursive(query);
    }

    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public RelNode convertSelect(SqlSelect select)
    {
        final SqlValidator.Scope selectScope = validator.getScope(select,
            SqlSelect.WHERE_OPERAND);
        final Blackboard bb = new Blackboard(selectScope);
        convertFrom(
            bb,
            select.getFrom());
        convertWhere(
            bb,
            select.getWhere());
        if (validator.isAggregate(select)) {
            convertAgg(
                bb,
                select.getGroup(),
                select.getHaving(),
                select.getSelectList());
        } else {
            convertSelectList(
                bb,
                select.getSelectList(),
                select);
        }
        if (select.isDistinct()) {
            bb.setRoot(new DistinctRel(cluster, bb.root));
        }
        convertOrder(
            bb,
            select.getOrderList());
        leaves.add(bb.root);
        mapScopeToRel.put(selectScope, bb.root);
        return bb.root;
    }

    /**
     * Converts a WHERE clause.
     *
     * @param bb
     * @param where WHERE clause, may be null
     */
    private void convertWhere(
        final Blackboard bb,
        final SqlNode where)
    {
        if (where == null) {
            return;
        }
        replaceSubqueries(bb, where);
        final RexNode convertedWhere = convertExpression(bb, where);
        bb.setRoot(new FilterRel(cluster, bb.root, convertedWhere));
    }

    private void replaceSubqueries(
        final Blackboard bb,
        final SqlNode expr)
    {
        findSubqueries(bb, expr);
        substituteSubqueries(bb);
    }

    private void substituteSubqueries(Blackboard bb)
    {
        for (int i = 0; i < bb.subqueries.size(); i++) {
            SqlNode node = (SqlNode) bb.subqueries.get(i);
            final RexNode expr = (RexNode) bb.mapSubqueryToExpr.get(node);
            if (expr == null) {
                RelNode converted;
                switch (node.getKind().getOrdinal()) {
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
                    converted =
                        convertExists(
                            bb,
                            seek,
                            condition,
                            rexBuilder.makeLiteral(true),
                            "$indicator");
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
                    converted =
                        convertExists(
                            bb,
                            select,
                            null,
                            rexBuilder.makeLiteral(true),
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
                    throw Util.newInternal("unexpected kind of subquery :"
                        + node);
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
    private RelNode convertExists(
        Blackboard bb,
        SqlNode seek,
        SqlNode condition,
        RexLiteral extraExpr,
        String extraName)
    {
        assert (extraExpr == null) || (extraName != null) : "precondition: extraExpr == null || extraName != null";
        RelNode converted = convertQueryOrInList(bb, seek);
        if (condition != null) {
            // We are translating an IN clause, so add a condition.
            RexNode conditionExp = null;
            final RexNode ref =
                rexBuilder.makeRangeReference(
                    bb.root.getRowType(),
                    0);
            if (condition instanceof SqlNodeList) {
                // If "seek" is "(emp,dept)", generate the condition "emp = Q.c1
                // and dept = Q.c2".
                SqlNodeList conditionList = (SqlNodeList) condition;
                for (int i = 0; i < conditionList.size(); i++) {
                    SqlNode conditionNode = conditionList.get(i);
                    RexNode e =
                        rexBuilder.makeCall(
                            rexBuilder.opTab.equalsOperator,
                            convertExpression(bb, conditionNode),
                            rexBuilder.makeFieldAccess(ref, i));
                    if (i == 0) {
                        conditionExp = e;
                    } else {
                        conditionExp =
                            rexBuilder.makeCall(opTab.andOperator,
                                conditionExp, e);
                    }
                }
            } else {
                // If "seek" is "emp", generate the condition "emp = Q.c1". The
                // query must have precisely one column.
                assert converted.getRowType().getFieldList().size() == 1;
                conditionExp =
                    rexBuilder.makeCall(
                        rexBuilder.opTab.equalsOperator,
                        convertExpression(bb, condition),
                        rexBuilder.makeFieldAccess(ref, 0));
            }
            converted = new FilterRel(cluster, converted, conditionExp);
        }
        if (extraExpr != null) {
            final RelDataType rowType = converted.getRowType();
            final RelDataTypeField [] fields = rowType.getFields();
            final RexNode [] expressions = new RexNode[fields.length + 1];
            String [] fieldNames = new String[fields.length + 1];
            final RexNode ref = rexBuilder.makeRangeReference(rowType, 0);
            for (int j = 0; j < fields.length; j++) {
                expressions[j] = rexBuilder.makeFieldAccess(ref, j);
                fieldNames[j] = fields[j].getName();
            }
            expressions[fields.length] = extraExpr;
            fieldNames[fields.length] =
                uniqueFieldName(fieldNames, fields.length, extraName);
            converted =
                new ProjectRel(cluster, converted, expressions, fieldNames,
                    ProjectRelBase.Flags.Boxed);
        }
        return converted;
    }

    private RelNode convertQueryOrInList(
        Blackboard bb,
        SqlNode seek)
    {
        // NOTE: Once we start accepting single-row queries as row
        // constructors, there will be an ambiguity here for a case like X IN
        // ((SELECT Y FROM Z)).  The SQL standard resolves the ambiguity by
        // saying that a lone select should be interpreted as a table
        // expression, not a row expression.  The semantic difference is that a
        // table expression can return multiple rows.
        if (seek instanceof SqlNodeList) {
            SqlNodeList list = (SqlNodeList) seek;
            RelNode [] inputs = new RelNode[list.size()];
            for (int i = 0; i < list.size(); i++) {
                SqlNode node = list.get(i);
                SqlCall call;
                if (isRowConstructor(node)) {
                    call = (SqlCall) node;
                } else {
                    // convert "1" to "row(1)"
                    call =
                        opTab.rowConstructor.createCall(
                            new SqlNode [] { node },
                            null);
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
    private static String uniqueFieldName(
        String [] names,
        int length,
        String s)
    {
        if (!contains(names, length, s)) {
            return s;
        }
        int n = length;
        while (true) {
            s = "EXPR_" + n;
            if (!contains(names, length, s)) {
                return s;
            }

            // FIXME jvs 15-Nov-2003:  If we ever get here, it's an infinite
            // loop; should be ++n?
        }
    }

    private static boolean contains(
        String [] names,
        int length,
        String s)
    {
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
    private void findSubqueries(
        Blackboard bb,
        SqlNode node)
    {
        switch (node.getKind().getOrdinal()) {
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
                    if (operand != null) {
                        findSubqueries(bb, operand);
                    }
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
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param node Expression to translate
     * @return Converted expression
     */
    public RexNode convertExpression(
        SqlNode node)
    {
        Blackboard bb = new Blackboard(null);
        return convertExpression(bb, node);
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format,
     * mapping identifier references to predefined expressions.
     *
     * @param node Expression to translate
     *
     * @param nameToNodeMap map from String to RexNode; when an SqlIdentifier
     * is encountered, it is used as a key and translated to the corresponding
     * value from this map
     *
     * @return Converted expression
     */
    public RexNode convertExpression(
        SqlNode node,
        final Map nameToNodeMap)
    {
        // REVIEW jvs 2-Jan-2005: should perhaps create a proper scope as well
        Blackboard bb = new Blackboard(null) 
            {
                RexNode lookupExp(String name)
                {
                    RexNode node = (RexNode) nameToNodeMap.get(name);
                    if (node == null) {
                        throw Util.newInternal(
                            "Unknown identifier '" + name
                            + "' encountered while expanding expression"
                            + node);
                    }
                    return node;
                }
            };
        return convertExpression(bb, node);
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param bb Workspace
     * @param node Expression to translate
     * @return Converted expression
     *
     * @pre bb != null
     */
    RexNode convertExpression(
        Blackboard bb,
        SqlNode node)
    {
        assert bb != null : "precondition: bb != null";
        if (bb.agg != null) {
            RexNode rex = bb.agg.lookupGroupExpr(node);
            if (rex != null) {
                return rex;
            }
        }
        final SqlNode [] operands;
        switch (node.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            operands = ((SqlCall) node).getOperands();
            return convertExpression(bb, operands[0]);
        case SqlKind.IdentifierORDINAL:
            return convertIdentifier(bb, (SqlIdentifier) node);
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
            final int fieldCount = expr.getType().getFieldList().size();
            return rexBuilder.makeFieldAccess(expr, fieldCount - 1);

        case SqlKind.OverORDINAL:
            return convertOver(bb, node);

        default:
            // fall through
        }

        // REVIEW jhyde 2004/8/11: replace all of this code with a method
        //   SqlOperator.convertToRex or something similar.
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;

            // aliases:
            // TODO: handle aliases in a more elegant way
            if (call.operator.equals(opTab.characterLengthFunc)) {
                call.operator = opTab.charLengthFunc;
            } else if (call.operator.equals(opTab.isUnknownOperator)) {
                call.operator = opTab.isNullOperator;
            } else if (call.operator.equals(opTab.isNotUnknownOperator)) {
                call.operator = opTab.isNotNullOperator;
            }

            if (call.operator.isAggregator()) {
                Util.permAssert(bb.agg != null,
                    "aggregate fun must occur in aggregation mode");
                return bb.agg.convertCall(call);
            }
            operands = call.getOperands();
            if (call.operator instanceof SqlJdbcFunctionCall) {
                SqlJdbcFunctionCall jdbcCall =
                    (SqlJdbcFunctionCall) call.operator;
                return convertExpression(
                    bb,
                    jdbcCall.getLookupCall());
            } else if (call.operator.equals(opTab.castFunc)) {
                return convertCast(bb, call);
            } else if (call.operator instanceof SqlFunction
                || call.operator instanceof SqlRowOperator) {
                final RexNode [] exprs =
                    convertExpressionList(bb, operands);
                return rexBuilder.makeCall(call.operator, exprs);
            } else if (call.operator instanceof SqlLikeOperator) {
                final RexNode [] exprs =
                    convertExpressionList(bb, operands);
                RexNode rexCall;
                if (((SqlLikeOperator) call.operator).negated) {
                    rexCall =
                        rexBuilder.makeCall(opTab.likeOperator, exprs);
                    rexCall =
                        rexBuilder.makeCall(opTab.notOperator, rexCall);
                } else {
                    rexCall = rexBuilder.makeCall(call.operator, exprs);
                }
                return rexCall;
            } else if (call.operator instanceof SqlPrefixOperator
                || call.operator instanceof SqlPostfixOperator) {
                final RexNode exp = convertExpression(bb, operands[0]);
                SqlOperator op = call.operator;
                if (op.equals(rexBuilder.opTab.prefixPlusOperator)) {
                    // Unary "+" has no effect. There is no
                    // corresponding Rex operator.
                    return exp;
                }
                return rexBuilder.makeCall(
                    op,
                    new RexNode [] { exp });
            } else if (call.operator instanceof SqlCaseOperator) {
                return convertCase(bb, (SqlCase) call);
            } else if (call.operator instanceof SqlBetweenOperator) {
                return convertBetween(bb, call);
            } else if (call.operator.equals(
                rexBuilder.opTab.literalChainOperator)) {
                return convertLiteralChain(bb, call);
            } else if ((call.operator instanceof SqlBinaryOperator) ||
                       (call.operator instanceof SqlMultisetOperator)) {
                final RexNode[] exprs =
                    convertExpressionList(bb, operands);
                return rexBuilder.makeCall(call.operator, exprs);
            }

            else {
                throw Util.needToImplement(node);
            }
        } else {
            throw Util.needToImplement(node);
        }
    }

    private RexNode convertOver(Blackboard bb, SqlNode node) {
        SqlCall call = (SqlCall) node;
        SqlCall aggCall = (SqlCall) call.operands[0];
        assert(aggCall.operator.isAggregator());
        SqlNode windowOrRef = call.operands[1];
        SqlWindow window = validator.resolveWindow(windowOrRef, bb.scope);
        final RexNode[] exprs =
            convertExpressionList(bb, aggCall.operands);
        final RelDataType type = validator.getValidatedNodeType(aggCall);
        return rexBuilder.makeOver(type, aggCall.operator, exprs, window,
            window.getLowerBound(), window.getUpperBound(), window.isRows());
    }

    /**
     * converts a between call node.
     */
    private RexNode convertBetween(
        Blackboard bb,
        SqlCall call)
    {
        final SqlNode value = call.operands[SqlBetweenOperator.VALUE_OPERAND];
        RexNode x = convertExpression(bb, value);
        final SqlBetweenOperator.Flag symmetric = (SqlBetweenOperator.Flag)
            call.operands[SqlBetweenOperator.SYMFLAG_OPERAND];
        boolean isAsymmetric = symmetric.isAsymmetric;
        final SqlNode lower = call.operands[SqlBetweenOperator.LOWER_OPERAND];
        RexNode y = convertExpression(bb, lower);
        final SqlNode upper = call.operands[SqlBetweenOperator.UPPER_OPERAND];
        RexNode z = convertExpression(bb, upper);

        RexNode res;

        RexNode ge1 =
            rexBuilder.makeCall(opTab.greaterThanOrEqualOperator, x, y);
        RexNode le1 = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, z);
        RexNode and1 = rexBuilder.makeCall(opTab.andOperator, ge1, le1);

        if (isAsymmetric) {
            res = and1;
        } else {
            RexNode ge2 =
                rexBuilder.makeCall(opTab.greaterThanOrEqualOperator, x, z);
            RexNode le2 =
                rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, y);
            RexNode and2 = rexBuilder.makeCall(opTab.andOperator, ge2, le2);
            res = rexBuilder.makeCall(opTab.orOperator, and1, and2);
        }
        final SqlBetweenOperator betweenOp =
            (SqlBetweenOperator) call.operator;
        if (betweenOp.negated) {
            res = rexBuilder.makeCall(opTab.notOperator, res);
        }
        return res;
    }

    /**
     * Converts a LiteralChain expression: that is, concatenates the operands
     * immediately, to produce a single literal string.
     */
    private RexNode convertLiteralChain(
        Blackboard bb,
        SqlCall call)
    {
        Util.discard(bb);
        // REVIEW mb: this code really belongs inside the LiteralChain operator
        assert call.operands.length > 0;
        assert call.operands[0] instanceof SqlLiteral :
            call.operands[0].getClass();
        SqlLiteral [] fragments =
            (SqlLiteral []) Arrays.asList(call.operands).toArray(
                new SqlLiteral[call.operands.length]);
        SqlLiteral sum = SqlUtil.concatenateLiterals(fragments);
        return convertNonNullLiteral(sum);
    }

    /**
     * converts a cast function node.
     * @param bb
     * @param call
     * @return
     */
    private RexNode convertCast(
        Blackboard bb,
        SqlCall call)
    {
        assert SqlKind.Cast.equals(call.operator.kind);
        SqlDataTypeSpec dataType = (SqlDataTypeSpec) call.operands[1];
        if (SqlUtil.isNullLiteral(call.operands[0], false)) {
            return convertExpression(bb, call.operands[0]);
        }
        RexNode arg = convertExpression(bb, call.operands[0]);
        return rexBuilder.makeCast(
            dataType.getType(),
            arg);
    }

    private RexNode convertCase(
        Blackboard bb,
        SqlCase call)
    {
        SqlNodeList whenList = call.getWhenOperands();
        SqlNodeList thenList = call.getThenOperands();
        RexNode [] whenThenElseRex = new RexNode[(whenList.size() * 2) + 1];
        assert (whenList.size() == thenList.size());

        for (int i = 0; i < whenList.size(); i++) {
            whenThenElseRex[i * 2] =
                convertExpression(bb, whenList.get(i));
            whenThenElseRex[(i * 2) + 1] =
                convertExpression(bb, thenList.get(i));
        }
        whenThenElseRex[whenThenElseRex.length - 1] =
            convertExpression(
                bb,
                call.getElseOperand());
        return rexBuilder.makeCall(call.operator, whenThenElseRex); //REVIEW 16-March-2004 wael: is there a better way?
    }

    private RexNode [] convertExpressionList(
        Blackboard bb,
        SqlNode [] nodes)
    {
        final RexNode [] exps = new RexNode[nodes.length];
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
     * @param from  FROM clause of a query. Examples include:<ul>
     *    <li>a single table ("SALES.EMP"),
     *    <li>an aliased table ("EMP AS E"),
     *    <li>a list of tables ("EMP, DEPT"),
     *    <li>an ANSI Join expression ("EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"),
     *    <li>a VALUES clause ("VALUES ('Fred', 20)"),
     *    <li>a query ("(SELECT * FROM EMP WHERE GENDER = 'F')"),
     *    <li>or any combination of the above.</ul>
     * @post return != null
     */
    private void convertFrom(
        Blackboard bb,
        SqlNode from)
    {
        switch (from.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            final SqlNode [] operands = ((SqlCall) from).getOperands();
            convertFrom(bb, operands[0]);
            return;
        case SqlKind.IdentifierORDINAL:
            final SqlValidator.Namespace fromNamespace =
                validator.getNamespace(from);
            RelOptTable table = getRelOptTable(fromNamespace, schema);
            bb.setRoot(table.toRel(cluster, connection));

            // REVIEW jvs 22-Jan-2004: This is adding a SqlNode as a
            // mapScopeToRel key.  Shouldn't it be a scope instead?
            mapScopeToRel.put(from, bb.root);
            leaves.add(bb.root);
            return;
        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            boolean isNatural = join.isNatural();
            SqlJoinOperator.JoinType joinType = join.getJoinType();
            final Blackboard leftBlackboard =
                new Blackboard(validator.getJoinScope(left));
            final Blackboard rightBlackboard =
                new Blackboard(validator.getJoinScope(right));
            convertFrom(leftBlackboard, left);
            RelNode leftRel = leftBlackboard.root;
            convertFrom(rightBlackboard, right);
            RelNode rightRel = rightBlackboard.root;
            if (isNatural) {
                throw Util.needToImplement("natural join");
            }
            int convertedJoinType = convertJoinType(joinType);
            if (convertedJoinType == JoinRel.JoinType.RIGHT) {
                // "class Join" does not support RIGHT, so swap...
                bb.setRoot(
                    createJoin(
                        bb,
                        rightRel,
                        leftRel,
                        join.getCondition(),
                        join.getConditionType(),
                        JoinRel.JoinType.LEFT));
            } else {
                bb.setRoot(
                    createJoin(
                        bb,
                        leftRel,
                        rightRel,
                        join.getCondition(),
                        join.getConditionType(),
                        convertedJoinType));
            }
            return;
        case SqlKind.SelectORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final RelNode rel = convertQueryRecursive(from);
            bb.setRoot(rel);
            return;
        case SqlKind.ValuesORDINAL:
            convertValues(bb, (SqlCall) from);
            return;
        case SqlKind.UnnestORDINAL:
            SqlCall call = (SqlCall) ((SqlCall) from).operands[0];
            final RelNode childRel;
            if (call.isA(SqlKind.MultisetValueConstructor)) {
                final SqlNodeList list = SqlUtil.toNodeList(call.operands);
                childRel =
                    new CollectRel(cluster, convertQueryOrInList(bb,list));
            } else if (call.isA(SqlKind.MultisetQueryConstructor)) {
                childRel = new CollectRel(
                    cluster, convertValidatedQuery(call.operands[0]));
            } else {
                childRel = convertValidatedQuery(call.operands[0]);
            }

            UncollectRel uncollectRel = new UncollectRel(cluster, childRel);
            bb.setRoot(uncollectRel);
            return;
        default:
            throw Util.newInternal("not a join operator " + from);
        }
    }

    private JoinRel createJoin(
        Blackboard bb,
        RelNode leftRel,
        RelNode rightRel,
        SqlNode condition,
        SqlJoinOperator.ConditionType conditionType,
        int joinType)
    {
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
                        new RelNode [] { leftRel, rightRel },
                        correlName);
                assert (expression != null);
            }
            cluster.query.mapDeferredToCorrel.clear();
        }

        // Make sure that left does not depend upon a correlating variable
        // coming from right. We'll swap them before we create a
        // JavaNestedLoopJoin.
        String [] variablesL2R =
            RelOptUtil.getVariablesSetAndUsed(rightRel, leftRel);
        String [] variablesR2L =
            RelOptUtil.getVariablesSetAndUsed(leftRel, rightRel);
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
        RexNode conditionExp =
            convertJoinCondition(bb, condition, conditionType, leftRel,
                rightRel);
        return new JoinRel(cluster, leftRel, rightRel, conditionExp, joinType,
            variablesStopped);
    }

    private RexNode convertJoinCondition(
        Blackboard bb,
        SqlNode condition,
        SqlJoinOperator.ConditionType conditionType,
        RelNode leftRel,
        RelNode rightRel)
    {
        if (condition == null) {
            return rexBuilder.makeLiteral(true);
        }
        switch (conditionType.getOrdinal()) {
        case SqlJoinOperator.ConditionType.On_ORDINAL:
            bb.setRoot(new RelNode [] { leftRel, rightRel });
            return convertExpression(bb, condition);
        case SqlJoinOperator.ConditionType.Using_ORDINAL:
            SqlNodeList list = (SqlNodeList) condition;
            RexNode conditionExp = null;
            for (int i = 0; i < list.size(); i++) {
                final SqlNode columnName = list.get(i);
                assert (columnName instanceof SqlIdentifier);
                RexNode exp = convertExpression(bb, columnName);
                if (i == 0) {
                    conditionExp = exp;
                } else {
                    conditionExp =
                        rexBuilder.makeCall(opTab.andOperator, conditionExp,
                            exp);
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
     * Converts the SELECT, GROUP BY and HAVING clauses of an aggregate query.
     * @param bb         Scope within which to resolve identifiers
     * @param groupList  GROUP BY clause, or null
     * @param having     HAVING clause
     * @param selectList SELECT list
     */
    private void convertAgg(
        Blackboard bb,
        SqlNodeList groupList,
        SqlNode having,
        SqlNodeList selectList)
    {
        assert bb.root != null : "precondition: child != null";
            final AggConverter aggConverter = new AggConverter(bb);

        // If group-by clause is missing, pretend that it has zero elements.
        if (groupList == null) {
            groupList = SqlNodeList.Empty;
        }

        // register the group exprs
        for (int i = 0; i < groupList.size(); i++) {
            aggConverter.addGroupExpr(groupList.get(i));
        }

        RexNode[] selectExprs = new RexNode[selectList.size()];
        RexNode havingExpr = null;
        try {
            Util.permAssert(bb.agg == null, "already in agg mode");
            bb.agg = aggConverter;
            // convert the the select and having expressions, so that the
            // agg converter knows which aggregations are required
            for (int i = 0; i < selectList.size(); i++) {
                SqlNode expr = selectList.get(i);
                selectExprs[i] = convertExpression(bb, expr);
            }

            if (having != null) {
                havingExpr = convertExpression(bb, having);
            }
        } finally {
            bb.agg = null;
        }

        // compute inputs to the aggregator
        RexNode[] preExprs = aggConverter.getPreExprs();
        bb.setRoot(
            new ProjectRel(
                cluster,
                bb.root,
                preExprs,
                null,
                ProjectRel.Flags.Boxed));

        // add the aggregator
        final AggregateRel.Call [] aggCalls = aggConverter.getAggCalls();
        bb.setRoot(
            new AggregateRel(
                cluster,
                bb.root,
                groupList.size(),
                aggCalls));

        // implement the SELECT list
        bb.setRoot(
            new ProjectRel(
                cluster,
                bb.root,
                selectExprs,
                null,
                ProjectRel.Flags.Boxed));

        // implement HAVING
        if (having != null) {
            bb.setRoot(
                new FilterRel(
                    cluster,
                    bb.root,
                    havingExpr));
        }
    }

    /**
     * Converts a {@link SqlLiteral SQL literal} to
     * a {@link RexLiteral REX literal}.
     *
     * <p>The result is {@link RexNode}, not {@link RexLiteral} because if the
     * literal is NULL (or the boolean Unknown value), we make a
     * <code>CAST(NULL AS type)</code> expression.
     */
    private RexNode convertLiteral(final SqlLiteral literal)
    {
        if (literal.getValue() == null) {
            RelDataType type;

            //Since there is no eq. RexLiteral of SqlLiteral.Unknown we
            //treat it as a cast(null as boolean)
            if (literal.typeName == SqlTypeName.Boolean) {
                type =
                    validator.typeFactory.createSqlType(SqlTypeName.Boolean);
                type =
                    validator.typeFactory.createTypeWithNullability(type, true);
            } else {
                type = validator.getValidatedNodeType(literal);
            }
            return rexBuilder.makeCast(
                type,
                rexBuilder.constantNull());
        } else {
            return convertNonNullLiteral(literal);
        }
    }

    /**
     * Converts a {@link SqlLiteral SQL literal} which we know not to be NULL
     * into a {@link RexLiteral REX literal}.
     * @param literal
     * @return
     */
    private RexLiteral convertNonNullLiteral(final SqlLiteral literal)
    {
        if (literal instanceof SqlSymbol) {
            return rexBuilder.makeSymbolLiteral((SqlSymbol) literal);
        }
        final Object value = literal.getValue();
        BitString bitString;
        switch (literal.typeName.ordinal) {
        case SqlTypeName.Decimal_ordinal:

            // exact number
            BigDecimal bd = (BigDecimal) value;
            return rexBuilder.makeExactLiteral(bd);
        case SqlTypeName.Double_ordinal:

            // approximate type
            // TODO:  preserve fixed-point precision and large integers
            return rexBuilder.makeApproxLiteral((BigDecimal) value);
        case SqlTypeName.Char_ordinal:
            return rexBuilder.makeCharLiteral((NlsString) value);
        case SqlTypeName.Boolean_ordinal:
            return rexBuilder.makeLiteral(((Boolean) value).booleanValue());
        case SqlTypeName.Binary_ordinal:
            bitString = (BitString) value;
            if ((bitString.getBitCount() % 8) == 0) {
                // An even number of hexits (e.g. X'ABCD') makes whole number
                // of bytes.
                byte [] bytes = bitString.getAsByteArray();
                return rexBuilder.makeBinaryLiteral(bytes);
            } else {
                // An odd number of hexits (e.g. X'ABC') leaves an unfinished
                // byte, so the whole thing is treated as a bit string.
                // (Yes, this is really what the standard asks for.)
                return rexBuilder.makeBitLiteral(bitString);
            }
        case SqlTypeName.Bit_ordinal:
            bitString = (BitString) value;
            return rexBuilder.makeBitLiteral(bitString);
        case SqlTypeName.Symbol_ordinal:
            return rexBuilder.makeSymbolLiteral((SqlSymbol) value);
        case SqlTypeName.Timestamp_ordinal:
            return rexBuilder.makeTimestampLiteral((Calendar) value,
                ((SqlTimestampLiteral) literal).precision);
        case SqlTypeName.Time_ordinal:
            return rexBuilder.makeTimeLiteral((Calendar) value,
                ((SqlTimeLiteral) literal).precision);
        case SqlTypeName.Date_ordinal:
            return rexBuilder.makeDateLiteral((Calendar) value);
        default:
            throw literal.typeName.unexpected();
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

        dynamicParamSqlNodes.set(dynamicParam.index, dynamicParam);
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
    private void convertOrder(
        Blackboard bb,
        SqlNodeList orderList)
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
                if (ordinalExp.typeName != SqlTypeName.Decimal) {
                    throw Util.needToImplement(ordinalExp);
                }

                // SQL ordinals are 1-based, but SortRel's are 0-based
                iOrdinal = sqlLiteral.intValue() - 1;
            } else if (orderItem.isA(SqlKind.Identifier)) {
                SqlIdentifier id = (SqlIdentifier) orderItem;
                iOrdinal =
                    bb.root.getRowType().getFieldOrdinal(id.getSimple());
                assert (iOrdinal != -1);
            } else {
                // TODO:  handle descending, collation sequence,
                // (and expressions, but flagged as non-standard)
                throw Util.needToImplement(orderItem);
            }
            assert (iOrdinal < bb.root.getRowType().getFieldList().size());
            collations[i] = new RelFieldCollation(iOrdinal);
        }
        bb.setRoot(new SortRel(cluster, bb.root, collations));
    }

    private RelNode convertQueryRecursive(SqlNode query)
    {
        final int kind = query.getKind().getOrdinal();
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
            final RelNode left = convertQueryRecursive(operands[0]);
            final RelNode right = convertQueryRecursive(operands[1]);
            boolean all = false;
            if (call.operator instanceof SqlSetOperator) {
                all = ((SqlSetOperator) (call.operator)).all;
            }
            switch (kind) {
            case SqlKind.UnionORDINAL:
                return new UnionRel(
                    cluster,
                    new RelNode [] { left, right },
                    all);
            case SqlKind.IntersectORDINAL:

                // TODO:  all
                return new IntersectRel(cluster, left, right);
            case SqlKind.ExceptORDINAL:
                throw Util.needToImplement(this);
            default:
                throw Util.newInternal("not a set operator "
                    + SqlKind.enumeration.getName(kind));
            }
        } else {
            throw Util.newInternal("not a query: " + query);
        }
    }

    private RelNode convertInsert(SqlInsert call)
    {
        SqlValidator.Namespace targetScope =
            validator.getNamespace(call.getTargetTable());
        RelNode sourceRel = convertQueryRecursive(call.getSourceSelect());
        RelOptTable targetTable = getRelOptTable(targetScope, schema);
        RelDataType lhsRowType = targetTable.getRowType();
        SqlNodeList targetColumnList = call.getTargetColumnList();

        RelDataType sourceRowType = sourceRel.getRowType();
        int nExps = lhsRowType.getFieldList().size();
        RexNode [] rhsExps = new RexNode[nExps];

        final RexNode sourceRef =
            rexBuilder.makeRangeReference(sourceRowType, 0);
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
                assert (iTarget != -1);
                rhsExps[iTarget] = rexBuilder.makeFieldAccess(sourceRef, iSrc);
            }

            for (int i = 0; i < nExps; ++i) {
                if (rhsExps[i] != null) {
                    continue;
                }

                rhsExps[i] =
                    defaultValueFactory.newDefaultValue(targetTable, i);
            }

            sourceRel =
                new ProjectRel(cluster, sourceRel, rhsExps, null,
                    ProjectRel.Flags.Boxed);
        }

        return new TableModificationRel(cluster, targetTable, connection,
            sourceRel, TableModificationRel.Operation.INSERT, null);
    }

    private RelNode convertDelete(SqlDelete call)
    {
        SqlIdentifier from = call.getTargetTable();
        SqlValidator.Namespace targetNamespace = validator.getNamespace(from);
        RelOptTable targetTable = getRelOptTable(targetNamespace, schema);
        RelNode sourceRel = convertSelect(call.getSourceSelect());
        return new TableModificationRel(cluster, targetTable, connection,
            sourceRel, TableModificationRel.Operation.DELETE, null);
    }

    private RelNode convertUpdate(SqlUpdate call)
    {
        SqlIdentifier from = call.getTargetTable();
        SqlValidator.Namespace targetNamespace = validator.getNamespace(from);
        RelOptTable targetTable = getRelOptTable(targetNamespace, schema);

        // convert update column list from SqlIdentifier to String
        List targetColumnNameList = new ArrayList();
        List targetColumnList = call.getTargetColumnList().getList();
        Iterator targetColumnIter = targetColumnList.iterator();
        while (targetColumnIter.hasNext()) {
            SqlIdentifier id = (SqlIdentifier) targetColumnIter.next();
            String name = id.getSimple();
            targetColumnNameList.add(name);
        }

        RelNode sourceRel = convertSelect(call.getSourceSelect());

        return new TableModificationRel(cluster, targetTable, connection,
            sourceRel, TableModificationRel.Operation.UPDATE,
            targetColumnNameList);
    }

    /**
     * Converts an identifier into an expression in a given scope. For
     * example, the "empno" in "select empno from emp join dept" becomes
     * "emp.empno".
     */
    private RexNode convertIdentifier(
        Blackboard bb,
        SqlIdentifier identifier)
    {
        // first check for reserved identifiers like CURRENT_USER
        final SqlCall call = validator.makeCall(identifier);
        if (call != null) {
            return convertExpression(bb, call);
        }

        if (bb.agg != null) {
            throw Util.newInternal("Identifier '" + identifier +
                "' is not a group expr");
        }

        if (bb.scope != null) {
            identifier = bb.scope.fullyQualify(identifier);
        }
        RexNode e = bb.lookupExp(identifier.names[0]);
        for (int i = 1; i < identifier.names.length; i++) {
            String name = identifier.names[i];
            e = rexBuilder.makeFieldAccess(e, name);
        }
        if (e instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) e;

            // adjust the type to account for nulls introduced by outer joins
            RelDataTypeField field = bb.getRootField(inputRef);
            if (field != null) {
                e = rexBuilder.makeInputRef(
                        field.getType(),
                        inputRef.index);
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
    private RelNode convertRowConstructor(
        Blackboard bb,
        SqlCall rowConstructor)
    {
        assert isRowConstructor(rowConstructor) : "precondition: isRowConstructor(rowConstructor), was: "
        + rowConstructor;

        final SqlNode [] operands = rowConstructor.getOperands();
        RexNode [] selectList = new RexNode[operands.length];
        String [] fieldNames = new String[operands.length];
        for (int i = 0; i < operands.length; ++i) {
            RexNode value = convertExpression(bb, operands[i]);
            selectList[i] = value;
            fieldNames[i] = validator.deriveAlias(operands[i], i);
        }

        // SELECT value-list FROM onerow
        final OneRowRel oneRow = new OneRowRel(cluster);
        return new ProjectRel(cluster, oneRow, selectList, fieldNames,
            ProjectRel.Flags.Boxed);
    }

    private void convertSelectList(
        Blackboard bb,
        SqlNodeList selectList, SqlSelect select)
    {
        selectList = validator.expandStar(selectList, select);
        replaceSubqueries(bb, selectList);
        String [] fieldNames = new String[selectList.size()];
        RexNode [] exps = new RexNode[selectList.size()];
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode node = selectList.get(i);
            exps[i] = convertExpression(bb, node);
            fieldNames[i] = validator.deriveAlias(node, i);
        }
        bb.setRoot(
            new ProjectRel(cluster, bb.root, exps, fieldNames,
                ProjectRel.Flags.Boxed));
    }

    /**
     * Converts a values clause (as in "INSERT INTO T(x,y) VALUES (1,2)")
     * into a relational expression.
     */
    private void convertValues(
        Blackboard bb,
        SqlCall values)
    {
        SqlNode [] rowConstructorList = values.getOperands();
        ArrayList unionRels = new ArrayList();
        for (int i = 0; i < rowConstructorList.length; i++) {
            SqlCall rowConstructor = (SqlCall) rowConstructorList[i];
            RelNode queryExpr = convertRowConstructor(bb, rowConstructor);
            unionRels.add(queryExpr);
        }

        if (unionRels.size() == 0) {
            throw Util.newInternal("empty values clause");
        } else if (unionRels.size() == 1) {
            bb.setRoot((RelNode) unionRels.get(0));
        } else {
            bb.setRoot(
                new UnionRel(cluster,
                    (RelNode []) unionRels.toArray(new RelNode[0]), true));
        }
        leaves.add(bb.root);

        // REVIEW jvs 22-Jan-2004:  should I add
        // mapScopeToRel.put(validator.getScope(values),bb.root);
        // ?
    }

    RelOptCluster createCluster(Environment env)
    {
        RelOptQuery query;
        if (cluster == null) {
            query = new RelOptQuery(planner);
        } else {
            query = cluster.query;
        }
        final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        return query.createCluster(env, typeFactory, rexBuilder);
    }

    /**
     * Converts a scope into a {@link RelOptTable}. This is only possible if
     * the scope represents an identifier, such as "sales.emp". Otherwise,
     * returns null.
     */
    public static RelOptTable getRelOptTable(
        SqlValidator.Namespace namespace,
        RelOptSchema schema)
    {
        if (namespace instanceof SqlValidator.IdentifierNamespace) {
            SqlValidator.IdentifierNamespace identifierNamespace =
                (SqlValidator.IdentifierNamespace) namespace;
            final String [] names = identifierNamespace.id.names;
            return schema.getTableForMember(names);
        } else {
            return null;
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A <code>SchemaCatalogReader</code> looks up catalog information from a
     * {@link org.eigenbase.relopt.RelOptSchema schema object}.
     */
    public static class SchemaCatalogReader
        implements SqlValidator.CatalogReader
    {
        private final RelOptSchema schema;
        private final boolean upperCase;

        public SchemaCatalogReader(
            RelOptSchema schema,
            boolean upperCase)
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
                schema.getTableForMember(
                    new String [] { maybeUpper(names[0]) });
            if (table != null) {
                return new SqlValidator.Table() {
                        public RelDataType getRowType()
                        {
                            return table.getRowType();
                        }

                        public String [] getQualifiedName()
                        {
                            return null;
                        }
                    };
            }
            return null;
        }

        public ArrayList getAllSchemaNames() { return null;}
        public ArrayList getAllSchemaNames(String catalogName) { return null;}
        public ArrayList getAllTableNames(String schemaName) { return null;}
        public ArrayList getAllTableNames(String catalogName, String schemaName)            {return null;}
        public ArrayList getAllTables() {return null;}

        private String maybeUpper(String s)
        {
            return upperCase ? s.toUpperCase() : s;
        }
    }

    /**
     * Workspace for translating an individual SELECT statement (or sub-SELECT).
     */
    private class Blackboard
    {
        /** Collection of {@link RelNode} objects which correspond to a
         * SELECT statement. */
        final SqlValidator.Scope scope;
        private RelNode root;
        private RelNode [] inputs;

        /** List of <code>IN</code> and <code>EXISTS</code> nodes inside this
         * <code>SELECT</code> statement (but not inside sub-queries). */
        private final ArrayList subqueries = new ArrayList();

        /** Maps IN and EXISTS {@link SqlSelect sub-queries} to the expressions
         * which will be used to access them. */
        private final HashMap mapSubqueryToExpr = new HashMap();
        /**
         * Workspace for building aggregates.
         */
        public AggConverter agg;

        /**
         * Creates a Blackboard
         *
         * @param scope Name-resolution scope for expressions validated within
         *   this query. Can be null if this Blackboard is for a leaf node,
         *   say the "emp" identifier in "SELECT * FROM emp".
         */
        Blackboard(SqlValidator.Scope scope)
        {
            this.scope = scope;
        }

        /**
         * Registers a relational expression
         *
         * @param rel Relational expression
         * @return Expression with which to refer to the row (or partial row)
         *   coming from this relational expression's side of the join.
         */
        public RexNode register(RelNode rel)
        {
            if (root == null) {
                setRoot(rel);
                return rexBuilder.makeRangeReference(
                    root.getRowType(),
                    0);
            } else {
                final JoinRel join =
                    new JoinRel(
                        rel.getCluster(),
                        root,
                        rel,
                        rexBuilder.makeLiteral(true),
                        JoinRel.JoinType.LEFT,
                        Collections.EMPTY_SET);
                setRoot(join);
                return rexBuilder.makeRangeReference(
                    rel.getRowType(),
                    join.getLeft().getRowType().getFieldList().size());
            }
        }

        void setRoot(RelNode root)
        {
            this.root = root;
            this.inputs = new RelNode [] { root };
        }

        void setRoot(RelNode [] inputs)
        {
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
            final SqlValidator.Scope [] ancestorScopes =
                new SqlValidator.Scope[1];
            SqlValidator.Namespace foundNs =
                scope.resolve(name, ancestorScopes, offsets);
            if (foundNs == null) {
                return null;
            }

            // Found in current query's from list.  Find which from item.
            // We assume that the order of the from clause items has been
            // preserved.
            SqlValidator.Scope ancestorScope = ancestorScopes[0];
            boolean isParent = ancestorScope != scope;
            int offset = offsets[0];
            RexNode result;
            if ((inputs != null) && !isParent) {
                final RelNode [] rels =
                    isParent
                    ? new RelNode [] { (RelNode) mapScopeToRel.get(ancestorScope) }
                    : inputs;
                result = lookup(offset, rels, isParent, null);
            } else {
                // We're referencing a relational expression which has not
                // been converted yet. This occurs when from items are
                // correlated, e.g. "select from emp as emp join emp.getDepts()
                // as dept". Create a temporary expression.
                assert isParent;
                DeferredLookup lookup =
                    new DeferredLookup(this, offset, isParent);
                String correlName =
                    cluster.query.createCorrelUnresolved(lookup);
                final RelDataType rowType = foundNs.getRowType();
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
            RelNode [] inputs,
            boolean isParent,
            String varName)
        {
            final ArrayList relList = flatten(inputs);
            if ((offset < 0) || (offset >= relList.size())) {
                throw Util.newInternal("could not find input " + offset);
            }
            int fieldOffset = 0;
            for (int i = 0; i < offset; i++) {
                final RelNode rel = (RelNode) relList.get(i);
                fieldOffset += rel.getRowType().getFieldList().size();
            }
            RelNode rel = (RelNode) relList.get(offset);
            if (isParent) {
                if (varName == null) {
                    varName = rel.getOrCreateCorrelVariable();
                } else {
                    // we are resolving a forward reference
                    rel.registerCorrelVariable(varName);
                }
                return rexBuilder.makeCorrel(
                    rel.getRowType(),
                    varName);
            } else {
                return rexBuilder.makeRangeReference(
                    rel.getRowType(),
                    fieldOffset);
            }
        }

        RelDataTypeField getRootField(RexInputRef inputRef)
        {
            int fieldOffset = inputRef.index;
            for (int i = 0; i < inputs.length; ++i) {
                RelDataType rowType = inputs[i].getRowType();
                if (rowType == null) {
                    // TODO:  remove this once leastRestrictive
                    // is correctly implemented
                    return null;
                }
                if (fieldOffset < rowType.getFieldList().size()) {
                    return rowType.getFields()[fieldOffset];
                }
                fieldOffset -= rowType.getFieldList().size();
            }
            throw new AssertionError();
        }

        private ArrayList flatten(RelNode [] rels)
        {
            ArrayList list = new ArrayList();
            flatten(rels, list);
            return list;
        }

        private void flatten(
            RelNode [] rels,
            ArrayList list)
        {
            for (int i = 0; i < rels.length; i++) {
                RelNode rel = rels[i];
                if (leaves.contains(rel)) {
                    list.add(rel);
                } else {
                    flatten(
                        rel.getInputs(),
                        list);
                }
            }
        }

        public void registerSubquery(SqlNode node)
        {
            subqueries.add(node);
        }
    }

    /**
     * Contains the information necessary to repeat a call to
     * {@link Blackboard#lookup}.
     */
    static class DeferredLookup
    {
        Blackboard bb;
        boolean isParent;
        int offset;

        DeferredLookup(
            Blackboard bb,
            int offset,
            boolean isParent)
        {
            this.bb = bb;
            this.offset = offset;
            this.isParent = isParent;
        }

        RexNode lookup(
            RelNode [] inputs,
            String varName)
        {
            return bb.lookup(offset, inputs, isParent, varName);
        }
    }

    /**
     * An implementation of DefaultValueFactory which always supplies NULL.
     */
    class NullDefaultValueFactory implements DefaultValueFactory
    {
        public RexNode newDefaultValue(
            RelOptTable table,
            int iColumn)
        {
            return rexBuilder.constantNull();
        }
    }

    /**
     * Converts expressions to aggregates.
     *
     * <p>Consider the expression
     *
     * SELECT deptno, SUM(2 * sal)
     * FROM emp
     * GROUP BY deptno
     *
     * Then<ul>
     * <li>groupExprs = {SqlIdentifier(deptno)}</li>
     * <li>convertedInputExprs = {RexInputRef(deptno),
     *                            2 * RefInputRef(sal)}</li>
     * <li>inputRefs = {RefInputRef(#0),
     *                  RexInputRef(#1)}</li>
     * <li>aggCalls = {AggCall(SUM, {1})}</li>
     * </ul>
     */
    class AggConverter
    {
        private final Blackboard bb;
        /**
         * The group-by expressions, in {@link SqlNode} format.
         */
        private final SqlNodeList groupExprs =
            new SqlNodeList(ParserPosition.ZERO);
        /**
         * Input expressions for the group columns and aggregates, in
         * {@link RexNode} format. The first elements of the list correspond
         * to the elements in {@link #groupExprs}; the remaining elements are
         * for aggregates.
         */
        private final ArrayList convertedInputExprs = new ArrayList();
        private final ArrayList inputRefs = new ArrayList();
        private final ArrayList aggCalls = new ArrayList();

        /**
         * Input expressions required by aggregates,
         * @param bb
         */

        public AggConverter(Blackboard bb)
        {
            this.bb = bb;
        }

        public void addGroupExpr(SqlNode expr)
        {
            RexNode convExpr = convertExpression(bb, expr);
            final int index = groupExprs.size();
            groupExprs.add(expr);
            convertedInputExprs.add(convExpr);
            inputRefs.add(rexBuilder.makeInputRef(convExpr.getType(), index));
        }

        public RexNode convertCall(SqlCall call)
        {
            assert call.operator.isAggregator();
            assert bb.agg == this;
            int[] args = new int[call.operands.length];
            try {
                // switch out of agg mode
                bb.agg = null;
                for (int i = 0; i < call.operands.length; i++) {
                    SqlNode operand = call.operands[i];
                    final RexNode convertedExpr = convertExpression(bb, operand);
                    args[i] = lookupOrCreateGroupExpr(convertedExpr);
                }
            } finally {
                // switch back into agg mode
                bb.agg = this;
            }
            final Aggregation aggregation = (Aggregation) call.operator;
            RelDataType type = validator.getValidatedNodeType(call);
            final AggregateRel.Call aggCall =
                new AggregateRel.Call(aggregation, args, type);
            int index = aggCalls.size();
            aggCalls.add(aggCall);
            final RexNode rex = rexBuilder.makeInputRef(type, index);
            return rex;
        }

        private int lookupOrCreateGroupExpr(RexNode expr)
        {
            for (int i = 0; i < convertedInputExprs.size(); i++) {
                RexNode convertedInputExpr = (RexNode) convertedInputExprs.get(i);
                if (expr.equals(convertedInputExpr)) {
                    return i;
                }
            }
            // not found -- add it
            int index = convertedInputExprs.size();
            convertedInputExprs.add(expr);
            return index;
        }

        /**
         * If an expression is structurally identical to one of the group-by
         * expressions, returns a reference to the expression, otherwise
         * returns null.
         */
        public RexNode lookupGroupExpr(SqlNode expr)
        {
            for (int i = 0; i < groupExprs.size(); i++) {
                SqlNode groupExpr = groupExprs.get(i);
                if (expr.equalsDeep(groupExpr)) {
                    return (RexNode) inputRefs.get(i);
                }
            }
            return null;
        }

        public RexNode[] getPreExprs()
        {
            return (RexNode[]) convertedInputExprs.toArray(
                new RexNode[convertedInputExprs.size()]);
        }

        public AggregateRel.Call[] getAggCalls()
        {
            return (AggregateRel.Call[])
                aggCalls.toArray(new AggregateRel.Call[aggCalls.size()]);
        }
    }

}


// End SqlToRelConverter.java
