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
package org.eigenbase.sql2rel;

import java.math.*;

import java.util.*;

import openjava.mop.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Converts a SQL parse tree (consisting of {@link org.eigenbase.sql.SqlNode}
 * objects) into a relational algebra expression (consisting of {@link
 * org.eigenbase.rel.RelNode} objects).
 *
 * <p>The public entry points are: {@link #convertQuery}, {@link
 * #convertExpression(SqlNode)}.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 10, 2003
 * @testcase
 */
public class SqlToRelConverter
{

    //~ Instance fields --------------------------------------------------------

    protected final SqlValidator validator;
    protected final RexBuilder rexBuilder;
    private final RelOptPlanner planner;
    private final RelOptConnection connection;
    protected final RelOptSchema schema;
    protected final RelOptCluster cluster;
    private final Map<SqlValidatorScope, LookupContext> mapScopeToLux =
        new HashMap<SqlValidatorScope, LookupContext>();
    private DefaultValueFactory defaultValueFactory;
    protected final List<RelNode> leaves = new ArrayList<RelNode>();
    private final List<SqlDynamicParam> dynamicParamSqlNodes =
        new ArrayList<SqlDynamicParam>();
    private final SqlOperatorTable opTab;
    private boolean shouldConvertTableAccess;
    protected final RelDataTypeFactory typeFactory;
    private final SqlNodeToRexConverter exprConverter;

    /**
     * Stack of names of datasets requested by the <code>
     * TABLE(SAMPLE(&lt;datasetName&gt;, &lt;query&gt;))</code> construct.
     */
    private final Stack<String> datasetStack = new Stack<String>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a converter.
     *
     * @param validator
     * @param schema
     * @param env
     * @param connection
     * @param rexBuilder
     *
     * @pre connection != null
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
        this.opTab =
            (validator
                == null) ? SqlStdOperatorTable.instance()
            : validator.getOperatorTable();
        this.validator = validator;
        this.schema = schema;
        this.planner = planner;
        this.connection = connection;
        this.defaultValueFactory = new NullDefaultValueFactory();
        this.rexBuilder = rexBuilder;
        this.typeFactory = rexBuilder.getTypeFactory();
        this.cluster = createCluster(env);
        this.shouldConvertTableAccess = true;
        this.exprConverter =
            new SqlNodeToRexConverterImpl(new StandardConvertletTable());
    }

    //~ Methods ----------------------------------------------------------------

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
     * Set a new DefaultValueFactory. To have any effect, this must be called
     * before any convert method.
     *
     * @param factory new DefaultValueFactory
     */
    public void setDefaultValueFactory(DefaultValueFactory factory)
    {
        defaultValueFactory = factory;
    }

    /**
     * Controls whether table access references are converted to physical rels
     * immediately. The optimizer doesn't like leaf rels to have {@link
     * CallingConvention#NONE}. However, if we are doing further conversion
     * passes (e.g. {@link RelStructuredTypeFlattener}), then we may need to
     * defer conversion. To have any effect, this must be called before any
     * convert method.
     *
     * @param enabled true for immediate conversion (the default); false to
     * generate logical TableAccessRel instances
     */
    public void enableTableAccessConversion(boolean enabled)
    {
        shouldConvertTableAccess = enabled;
    }

    /**
     * Converts an unvalidated query's parse tree into a relational expression.
     *
     * @param query Query to convert
     * @param needsValidation Whether to validate the query before converting;
     * <code>false</code> if the query has already been validated.
     * @param top Whether the query is top-level, say if its result will become
     * a JDBC result set; <code>false</code> if the query will be part of a
     * view.
     */
    public RelNode convertQuery(
        SqlNode query,
        final boolean needsValidation,
        final boolean top)
    {
        if (needsValidation) {
            query = validator.validate(query);
        }
        final RelNode result = convertQueryRecursive(query, top);

        if (!needsValidation
            && !query.getKind().isA(SqlKind.Dml)) {
            // Verify that conversion from SQL to relational algebra did
            // not perturb any type information.  (We can't do this if the
            // SQL statement is something like an INSERT which has no
            // validator type information associated with its result,
            // hence the namespace check above.)
            RelDataType validatedRowType =
                validator.getValidatedNodeType(query);
            validatedRowType = uniquifyFields(validatedRowType);
            RelDataType convertedRowType = result.getRowType();
            if (!RelOptUtil.equal(
                    "validated row type",
                    validatedRowType,
                    "converted row type",
                    convertedRowType,
                    false)) {
                throw Util.newInternal(
                    "Conversion to relational algebra failed to preserve "
                    + "datatypes:" + Util.lineSeparator
                    + "validated type:" + Util.lineSeparator
                    + validatedRowType.getFullTypeString() + Util.lineSeparator
                    + "converted type:" + Util.lineSeparator
                    + convertedRowType.getFullTypeString() + Util.lineSeparator
                    + "rel:" + Util.lineSeparator
                    + RelOptUtil.toString(result));
            }
        }
        return result;
    }

    private RelDataType uniquifyFields(RelDataType rowType)
    {
        final List<String> fieldNameList = RelOptUtil.getFieldNameList(rowType);
        final List<RelDataType> fieldTypeList =
            RelOptUtil.getFieldTypeList(rowType);
        SqlValidatorUtil.uniquify(fieldNameList);
        return
            validator.getTypeFactory().createStructType(
                fieldTypeList,
                fieldNameList);
    }

    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public RelNode convertSelect(SqlSelect select)
    {
        final SqlValidatorScope selectScope = validator.getWhereScope(select);
        final Blackboard bb = createBlackboard(selectScope);
        convertSelectImpl(bb, select);
        mapScopeToLux.put(
            bb.scope,
            new LookupContext(bb.root));
        return bb.root;
    }

    /**
     * Factory method for creating translation workspace.
     */
    protected Blackboard createBlackboard(SqlValidatorScope scope)
    {
        return new Blackboard(scope);
    }

    /**
     * Implementation of {@link #convertSelect(SqlSelect)}; derived class may
     * override.
     */
    protected void convertSelectImpl(
        final Blackboard bb,
        SqlSelect select)
    {
        convertFrom(
            bb,
            select.getFrom());
        convertWhere(
            bb,
            select.getWhere());

        List<SqlNode> orderExprList = new ArrayList<SqlNode>();
        List<RelFieldCollation> collationList =
            new ArrayList<RelFieldCollation>();
        gatherOrderExprs(
            bb,
            select,
            select.getOrderList(),
            orderExprList,
            collationList);

        if (validator.isAggregate(select)) {
            convertAgg(
                bb,
                select.getGroup(),
                select.getHaving(),
                select.getSelectList(),
                orderExprList);
        } else {
            convertSelectList(
                bb,
                select.getSelectList(),
                orderExprList,
                select);
        }
        if (select.isDistinct()) {
            bb.setRoot(
                RelOptUtil.createDistinctRel(bb.root),
                false);
        }
        convertOrder(select, bb, collationList, orderExprList);
        bb.setRoot(bb.root, true);
    }

    private void convertOrder(
        SqlSelect select,
        final Blackboard bb,
        List<RelFieldCollation> collationList,
        List<SqlNode> orderExprList)
    {
        if (select.getOrderList() == null) {
            return;
        }

        // Create a sorter using the previously constructed collations.
        bb.setRoot(
            new SortRel(
                cluster,
                bb.root,
                collationList.toArray(
                    new RelFieldCollation[collationList.size()])),
            false);

        // If extra exressions were added to the project list for sorting,
        // add another project to remove them.
        if (orderExprList.size() > 0) {
            List<RexNode> exprs = new ArrayList<RexNode>();
            List<String> names = new ArrayList<String>();
            final RelDataType rowType = bb.root.getRowType();
            int fieldCount = rowType.getFieldCount() - orderExprList.size();
            for (int i = 0; i < fieldCount; i++) {
                exprs.add(RelOptUtil.createInputRef(bb.root, i));
                names.add(rowType.getFields()[i].getName());
            }
            bb.setRoot(
                CalcRel.createProject(bb.root, exprs, names),
                false);
        }
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
        final RexNode convertedWhere = bb.convertExpression(where);
        bb.setRoot(
            CalcRel.createFilter(bb.root, convertedWhere),
            false);
    }

    private void replaceSubqueries(
        final Blackboard bb,
        final SqlNode expr)
    {
        findSubqueries(bb, expr);
        for (SqlNode node : bb.subqueryList) {
            substituteSubquery(bb, node);
        }
    }

    private void substituteSubquery(Blackboard bb, SqlNode node)
    {
        JoinRelType joinType = JoinRelType.INNER;
        SqlNode leftJoinKeysForIn = null;
        final RexNode expr = bb.mapSubqueryToExpr.get(node);
        if (expr != null) {
            // Already done.
            return;
        }
        RelNode converted;
        switch (node.getKind().getOrdinal()) {
        case SqlKind.CursorConstructorORDINAL:
            convertCursor(bb, (SqlCall) node);
            return;
        case SqlKind.MultisetQueryConstructorORDINAL:
        case SqlKind.MultisetValueConstructorORDINAL: {
            converted = convertMultisets(
                    new SqlNode[] { node },
                    bb);
            break;
        }
        case SqlKind.InORDINAL: {
            SqlCall call = (SqlCall) node;
            final SqlNode [] operands = call.getOperands();
            leftJoinKeysForIn = operands[0];
            final SqlNode seek = operands[1];
            if (seek instanceof SqlNodeList) {
                SqlNodeList valuesList = (SqlNodeList) seek;
                if (valuesList.size() < getInSubqueryThreshold()) {
                    // We're under the threshold, so convert to OR.
                    RexNode expression =
                        convertInToOr(
                            bb,
                            leftJoinKeysForIn,
                            valuesList);
                    bb.mapSubqueryToExpr.put(node, expression);
                    return;
                } else {
                    // Otherwise, let convertExists translate
                    // values list into an inline table for the
                    // reference to Q below.
                }
            }

            // FIXME jvs 30-Apr-2006:  Null semantics are incorrect here.

            // "select from emp where emp.deptno in (Q)"
            //
            // becomes
            //
            // "select from
            //   emp join (select distinct * from (Q)) q
            //   on emp.deptno = q.col1"
            converted = convertExists(
                    bb,
                    seek,
                    null,
                    null,
                    null);
            converted = RelOptUtil.createDistinctRel(converted);
            break;
        }
        case SqlKind.ExistsORDINAL: {
            // FIXME jvs 30-Apr-2006: This is wrong; have to deal with
            // duplicates from Q returning more than one row.

            // "select from emp where exists (Q)"
            // becomes
            // "select from emp left join (select *, TRUE from (Q)) q
            //   where q.t is true"
            SqlCall call = (SqlCall) node;
            SqlSelect select = (SqlSelect) call.getOperands()[0];
            converted =
                convertExists(
                    bb,
                    select,
                    null,
                    rexBuilder.makeLiteral(true),
                    "$indicator");
            joinType = JoinRelType.LEFT;
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
        final RexNode expression =
            bb.register(
                converted,
                joinType,
                leftJoinKeysForIn);
        bb.mapSubqueryToExpr.put(node, expression);
    }

    /**
     * Converts "x IN (1, 2, ...)" to "x=1 OR x=2 OR ...".
     *
     * @param leftKeys LHS
     * @param valuesList RHS
     *
     * @return converted expression
     */
    private RexNode convertInToOr(
        Blackboard bb,
        SqlNode leftKeys,
        SqlNodeList valuesList)
    {
        RexNode result = null;
        for (SqlNode rightVals : valuesList) {
            // REVIEW jvs 1-May-2006:  Normalize row types here?  Also,
            // do we need to convert leftKeys over and over, or can
            // we just do it once and alias?
            RexNode rexComparison =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.equalsOperator,
                    bb.convertExpression(leftKeys),
                    bb.convertExpression(rightVals));
            if (result == null) {
                result = rexComparison;
            } else {
                // TODO jvs 1-May-2006: Generalize
                // RexUtil.andRexNodeList and use that.
                result =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.orOperator,
                        result,
                        rexComparison);
            }
        }
        assert (result != null);
        return result;
    }

    /**
     * Gets the list size threshold under which {@link #convertInToOr} is used.
     * Lists of this size or greater will instead be converted to use a join
     * against an inline table ({@link ValuesRel}) rather than a predicate. A
     * threshold of 0 forces usage of an inline table in all cases; a threshold
     * of Integer.MAX_VALUE forces usage of OR in all cases
     *
     * @return threshold, default 20
     */
    protected int getInSubqueryThreshold()
    {
        return 20;
    }

    /**
     * Creates the condition for a join implementing an IN clause.
     *
     * @param bb blackboard to use
     * @param keys LHS of IN
     * @param rightOffset offset for references to fields on RHS
     *
     * @return join condition
     */
    private RexNode createJoinConditionForIn(
        Blackboard bb,
        SqlNode keys,
        int rightOffset)
    {
        List<RexNode> conditions = new ArrayList<RexNode>();
        final RexNode ref =
            rexBuilder.makeRangeReference(
                bb.root.getRowType(),
                0,
                false);

        if (keys instanceof SqlNodeList) {
            // If "seek" is "(emp,dept)", generate the condition "emp = Q.c1
            // and dept = Q.c2".
            SqlNodeList keyList = (SqlNodeList) keys;
            for (int i = 0; i < keyList.size(); i++) {
                SqlNode conditionNode = keyList.get(i);
                conditions.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.equalsOperator,
                        bb.convertExpression(conditionNode),
                        rexBuilder.makeFieldAccess(ref, rightOffset + i)));
            }
        } else {
            // If "seek" is "emp", generate the condition "emp = Q.c1".
            conditions.add(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.equalsOperator,
                    bb.convertExpression(keys),
                    rexBuilder.makeFieldAccess(ref, rightOffset + 0)));
        }
        return RexUtil.andRexNodeList(rexBuilder, conditions);
    }

    /**
     * Converts an EXISTS or IN predicate into a join. For EXISTS, the subquery
     * produces an indicator variable, and the result is a relational expression
     * which outer joins that indicator to the original query. After performing
     * the outer join, the condition will be TRUE if the EXISTS condition holds,
     * NULL otherwise.
     *
     * <p>FIXME jvs 1-May-2006: null semantics for IN are currently broken
     *
     * @param bb
     * @param seek A query, for example 'select * from emp' or 'values (1,2,3)'
     * or '('Foo', 34)'.
     * @param condition May be null, or a node, or a node list
     * @param extraExpr Column expression to add, or null for none. "TRUE" for
     * EXISTS, null for IN
     * @param extraName Name of expression to add, or null for none
     *
     * @return join expression
     *
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
        final SqlValidatorScope seekScope =
            (seek instanceof SqlSelect)
            ? validator.getSelectScope((SqlSelect) seek)
            : null;
        final Blackboard seekBb = createBlackboard(seekScope);
        RelNode seekRel = convertQueryOrInList(seekBb, seek);
        List<RexNode> conditions = new ArrayList<RexNode>();
        if (condition != null) {
            // We are translating an IN clause, so add a condition.
            final RexNode ref =
                rexBuilder.makeRangeReference(
                    bb.root.getRowType(),
                    0,
                    false);
            if (condition instanceof SqlNodeList) {
                // If "seek" is "(emp,dept)", generate the condition "emp = Q.c1
                // and dept = Q.c2".
                SqlNodeList conditionList = (SqlNodeList) condition;
                for (int i = 0; i < conditionList.size(); i++) {
                    SqlNode conditionNode = conditionList.get(i);
                    conditions.add(
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.equalsOperator,
                            bb.convertExpression(conditionNode),
                            rexBuilder.makeFieldAccess(ref, i)));
                }
            } else {
                // If "seek" is "emp", generate the condition "emp = Q.c1". The
                // query must have precisely one column.
                assert seekRel.getRowType().getFieldCount() == 1;
                conditions.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.equalsOperator,
                        bb.convertExpression(condition),
                        rexBuilder.makeFieldAccess(ref, 0)));
            }
        }
        return
            RelOptUtil.createExistsPlan(
                cluster,
                seekRel,
                RexUtil.emptyExpressionArray,
                extraExpr,
                extraName);
    }

    private RelNode convertQueryOrInList(
        Blackboard bb,
        SqlNode seek)
    {
        // NOTE: Once we start accepting single-row queries as row constructors,
        // there will be an ambiguity here for a case like X IN ((SELECT Y FROM
        // Z)).  The SQL standard resolves the ambiguity by saying that a lone
        // select should be interpreted as a table expression, not a row
        // expression.  The semantic difference is that a table expression can
        // return multiple rows.
        if (seek instanceof SqlNodeList) {
            // NOTE jvs 30-Apr-2006: We combine all rows consisting entirely of
            // literals into a single ValuesRel; this gives the optimizer a
            // smaller input tree.  For everything else (computed expressions,
            // row subqueries), we union each row in as a projection on top of a
            // OneRowRel.

            List<List<RexLiteral>> tupleList =
                new ArrayList<List<RexLiteral>>();
            RelDataType rowType = validator.getValidatedNodeType(seek);
            rowType = SqlTypeUtil.promoteToRowType(
                    typeFactory,
                    rowType,
                    null);

            SqlNodeList nodeList = (SqlNodeList) seek;
            List<RelNode> unionInputs = new ArrayList<RelNode>();
            for (SqlNode node : nodeList) {
                SqlCall call;
                if (isRowConstructor(node)) {
                    call = (SqlCall) node;
                    boolean allLiteral = true;
                    List<RexLiteral> tuple = new ArrayList<RexLiteral>();
                    int i = 0;
                    for (SqlNode operand : call.operands) {
                        if (!(operand instanceof SqlLiteral)) {
                            tuple = null;
                            break;
                        }
                        RexLiteral rexLiteral =
                            (RexLiteral) exprConverter.convertLiteral(
                                bb,
                                (SqlLiteral) operand);
                        rexLiteral = coerceLiteral(
                                rexLiteral,
                                rowType,
                                i++);
                        tuple.add(rexLiteral);
                    }
                    if (tuple != null) {
                        tupleList.add(tuple);
                        continue;
                    }
                } else {
                    if (node instanceof SqlLiteral) {
                        RexLiteral rexLiteral =
                            (RexLiteral) exprConverter.convertLiteral(
                                bb,
                                (SqlLiteral) node);
                        rexLiteral = coerceLiteral(
                                rexLiteral,
                                rowType,
                                0);
                        tupleList.add(
                            Collections.singletonList(rexLiteral));
                        continue;
                    }

                    // convert "1" to "row(1)"
                    call =
                        SqlStdOperatorTable.rowConstructor.createCall(
                            new SqlNode[] { node },
                            SqlParserPos.ZERO);
                }
                unionInputs.add(convertRowConstructor(bb, call));
            }
            ValuesRel valuesRel = new ValuesRel(
                    cluster,
                    rowType,
                    tupleList);
            RelNode resultRel;
            if (unionInputs.isEmpty()) {
                resultRel = valuesRel;
            } else {
                if (!tupleList.isEmpty()) {
                    unionInputs.add(valuesRel);
                }
                UnionRel unionRel =
                    new UnionRel(
                        cluster,
                        unionInputs.toArray(RelNode.emptyArray),
                        true);
                resultRel = unionRel;
            }
            leaves.add(resultRel);
            return resultRel;
        } else {
            return convertQueryRecursive(seek, false);
        }
    }

    private RexLiteral coerceLiteral(
        RexLiteral literal,
        RelDataType rowType,
        int iField)
    {
        RelDataTypeField field =
            (RelDataTypeField) rowType.getFieldList().get(iField);
        RelDataType type = field.getType();
        if (!SqlTypeUtil.isExactNumeric(type)) {
            return literal;
        }
        BigDecimal value = (BigDecimal) literal.getValue();
        BigDecimal roundedValue = value.setScale(type.getScale());
        return rexBuilder.makeExactLiteral(
                roundedValue,
                type);
    }

    private boolean isRowConstructor(SqlNode node)
    {
        if (!node.isA(SqlKind.Row)) {
            return false;
        }
        SqlCall call = (SqlCall) node;
        return call.getOperator().getName().equalsIgnoreCase("row");
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
        case SqlKind.MultisetQueryConstructorORDINAL:
        case SqlKind.MultisetValueConstructorORDINAL:
        case SqlKind.CursorConstructorORDINAL:
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
     *
     * @return Converted expression
     */
    public RexNode convertExpression(
        SqlNode node)
    {
        Blackboard bb = createBlackboard(null);
        return bb.convertExpression(node);
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format,
     * mapping identifier references to predefined expressions.
     *
     * @param node Expression to translate
     * @param nameToNodeMap map from String to {@link RexNode}; when an {@link
     * SqlIdentifier} is encountered, it is used as a key and translated to the
     * corresponding value from this map
     *
     * @return Converted expression
     */
    public RexNode convertExpression(
        SqlNode node,
        final Map<String, RexNode> nameToNodeMap)
    {
        // REVIEW jvs 2-Jan-2005: should perhaps create a proper scope as well
        Blackboard bb =
            new Blackboard(null) {
                RexNode lookupExp(String name)
                {
                    RexNode node = nameToNodeMap.get(name);
                    if (node == null) {
                        throw Util.newInternal(
                            "Unknown identifier '" + name
                            + "' encountered while expanding expression"
                            + node);
                    }
                    return node;
                }
            };
        return bb.convertExpression(node);
    }

    /**
     * Converts a non-standard expression.
     *
     * <p>This method is an extension-point for derived classes can override. If
     * this method returns a null result, the normal expression translation
     * process will proceeed. The default implementation always returns null.
     *
     * @param node Expression
     * @param bb Blackboard
     *
     * @return null to proceed with the usual expression translation process
     */
    protected RexNode convertExtendedExpression(
        SqlNode node,
        Blackboard bb)
    {
        return null;
    }

    private RexNode convertOver(Blackboard bb, SqlNode node)
    {
        SqlCall call = (SqlCall) node;
        SqlCall aggCall = (SqlCall) call.operands[0];
        SqlNode windowOrRef = call.operands[1];
        final SqlWindow window =
            validator.resolveWindow(windowOrRef, bb.scope, true);
        final SqlNodeList partitionList = window.getPartitionList();
        final RexNode [] partitionKeys = new RexNode[partitionList.size()];
        for (int i = 0; i < partitionKeys.length; i++) {
            partitionKeys[i] = bb.convertExpression(partitionList.get(i));
        }
        SqlNodeList orderList = window.getOrderList();
        if ((orderList.size() == 0) && !window.isRows()) {
            // A logical range requires an ORDER BY clause. Use the implicit
            // ordering of this relation. There must be one, otherwise it would
            // have failed validation.
            orderList = bb.scope.getOrderList();
            Util.permAssert(orderList != null,
                "Relation should have sort key for implicit ORDER BY");
            Util.permAssert(orderList.size() > 0, "sort key must not be empty");
        }
        final RexNode [] orderKeys = new RexNode[orderList.size()];
        for (int i = 0; i < orderKeys.length; i++) {
            orderKeys[i] = bb.convertExpression(orderList.get(i));
        }
        RexNode rexAgg = exprConverter.convertCall(bb, aggCall);

        // Walk over the tree and apply 'over' to all agg functions. This is
        // necessary because the returned expression is not necessarily a call
        // to an agg function. For example, AVG(x) becomes SUM(x) / COUNT(x).
        final RexShuttle visitor =
            new HistogramShuttle(partitionKeys, orderKeys, window);
        return rexAgg.accept(visitor);
    }

    /**
     * Converts a FROM clause into a relational expression.
     *
     * @param bb Scope within which to resolve identifiers
     * @param from FROM clause of a query. Examples include:<ul>
     * <li>a single table ("SALES.EMP"),
     * <li>an aliased table ("EMP AS E"),
     * <li>a list of tables ("EMP, DEPT"),
     * <li>an ANSI Join expression ("EMP JOIN DEPT ON EMP.DEPTNO =
     * DEPT.DEPTNO"),
     * <li>a VALUES clause ("VALUES ('Fred', 20)"),
     * <li>a query ("(SELECT * FROM EMP WHERE GENDER = 'F')"),
     * <li>or any combination of the above.
     * </ul>
     *
     * @post return != null
     */
    protected void convertFrom(
        Blackboard bb,
        SqlNode from)
    {
        SqlCall call;
        final SqlNode [] operands;
        switch (from.getKind().getOrdinal()) {
        case SqlKind.AsORDINAL:
            operands = ((SqlCall) from).getOperands();
            convertFrom(bb, operands[0]);
            return;

        case SqlKind.TableSampleORDINAL:
            operands = ((SqlCall) from).getOperands();
            SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands[1]);
            if (sampleSpec instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
                String sampleName =
                    ((SqlSampleSpec.SqlSubstitutionSampleSpec) sampleSpec)
                    .getName();
                datasetStack.push(sampleName);
                convertFrom(bb, operands[0]);
                datasetStack.pop();
            } else {
                throw Util.newInternal(
                    "unknown TABLESAMPLE type: " + sampleSpec);
            }
            return;

        case SqlKind.IdentifierORDINAL:
            final SqlValidatorNamespace fromNamespace =
                validator.getNamespace(from);
            final String datasetName =
                datasetStack.isEmpty() ? null : datasetStack.peek();
            RelOptTable table =
                SqlValidatorUtil.getRelOptTable(
                    fromNamespace,
                    schema,
                    datasetName);
            final RelNode tableRel;
            if (shouldConvertTableAccess) {
                tableRel = table.toRel(cluster, connection);
            } else {
                tableRel = new TableAccessRel(cluster, table, connection);
            }
            bb.setRoot(tableRel, true);
            return;

        case SqlKind.JoinORDINAL:
            final SqlJoin join = (SqlJoin) from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            boolean isNatural = join.isNatural();
            SqlJoinOperator.JoinType joinType = join.getJoinType();
            final Blackboard leftBlackboard =
                createBlackboard(validator.getJoinScope(left));
            final Blackboard rightBlackboard =
                createBlackboard(validator.getJoinScope(right));
            convertFrom(leftBlackboard, left);
            RelNode leftRel = leftBlackboard.root;
            convertFrom(rightBlackboard, right);
            RelNode rightRel = rightBlackboard.root;
            if (isNatural) {
                throw Util.needToImplement("natural join");
            }
            JoinRelType convertedJoinType = convertJoinType(joinType);
            final JoinRelBase joinRel;
            joinRel =
                createJoin(
                    bb,
                    leftRel,
                    rightRel,
                    join.getCondition(),
                    join.getConditionType(),
                    convertedJoinType);
            bb.setRoot(joinRel, false);
            return;

        case SqlKind.SelectORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
        case SqlKind.UnionORDINAL:
            final RelNode rel = convertQueryRecursive(from, false);
            bb.setRoot(rel, false);
            return;

        case SqlKind.ValuesORDINAL:
            convertValues(bb, (SqlCall) from);
            return;

        case SqlKind.UnnestORDINAL:
            call = (SqlCall) ((SqlCall) from).operands[0];
            replaceSubqueries(bb, call);
            RexNode [] exprs = { bb.convertExpression(call) };
            final String [] fieldNames = { validator.deriveAlias(call, 0) };
            final RelNode childRel =
                CalcRel.createProject(
                    (null != bb.root) ? bb.root : new OneRowRel(cluster),
                    exprs,
                    fieldNames);

            UncollectRel uncollectRel = new UncollectRel(cluster, childRel);
            bb.setRoot(uncollectRel, true);
            return;

        case SqlKind.CollectionTableORDINAL:
            call = (SqlCall) from;

            // Dig out real call; TABLE() wrapper is just syntactic.
            assert (call.getOperands().length == 1);
            call = (SqlCall) call.getOperands()[0];
            convertCollectionTable(bb, call);
            return;

        default:
            throw Util.newInternal("not a join operator " + from);
        }
    }

    protected void convertCollectionTable(
        Blackboard bb,
        SqlCall call)
    {
        if (call.getOperator() == SqlStdOperatorTable.sampleFunction) {
            final String sampleName = SqlLiteral.stringValue(call.operands[0]);
            datasetStack.push(sampleName);
            SqlCall cursorCall = (SqlCall) call.operands[1];
            SqlNode query = cursorCall.operands[0];
            RelNode converted = convertQuery(query, false, false);
            bb.setRoot(converted, false);
            datasetStack.pop();
            return;
        }
        replaceSubqueries(bb, call);
        RexNode rexCall = bb.convertExpression(call);
        RelNode [] inputs = bb.retrieveCursors();
        TableFunctionRel callRel =
            new TableFunctionRel(
                cluster,
                rexCall,
                validator.getValidatedNodeType(call),
                inputs);
        Set<RelColumnMapping> columnMappings =
            getColumnMappings(call.getOperator());
        callRel.setColumnMappings(columnMappings);
        bb.setRoot(callRel, true);
    }

    private Set<RelColumnMapping> getColumnMappings(SqlOperator op)
    {
        SqlReturnTypeInference rti = op.getReturnTypeInference();
        if (rti == null) {
            return null;
        }
        if (rti instanceof TableFunctionReturnTypeInference) {
            TableFunctionReturnTypeInference tfrti =
                (TableFunctionReturnTypeInference) rti;
            return tfrti.getColumnMappings();
        } else {
            return null;
        }
    }

    private JoinRelBase createJoin(
        Blackboard bb,
        RelNode leftRel,
        RelNode rightRel,
        SqlNode condition,
        SqlJoinOperator.ConditionType conditionType,
        JoinRelType joinType)
    {
        // REVIEW Wael: I changed the implementation of lookup:ing deffered
        // variables. Probably this code abuses the intended api, but there
        // seem to be saffron code depending on it and didnt want to break any
        // intended plan, sent Julian an email.
        Set<String> correlatedVariables = RelOptUtil.getVariablesUsed(rightRel);
        if (correlatedVariables.size() > 0) {
            List<CorrelatorRel.Correlation> correlations =
                new ArrayList<CorrelatorRel.Correlation>();
            for (String name : correlatedVariables) {
                Map<RelOptQuery.DeferredLookup, String> mapDeferredToCorrel =
                    cluster.getQuery().getMapDeferredToCorrel();
                for (RelOptQuery.DeferredLookup lookup
                    : mapDeferredToCorrel.keySet()) {
                    String correlName = mapDeferredToCorrel.get(lookup);
                    if (correlName.equals(name)) {
                        RexFieldAccess correlNode = lookup.getFieldAccess(name);
                        final int pos =
                            leftRel.getRowType().getFieldOrdinal(
                                correlNode.getField().getName());
                        assert (leftRel.getRowType().getField(
                                    correlNode.getField().getName()).getType()
                                == correlNode.getType());
                        if (pos != -1) {
                            correlations.add(
                                new CorrelatorRel.Correlation(
                                    RelOptQuery.getCorrelOrdinal(correlName),
                                    pos));
                        }
                    }
                }
            }
            return
                new CorrelatorRel(
                    rightRel.getCluster(),
                    leftRel,
                    rightRel,
                    correlations,
                    joinType);
        }
        RexNode conditionExp =
            convertJoinCondition(bb,
                condition,
                conditionType,
                leftRel,
                rightRel);
        return
            new JoinRel(
                cluster,
                leftRel,
                rightRel,
                conditionExp,
                joinType,
                Collections.EMPTY_SET);
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
            bb.setRoot(new RelNode[] { leftRel, rightRel });
            return bb.convertExpression(condition);
        case SqlJoinOperator.ConditionType.Using_ORDINAL:
            SqlNodeList list = (SqlNodeList) condition;
            RexNode conditionExp = null;
            for (int i = 0; i < list.size(); i++) {
                final SqlNode columnName = list.get(i);
                assert columnName instanceof SqlIdentifier;
                RexNode exp = bb.convertExpression(columnName);
                if (i == 0) {
                    conditionExp = exp;
                } else {
                    conditionExp =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            conditionExp,
                            exp);
                }
            }
            assert conditionExp != null;
            return conditionExp;
        default:
            throw conditionType.unexpected();
        }
    }

    private static JoinRelType convertJoinType(
        SqlJoinOperator.JoinType joinType)
    {
        switch (joinType.getOrdinal()) {
        case SqlJoinOperator.JoinType.Comma_ORDINAL:
        case SqlJoinOperator.JoinType.Inner_ORDINAL:
        case SqlJoinOperator.JoinType.Cross_ORDINAL:
            return JoinRelType.INNER;
        case SqlJoinOperator.JoinType.Full_ORDINAL:
            return JoinRelType.FULL;
        case SqlJoinOperator.JoinType.Left_ORDINAL:
            return JoinRelType.LEFT;
        case SqlJoinOperator.JoinType.Right_ORDINAL:
            return JoinRelType.RIGHT;
        default:
            throw joinType.unexpected();
        }
    }

    /**
     * Converts the SELECT, GROUP BY and HAVING clauses of an aggregate query.
     *
     * @param bb Scope within which to resolve identifiers
     * @param groupList GROUP BY clause, or null
     * @param having HAVING clause
     * @param selectList SELECT list
     * @param orderExprList Additional expressions needed to implement ORDER BY
     */
    private void convertAgg(
        Blackboard bb,
        SqlNodeList groupList,
        SqlNode having,
        SqlNodeList selectList,
        List<SqlNode> orderExprList)
    {
        assert bb.root != null : "precondition: child != null";
        final AggConverter aggConverter = new AggConverter(bb);

        // If group-by clause is missing, pretend that it has zero elements.
        if (groupList == null) {
            groupList = SqlNodeList.Empty;
        }

        // register the group exprs
        for (int i = 0; i < groupList.size(); i++) {
            final SqlNode groupExpr = groupList.get(i);
            final SqlNode expandedGroupExpr =
                validator.expand(groupExpr, bb.scope);
            aggConverter.addGroupExpr(expandedGroupExpr);
        }

        final int selectCount = selectList.size();
        final int orderCount = orderExprList.size();
        RexNode [] selectExprs = new RexNode[selectCount + orderCount];
        String [] selectNames = new String[selectCount + orderCount];
        RexNode havingExpr = null;
        try {
            Util.permAssert(bb.agg == null, "already in agg mode");
            bb.agg = aggConverter;

            // convert the select and having expressions, so that the
            // agg converter knows which aggregations are required
            for (int i = 0; i < selectCount; i++) {
                SqlNode expr = selectList.get(i);
                selectExprs[i] = bb.convertExpression(expr);
                selectNames[i] = validator.deriveAlias(expr, i);
            }

            for (int i = 0; i < orderCount; i++) {
                SqlNode expr = orderExprList.get(i);
                selectExprs[selectCount + i] = bb.convertExpression(expr);
                selectNames[selectCount + i] =
                    validator.deriveAlias(expr, selectCount + i);
            }

            if (having != null) {
                havingExpr = bb.convertExpression(having);
            }
        } finally {
            bb.agg = null;
        }

        // compute inputs to the aggregator
        RexNode [] preExprs = aggConverter.getPreExprs();
        if (preExprs.length == 0) {
            // Special case for COUNT(*), where we can end up with no inputs at
            // all.  The rest of the system doesn't like 0-tuples, so we select
            // a dummy constant here.
            preExprs = new RexNode[1];
            preExprs[0] = rexBuilder.makeLiteral(true);
        }
        bb.setRoot(
            CalcRel.createProject(
                bb.root,
                preExprs,
                null),
            false);

        // add the aggregator
        final AggregateRel.Call [] aggCalls = aggConverter.getAggCalls();
        bb.setRoot(
            new AggregateRel(
                cluster,
                bb.root,
                groupList.size(),
                aggCalls),
            false);

        // implement HAVING
        if (having != null) {
            bb.setRoot(
                CalcRel.createFilter(bb.root, havingExpr),
                false);
        }

        // implement the SELECT list
        bb.setRoot(
            CalcRel.createProject(
                bb.root,
                selectExprs,
                selectNames),
            false);
    }

    public RexDynamicParam convertDynamicParam(
        final SqlDynamicParam dynamicParam)
    {
        // REVIEW jvs 8-Jan-2005:  dynamic params may be encountered out of
        // order.  Should probably cross-check with the count from the parser
        // at the end and make sure they all got filled in.  Why doesn't List
        // have a resize() method?!?  Make this a utility.
        while (dynamicParam.getIndex() >= dynamicParamSqlNodes.size()) {
            dynamicParamSqlNodes.add(null);
        }

        dynamicParamSqlNodes.set(
            dynamicParam.getIndex(),
            dynamicParam);
        return
            rexBuilder.makeDynamicParam(
                getDynamicParamType(dynamicParam.getIndex()),
                dynamicParam.getIndex());
    }

    /**
     * Creates a list of collations required to implement the ORDER BY clause,
     * if there is one. Populates <code>extraOrderExprs</code> with any sort
     * expressions which are not in the select clause.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Select clause, null if order by is applied to set operation
     * (UNION etc.)
     * @param orderList Order by clause, may be null
     * @param extraOrderExprs Sort expressions which are not in the select
     * clause (output)
     * @param collationList List of collations (output)
     *
     * @pre bb.root != null
     */
    private void gatherOrderExprs(
        Blackboard bb,
        SqlSelect select,
        SqlNodeList orderList,
        List<SqlNode> extraOrderExprs,
        List<RelFieldCollation> collationList)
    {
        // TODO:  add validation rules to SqlValidator also
        assert bb.root != null : "precondition: child != null";
        if (orderList == null) {
            return;
        }
        for (SqlNode orderItem : orderList) {
            collationList.add(
                convertOrderItem(
                    select,
                    orderItem,
                    extraOrderExprs,
                    RelFieldCollation.Direction.Ascending));
        }
    }

    private RelFieldCollation convertOrderItem(
        SqlSelect select,
        SqlNode orderItem,
        List<SqlNode> extraExprs,
        RelFieldCollation.Direction direction)
    {
        // DESC keyword, e.g. 'select a, b from t order by a desc'.
        if (orderItem instanceof SqlCall) {
            SqlCall call = (SqlCall) orderItem;
            if (call.getOperator() == SqlStdOperatorTable.descendingOperator) {
                return
                    convertOrderItem(select,
                        call.operands[0],
                        extraExprs,
                        RelFieldCollation.Direction.Descending);
            }
        }

        SqlNode converted = validator.expandOrderExpr(select, orderItem);

        // Scan the select list and order exprs for an identical expression.
        if (select != null) {
            SelectScope selectScope =
                (SelectScope) validator.getRawSelectScope(select);
            int ordinal = -1;
            for (SqlNode selectItem : selectScope.getExpandedSelectList()) {
                ++ordinal;
                if (selectItem.getKind() == SqlKind.As) {
                    selectItem = ((SqlCall) selectItem).operands[0];
                }
                if (converted.equalsDeep(selectItem, false)) {
                    return new RelFieldCollation(ordinal, direction);
                }
            }

            for (SqlNode extraExpr : extraExprs) {
                ++ordinal;
                if (converted.equalsDeep(extraExpr, false)) {
                    return new RelFieldCollation(ordinal, direction);
                }
            }
        }

        // TODO:  handle collation sequence
        // TODO: flag expressions as non-standard

        int ordinal = select.getSelectList().size() + extraExprs.size();
        extraExprs.add(converted);
        return new RelFieldCollation(ordinal, direction);
    }

    /**
     * Recursively converts a query to a relational expression.
     *
     * @param query Query
     * @param top Whether this query is the top-level query of the statement
     *
     * @return Relational expression
     */
    protected RelNode convertQueryRecursive(SqlNode query, boolean top)
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
        } else if (query.isA(SqlKind.Merge)) {
            final SqlMerge call = (SqlMerge) query;
            return convertMerge(call);
        } else if (query instanceof SqlCall) {
            final SqlCall call = (SqlCall) query;
            final SqlNode [] operands = call.getOperands();
            final RelNode left = convertQueryRecursive(operands[0], false);
            final RelNode right = convertQueryRecursive(operands[1], false);
            boolean all = false;
            if (call.getOperator() instanceof SqlSetOperator) {
                all = ((SqlSetOperator) (call.getOperator())).isAll();
            }
            switch (kind) {
            case SqlKind.UnionORDINAL:
                return
                    new UnionRel(
                        cluster,
                        new RelNode[] { left, right },
                        all);
            case SqlKind.IntersectORDINAL:

                // TODO:  all
                if (!all) {
                    return
                        new IntersectRel(
                            cluster,
                            new RelNode[] { left, right },
                            all);
                } else {
                    throw Util.newInternal(
                        "set operator INTERSECT ALL not suported");
                }
            case SqlKind.ExceptORDINAL:

                // TODO:  all
                if (!all) {
                    return
                        new MinusRel(
                            cluster,
                            new RelNode[] { left, right },
                            all);
                } else {
                    throw Util.newInternal(
                        "set operator EXCEPT ALL not suported");
                }
            default:
                throw Util.newInternal(
                    "not a set operator "
                    + SqlKind.enumeration.getName(kind));
            }
        } else {
            throw Util.newInternal("not a query: " + query);
        }
    }

    protected RelNode convertInsert(SqlInsert call)
    {
        RelOptTable targetTable = getTargetTable(call);

        RelNode sourceRel = convertQueryRecursive(
                call.getSource(),
                false);
        RelNode massagedRel = convertColumnList(call, sourceRel);

        return
            new TableModificationRel(
                cluster,
                targetTable,
                connection,
                massagedRel,
                TableModificationRel.Operation.INSERT,
                null,
                false);
    }

    protected RelOptTable getTargetTable(SqlNode call)
    {
        SqlValidatorNamespace targetNs = validator.getNamespace(call);
        RelOptTable targetTable =
            SqlValidatorUtil.getRelOptTable(targetNs, schema, null);
        return targetTable;
    }

    /**
     * Creates a source for an INSERT statement.
     *
     * <p>If the column list is not specified, source expressions match target
     * columns in order.
     *
     * <p>If the column list is specified, Source expressions are mapped to
     * target columns by name via targetColumnList, and may not cover the entire
     * target table. So, we'll make up a full row, using a combination of
     * default values and the source expressions provided.
     *
     * @param call Insert expression
     * @param sourceRel Source relational expression
     *
     * @return Converted INSERT statement
     */
    protected RelNode convertColumnList(
        SqlInsert call,
        RelNode sourceRel)
    {
        RelDataType sourceRowType = sourceRel.getRowType();
        final RexNode sourceRef =
            rexBuilder.makeRangeReference(sourceRowType, 0, false);
        final List<String> targetColumnNames = new ArrayList<String>();
        List<RexNode> columnExprs = new ArrayList<RexNode>();
        collectInsertTargets(call, sourceRef, targetColumnNames, columnExprs);

        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType targetRowType = targetTable.getRowType();
        int expCount = targetRowType.getFieldCount();
        RexNode [] sourceExps = new RexNode[expCount];

        // Walk the name list and place the associated value in the
        // expression list according to the ordinal value returned from
        // the table construct, leaving nulls in the list for columns
        // that are not referenced.
        for (int i = 0; i < targetColumnNames.size(); i++) {
            String targetColumnName = targetColumnNames.get(i);
            int iTarget = targetRowType.getFieldOrdinal(targetColumnName);
            assert iTarget != -1;
            sourceExps[iTarget] = columnExprs.get(i);
        }

        // Walk the expresion list and get default values for any columns
        // that were not supplied in the statement
        for (int i = 0; i < expCount; ++i) {
            if (sourceExps[i] != null) {
                if (defaultValueFactory.isGeneratedAlways(targetTable, i)) {
                    // TODO: throw a proper exception
                    throw Util.needToImplement(
                        "insert value into field which is always generated");
                }
                continue;
            }

            sourceExps[i] =
                defaultValueFactory.newColumnDefaultValue(targetTable, i);
        }

        return CalcRel.createProject(sourceRel, sourceExps, null);
    }

    /**
     * Given an INSERT statement, collects the list of names to be populated and
     * the expressions to put in them.
     *
     * @param call Insert statement
     * @param sourceRef Expression representing a row from the source relational
     * expression
     * @param targetColumnNames List of target column names, to be populated
     * @param columnExprs List of expressions, to be populated
     */
    protected void collectInsertTargets(
        SqlInsert call,
        final RexNode sourceRef,
        final List<String> targetColumnNames,
        List<RexNode> columnExprs)
    {
        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType targetRowType = targetTable.getRowType();
        SqlNodeList targetColumnList = call.getTargetColumnList();
        if (targetColumnList == null) {
            targetColumnNames.addAll(
                RelOptUtil.getFieldNameList(targetRowType));
        } else {
            for (int i = 0; i < targetColumnList.size(); i++) {
                SqlIdentifier id = (SqlIdentifier) targetColumnList.get(i);
                targetColumnNames.add(id.getSimple());
            }
        }

        for (int i = 0; i < targetColumnNames.size(); i++) {
            final RexNode expr = rexBuilder.makeFieldAccess(sourceRef, i);
            columnExprs.add(expr);
        }
    }

    private RelNode convertDelete(SqlDelete call)
    {
        RelOptTable targetTable = getTargetTable(call);
        RelNode sourceRel = convertSelect(call.getSourceSelect());
        return
            new TableModificationRel(cluster,
                targetTable,
                connection,
                sourceRel,
                TableModificationRel.Operation.DELETE,
                null,
                false);
    }

    private RelNode convertUpdate(SqlUpdate call)
    {
        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        List<String> targetColumnNameList = new ArrayList<String>();
        for (SqlNode node : call.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier) node;
            String name = id.getSimple();
            targetColumnNameList.add(name);
        }

        RelNode sourceRel = convertSelect(call.getSourceSelect());

        return
            new TableModificationRel(cluster,
                targetTable,
                connection,
                sourceRel,
                TableModificationRel.Operation.UPDATE,
                targetColumnNameList,
                false);
    }

    private RelNode convertMerge(SqlMerge call)
    {
        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        List<String> targetColumnNameList = new ArrayList<String>();
        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall != null) {
            for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
                SqlIdentifier id = (SqlIdentifier) targetColumn;
                String name = id.getSimple();
                targetColumnNameList.add(name);
            }
        }

        // replace the projection of the source select with a
        // projection that contains the following:
        // 1) the expressions corresponding to the new insert row (if there is
        //    an insert)
        // 2) all columns from the target table (if there is an update)
        // 3) the set expressions in the update call (if there is an update)

        // first, convert the merge's source select to construct the columns
        // from the target table and the set expressions in the update call
        RelNode mergeSourceRel = convertSelect(call.getSourceSelect());

        // then, convert the insert statement so we can get the insert
        // values expressions
        SqlInsert insertCall = call.getInsertCall();
        int nLevel1Exprs = 0;
        RexNode [] level1InsertExprs = null;
        RexNode [] level2InsertExprs = null;
        if (insertCall != null) {
            RelNode insertRel = convertInsert(insertCall);

            // there are 2 level of projections in the insert source; combine
            // them into a single project; level1 refers to the topmost project;
            // the level1 projection contains references to the level2
            // expressions, except in the case where no target expression was
            // provided, in which case, the expression is the default value for
            // the column
            level1InsertExprs =
                ((ProjectRel) insertRel.getInput(0)).getProjectExps();
            level2InsertExprs =
                ((ProjectRel) insertRel.getInput(0).getInput(0))
                .getProjectExps();
            nLevel1Exprs = level1InsertExprs.length;
        }

        JoinRel joinRel = (JoinRel) mergeSourceRel.getInput(0);
        int nSourceFields = joinRel.getLeft().getRowType().getFieldCount();
        int numProjExprs = nLevel1Exprs;
        if (updateCall != null) {
            numProjExprs +=
                mergeSourceRel.getRowType().getFieldCount() - nSourceFields;
        }
        RexNode [] projExprs = new RexNode[numProjExprs];
        for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
            if (level1InsertExprs[level1Idx] instanceof RexInputRef) {
                int level2Idx =
                    ((RexInputRef) level1InsertExprs[level1Idx]).getIndex();
                projExprs[level1Idx] = level2InsertExprs[level2Idx];
            } else {
                projExprs[level1Idx] = level1InsertExprs[level1Idx];
            }
        }
        if (updateCall != null) {
            RexNode [] updateExprs =
                ((ProjectRel) mergeSourceRel).getProjectExps();
            for (int i = 0; i < (numProjExprs - nLevel1Exprs); i++) {
                projExprs[i + nLevel1Exprs] = updateExprs[nSourceFields + i];
            }
        }

        RelNode massagedRel = CalcRel.createProject(joinRel, projExprs, null);

        return
            new TableModificationRel(cluster,
                targetTable,
                connection,
                massagedRel,
                TableModificationRel.Operation.MERGE,
                targetColumnNameList,
                false);
    }

    /**
     * Creates a projection that consists of expressions corresponding to insert
     * values followed by the columns from the target table and update set
     * expressions.
     *
     * @param projChild the relnode that will be the child of the created
     * projection
     * @param insertExprs the insert expressions that will make up the first
     * part of the projection
     * @param baseProject the orginal projection that consists of the target
     * table columns and update set expressions
     * @param nOrigInsertFields the number of fields in the base project that
     * correspond to insert exprs
     * @param createRefs true if the target table columns and update set
     * expressions should be created as references rather than just using the
     * original expression
     *
     * @return created projection
     */
    private RelNode createSourceProject(
        RelNode projChild,
        RexNode [] insertExprs,
        ProjectRel baseProject,
        int nOrigInsertFields,
        boolean createRefs)
    {
        // copy the expressions from the original projection corresponding
        // to the target table and the update set expressions; these
        // expressions follow the fields corresponding to the source

        RexNode [] baseProjExprs = baseProject.getProjectExps();

        int nTargetFields = insertExprs.length;
        int newNumProjFields =
            baseProject.getRowType().getFieldCount() - nOrigInsertFields
            + nTargetFields;
        RexNode [] newSourceExps = new RexNode[newNumProjFields];
        System.arraycopy(insertExprs, 0, newSourceExps, 0, nTargetFields);
        for (int i = nTargetFields, j = nOrigInsertFields; i < newNumProjFields;
            i++, j++) {
            if (createRefs) {
                newSourceExps[i] =
                    rexBuilder.makeInputRef(
                        baseProjExprs[j].getType(),
                        j);
            } else {
                newSourceExps[i] = baseProjExprs[j];
            }
        }

        return CalcRel.createProject(projChild, newSourceExps, null);
    }

    /**
     * Converts an identifier into an expression in a given scope. For example,
     * the "empno" in "select empno from emp join dept" becomes "emp.empno".
     */
    private RexNode convertIdentifier(
        Blackboard bb,
        SqlIdentifier identifier)
    {
        // first check for reserved identifiers like CURRENT_USER
        final SqlCall call = SqlUtil.makeCall(opTab, identifier);
        if (call != null) {
            return bb.convertExpression(call);
        }

        if (bb.agg != null) {
            throw Util.newInternal(
                "Identifier '" + identifier
                + "' is not a group expr");
        }

        if (bb.scope != null) {
            identifier = bb.scope.fullyQualify(identifier);
        }
        RexNode e = bb.lookupExp(identifier.names[0]);
        final String correlationName;
        if (e instanceof RexCorrelVariable) {
            correlationName = ((RexCorrelVariable) e).getName();
        } else {
            correlationName = null;
        }

        for (int i = 1; i < identifier.names.length; i++) {
            String name = identifier.names[i];
            e = rexBuilder.makeFieldAccess(e, name);
        }
        if (e instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) e;

            // adjust the type to account for nulls introduced by outer joins
            RelDataTypeField field = bb.getRootField(inputRef);
            if (field != null) {
                e =
                    rexBuilder.makeInputRef(
                        field.getType(),
                        inputRef.getIndex());
            }
        }

        if (null != correlationName) {
            final RexNode prev =
                bb.mapCorrelateVariableToRexNode.put(correlationName, e);
            assert prev == null;
        }
        return e;
    }

    /**
     * Converts a row constructor into a relational expression.
     *
     * @param bb
     * @param rowConstructor
     *
     * @return Relational expression which returns a single row.
     *
     * @pre isRowConstructor(rowConstructor)
     */
    private RelNode convertRowConstructor(
        Blackboard bb,
        SqlCall rowConstructor)
    {
        Util.pre(
            isRowConstructor(rowConstructor),
            "isRowConstructor(rowConstructor)");

        final SqlNode [] operands = rowConstructor.getOperands();
        return convertMultisets(operands, bb);
    }

    private RelNode convertCursor(Blackboard bb, SqlCall cursorCall)
    {
        assert (cursorCall.operands.length == 1);
        SqlNode query = cursorCall.operands[0];
        RelNode converted = convertQuery(query, false, false);
        int iCursor = bb.cursors.size();
        bb.cursors.add(converted);
        RexNode expr = new RexInputRef(
                iCursor,
                converted.getRowType());
        bb.mapSubqueryToExpr.put(cursorCall, expr);
        return converted;
    }

    private RelNode convertMultisets(final SqlNode [] operands, Blackboard bb)
    {
        // TODO Wael 2/04/05: this implementation is not the most efficent in
        // terms of planning since it generates XOs that can be reduced.
        List<Object> joinList = new ArrayList<Object>();
        List<SqlNode> lastList = new ArrayList<SqlNode>();
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            if (!(operand instanceof SqlCall)) {
                lastList.add(operand);
                continue;
            }

            final SqlCall call = (SqlCall) operand;
            final SqlOperator op = call.getOperator();
            if ((op != SqlStdOperatorTable.multisetValueConstructor)
                && (op != SqlStdOperatorTable.multisetQueryConstructor)) {
                lastList.add(operand);
                continue;
            }
            final RelNode input;
            if (op == SqlStdOperatorTable.multisetValueConstructor) {
                final SqlNodeList list = SqlUtil.toNodeList(call.operands);
                assert bb.scope instanceof SelectScope : bb.scope;
                CollectNamespace nss =
                    (CollectNamespace) validator.getNamespace(call);
                Blackboard usedBb;
                if (null != nss) {
                    usedBb = createBlackboard(nss.getScope());
                } else {
                    usedBb = createBlackboard(new ListScope(bb.scope) {
                                public SqlNode getNode()
                                {
                                    return call;
                                }
                            });
                }
                RelDataType multisetType = validator.getValidatedNodeType(call);
                validator.setValidatedNodeType(
                    list,
                    multisetType.getComponentType());
                input = convertQueryOrInList(usedBb, list);
            } else {
                input = convertQuery(call.operands[0], false, true);
            }

            if (lastList.size() > 0) {
                joinList.add(lastList);
            }
            lastList = new ArrayList<SqlNode>();
            CollectRel collectRel =
                new CollectRel(
                    cluster,
                    input,
                    validator.deriveAlias(call, i));
            joinList.add(collectRel);
        }

        if (joinList.size() == 0) {
            joinList.add(lastList);
        }

        for (int i = 0; i < joinList.size(); i++) {
            Object o = joinList.get(i);
            if (o instanceof List) {
                List<SqlNode> projectList = (List<SqlNode>) o;
                final List<RexNode> selectList = new ArrayList<RexNode>();
                final List<String> fieldNameList = new ArrayList<String>();
                for (int j = 0; j < projectList.size(); j++) {
                    SqlNode operand = projectList.get(j);
                    selectList.add(bb.convertExpression(operand));

                    // REVIEW angel 5-June-2005: Use deriveAliasFromOrdinal
                    // instead of deriveAlias to match field names from
                    // SqlRowOperator. Otherwise, get error   Type
                    // 'RecordType(INTEGER EMPNO)' has no field 'EXPR$0' when
                    // doing   select * from unnest(     select multiset[empno]
                    // from sales.emps);

                    fieldNameList.add(SqlUtil.deriveAliasFromOrdinal(j));
                }
                joinList.set(
                    i,
                    CalcRel.createProject(
                        new OneRowRel(cluster),
                        selectList,
                        fieldNameList));
            }
        }

        RelNode ret = (RelNode) joinList.get(0);
        for (int i = 1; i < joinList.size(); i++) {
            RelNode relNode = (RelNode) joinList.get(i);
            ret =
                new JoinRel(
                    cluster,
                    ret,
                    relNode,
                    rexBuilder.makeLiteral(true),
                    JoinRelType.INNER,
                    Collections.EMPTY_SET);
        }
        return ret;
    }

    private void convertSelectList(
        Blackboard bb,
        SqlNodeList selectList,
        List<SqlNode> orderList,
        SqlSelect select)
    {
        selectList = validator.expandStar(selectList, select, false);
        replaceSubqueries(bb, selectList);
        List<String> fieldNames = new ArrayList<String>();
        List<RexNode> exprs = new ArrayList<RexNode>();
        List<String> aliases = new ArrayList<String>();

        // Project select clause.
        int i = -1;
        for (SqlNode expr : selectList) {
            ++i;
            exprs.add(bb.convertExpression(expr));
            fieldNames.add(deriveAlias(expr, aliases, i));
        }

        // Project extra fields for sorting.
        for (SqlNode expr : orderList) {
            ++i;
            SqlNode expr2 = validator.expandOrderExpr(select, expr);
            exprs.add(bb.convertExpression(expr2));
            fieldNames.add(deriveAlias(expr, aliases, i));
        }

        bb.setRoot(
            CalcRel.createProject(bb.root, exprs, fieldNames),
            false);
    }

    private String deriveAlias(
        final SqlNode node,
        List<String> aliases,
        final int ordinal)
    {
        String alias = validator.deriveAlias(node, ordinal);
        if ((alias == null) || aliases.contains(alias)) {
            String aliasBase = (alias == null) ? "EXPR$" : alias;
            for (int j = 0;; j++) {
                alias = aliasBase + j;
                if (!aliases.contains(alias)) {
                    break;
                }
            }
        }
        aliases.add(alias);
        return alias;
    }

    /**
     * Converts a values clause (as in "INSERT INTO T(x,y) VALUES (1,2)") into a
     * relational expression.
     */
    private void convertValues(
        Blackboard bb,
        SqlCall values)
    {
        SqlNode [] rowConstructorList = values.getOperands();
        ArrayList<RelNode> unionRels = new ArrayList<RelNode>();
        for (int i = 0; i < rowConstructorList.length; i++) {
            SqlCall rowConstructor = (SqlCall) rowConstructorList[i];

            Blackboard tmpBb = createBlackboard(bb.scope);
            replaceSubqueries(tmpBb, rowConstructor);
            RexNode [] exps = new RexNode[rowConstructor.operands.length];
            String [] fieldNames = new String[rowConstructor.operands.length];
            for (int j = 0; j < rowConstructor.operands.length; j++) {
                final SqlNode node = rowConstructor.operands[j];
                exps[j] = tmpBb.convertExpression(node);
                fieldNames[j] = validator.deriveAlias(node, j);
            }
            RelNode in =
                (null == tmpBb.root) ? new OneRowRel(cluster) : tmpBb.root;
            unionRels.add(
                CalcRel.createProject(
                    in,
                    exps,
                    fieldNames));
        }

        if (unionRels.size() == 0) {
            throw Util.newInternal("empty values clause");
        } else if (unionRels.size() == 1) {
            bb.setRoot(
                unionRels.get(0),
                true);
        } else {
            bb.setRoot(
                new UnionRel(
                    cluster,
                    (RelNode []) unionRels.toArray(new RelNode[0]),
                    true),
                true);
        }

        // REVIEW jvs 22-Jan-2004:  should I add
        // mapScopeToLux.put(validator.getScope(values),bb.root);
        // ?
    }

    RelOptCluster createCluster(Environment env)
    {
        RelOptQuery query;
        if (cluster == null) {
            query = new RelOptQuery(planner);
        } else {
            query = cluster.getQuery();
        }
        final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        return query.createCluster(env, typeFactory, rexBuilder);
    }

    public RexNode convertField(
        RelDataType inputRowType,
        RelDataTypeField field)
    {
        final RelDataTypeField inputField =
            inputRowType.getField(field.getName());
        if (inputField == null) {
            throw Util.newInternal("field not found: " + field);
        }
        return
            RexUtil.maybeCast(
                rexBuilder,
                field.getType(),
                rexBuilder.makeInputRef(
                    inputField.getType(),
                    inputField.getIndex()));
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A <code>SchemaCatalogReader</code> looks up catalog information from a
     * {@link org.eigenbase.relopt.RelOptSchema schema object}.
     */
    public static class SchemaCatalogReader
        implements SqlValidatorCatalogReader
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

        public SqlValidatorTable getTable(String [] names)
        {
            if (names.length != 1) {
                return null;
            }
            final RelOptTable table =
                schema.getTableForMember(new String[] { maybeUpper(names[0]) });
            if (table != null) {
                return new SqlValidatorTable() {
                        public RelDataType getRowType()
                        {
                            return table.getRowType();
                        }

                        public String [] getQualifiedName()
                        {
                            return null;
                        }

                        public boolean isMonotonic(String columnName)
                        {
                            return false;
                        }

                        public SqlAccessType getAllowedAccess()
                        {
                            return SqlAccessType.ALL;
                        }
                    };
            }
            return null;
        }

        public RelDataType getNamedType(SqlIdentifier typeName)
        {
            return null;
        }

        public SqlMoniker [] getAllSchemaObjectNames(String [] names)
        {
            throw new UnsupportedOperationException();
        }

        private String maybeUpper(String s)
        {
            return upperCase ? s.toUpperCase() : s;
        }
    }

    /**
     * Workspace for translating an individual SELECT statement (or sub-SELECT).
     */
    protected class Blackboard
        implements SqlRexContext,
            SqlVisitor<RexNode>
    {
        /**
         * Collection of {@link RelNode} objects which correspond to a SELECT
         * statement.
         */
        public final SqlValidatorScope scope;
        public RelNode root;
        private RelNode [] inputs;
        private final Map<String, RexNode> mapCorrelateVariableToRexNode =
            new HashMap<String, RexNode>();

        List<RelNode> cursors;

        /**
         * List of <code>IN</code> and <code>EXISTS</code> nodes inside this
         * <code>SELECT</code> statement (but not inside sub-queries).
         */
        private final List<SqlNode> subqueryList = new ArrayList<SqlNode>();

        /**
         * Maps IN and EXISTS {@link SqlSelect sub-queries} to the expressions
         * which will be used to access them.
         */
        private final Map<SqlNode, RexNode> mapSubqueryToExpr =
            new HashMap<SqlNode, RexNode>();

        /**
         * Workspace for building aggregates.
         */
        AggConverter agg;

        /**
         * Creates a Blackboard.
         *
         * @param scope Name-resolution scope for expressions validated within
         * this query. Can be null if this Blackboard is for a leaf node, say
         * the "emp" identifier in "SELECT * FROM emp".
         */
        protected Blackboard(SqlValidatorScope scope)
        {
            this.scope = scope;
            cursors = new ArrayList<RelNode>();
        }

        public RexNode register(
            RelNode rel,
            JoinRelType joinType)
        {
            return register(rel, joinType, null);
        }

        /**
         * Registers a relational expression.
         *
         * @param rel Relational expression
         * @param joinType Join type
         * @param leftJoinKeysForIn LHS of IN clause, or null for expressions
         * other than IN
         *
         * @return Expression with which to refer to the row (or partial row)
         * coming from this relational expression's side of the join.
         */
        public RexNode register(
            RelNode rel,
            JoinRelType joinType,
            SqlNode leftJoinKeysForIn)
        {
            assert joinType != null;
            if (root == null) {
                assert (leftJoinKeysForIn == null);
                setRoot(rel, false);
                return
                    rexBuilder.makeRangeReference(
                        root.getRowType(),
                        0,
                        false);
            } else {
                int rightOffset = root.getRowType().getFieldList().size();

                final JoinRelBase join =
                    createJoin(
                        this,
                        root,
                        rel,
                        null,
                        SqlJoinOperator.ConditionType.None,
                        joinType);

                setRoot(join, false);

                if (leftJoinKeysForIn != null) {
                    RexNode postJoinFilter =
                        createJoinConditionForIn(
                            this,
                            leftJoinKeysForIn,
                            rightOffset);
                    setRoot(
                        CalcRel.createFilter(join, postJoinFilter),
                        false);
                }

                return
                    rexBuilder.makeRangeReference(
                        rel.getRowType(),
                        join.getLeft().getRowType().getFieldCount(),
                        joinType.generatesNullsOnRight());
            }
        }

        public void setRoot(RelNode root, boolean leaf)
        {
            this.root = root;
            this.inputs = new RelNode[] { root };
            if (leaf) {
                leaves.add(root);
            }
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
         * @return a {@link RexFieldAccess} or {@link RexRangeRef}, or null if
         * not found
         */
        RexNode lookupExp(String name)
        {
            int [] offsets = { -1 };
            final SqlValidatorScope [] ancestorScopes = { null };
            SqlValidatorNamespace foundNs =
                scope.resolve(name, ancestorScopes, offsets);
            if (foundNs == null) {
                return null;
            }

            // Found in current query's from list.  Find which from item.
            // We assume that the order of the from clause items has been
            // preserved.
            SqlValidatorScope ancestorScope = ancestorScopes[0];
            boolean isParent = ancestorScope != scope;
            int offset = offsets[0];
            RexNode result;
            if ((inputs != null) && !isParent) {
                final LookupContext rels =
                    isParent ? mapScopeToLux.get(ancestorScope)
                    : new LookupContext(this, inputs);
                result = lookup(offset, rels, isParent, null);
            } else {
                // We're referencing a relational expression which has not been
                // converted yet. This occurs when from items are correlated,
                // e.g. "select from emp as emp join emp.getDepts() as dept".
                // Create a temporary expression.
                assert isParent;
                RelOptQuery.DeferredLookup lookup =
                    new DeferredLookupImpl(this, offset, isParent);
                String correlName =
                    cluster.getQuery().createCorrelUnresolved(lookup);
                final RelDataType rowType = foundNs.getRowType();
                result = rexBuilder.makeCorrel(rowType, correlName);
            }
            return result;
        }

        /**
         * Creates an expression with which to reference <code>
         * expression</code>, whose offset in its from-list is <code>
         * offset</code>.
         */
        RexNode lookup(
            int offset,
            LookupContext lookupContext,
            boolean isParent,
            String varName)
        {
            int [] fieldOffsets = { 0 };
            RelNode rel = lookupContext.findRel(offset, fieldOffsets);
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
                return
                    rexBuilder.makeRangeReference(
                        rel.getRowType(),
                        fieldOffsets[0],
                        false);
            }
        }

        RelDataTypeField getRootField(RexInputRef inputRef)
        {
            int fieldOffset = inputRef.getIndex();
            for (int i = 0; i < inputs.length; ++i) {
                RelDataType rowType = inputs[i].getRowType();
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

        public void flatten(
            RelNode [] rels,
            List<RelNode> list)
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

        void registerSubquery(SqlNode node)
        {
            subqueryList.add(node);
        }

        RelNode [] retrieveCursors()
        {
            RelNode [] cursorArray = cursors.toArray(RelNode.emptyArray);
            cursors.clear();
            return cursorArray;
        }

        // implement ConvertletContext
        public RexNode convertExpression(SqlNode expr)
        {
            // If we're in aggregation mode and this is an expression in the
            // GROUP BY clause, return a reference to the field.
            if (agg != null) {
                RexNode rex = agg.lookupGroupExpr(expr);
                if (rex != null) {
                    return rex;
                }
            }

            // Allow the derived class chance to override the standard
            // behavior for special kinds of expressions.
            RexNode rex = convertExtendedExpression(expr, this);
            if (rex != null) {
                return rex;
            }

            // Sub-queries and OVER expressions are not like ordinary
            // expressions.
            switch (expr.getKind().getOrdinal()) {
            case SqlKind.CursorConstructorORDINAL:
            case SqlKind.SelectORDINAL:
            case SqlKind.InORDINAL:
            case SqlKind.ExistsORDINAL:
                rex = mapSubqueryToExpr.get(expr);
                assert rex != null : "rex != null";

                if (expr.getKind() == SqlKind.CursorConstructor) {
                    // cursor reference is pre-baked
                    return rex;
                }

                RexNode fieldAccess;
                boolean needTruthTest = false;
                if (expr.getKind() == SqlKind.In) {
                    if (rex instanceof RexRangeRef) {
                        // IN was converted to subquery.

                        // FIXME jvs 30-Apr-2006: For now, adding TRUE IS TRUE
                        // (instead of just TRUE) for IN due to FNL-33.  This
                        // is all wrong in the case where nulls are possible.
                        fieldAccess = rexBuilder.makeLiteral(true);
                        needTruthTest = true;
                    } else {
                        // IN was converted to OR; nothing more needed.
                        return rex;
                    }
                } else {
                    // The indicator column is the last field of the subquery.
                    final int fieldCount = rex.getType().getFieldCount();
                    fieldAccess =
                        rexBuilder.makeFieldAccess(rex, fieldCount - 1);

                    // The indicator column will be nullable if it comes from
                    // the null-generating side of the join. For EXISTS, add an
                    // "IS TRUE" check so that the result is "BOOLEAN NOT NULL".
                    if (fieldAccess.getType().isNullable()) {
                        if (expr.getKind() == SqlKind.Exists) {
                            needTruthTest = true;
                        }
                    }
                }
                if (needTruthTest) {
                    fieldAccess =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.isTrueOperator,
                            fieldAccess);
                }
                return fieldAccess;

            case SqlKind.OverORDINAL:
                return convertOver(this, expr);

            default:
                // fall through
            }

            // Apply standard conversions.
            rex = expr.accept(this);
            Util.permAssert(rex != null, "conversion result not null");
            return rex;
        }

        // implement ConvertletContext
        public RexBuilder getRexBuilder()
        {
            return rexBuilder;
        }

        // implement ConvertletContext
        public RexRangeRef getSubqueryExpr(SqlCall call)
        {
            return (RexRangeRef) mapSubqueryToExpr.get(call);
        }

        // implement ConvertletContext
        public RelDataTypeFactory getTypeFactory()
        {
            return typeFactory;
        }

        // implement ConvertletContext
        public DefaultValueFactory getDefaultValueFactory()
        {
            return defaultValueFactory;
        }

        // implement ConvertletContext
        public SqlValidator getValidator()
        {
            return validator;
        }

        // implement ConvertletContext
        public RexNode convertLiteral(SqlLiteral literal)
        {
            return exprConverter.convertLiteral(this, literal);
        }

        public RexNode convertInterval(SqlIntervalQualifier intervalQualifier)
        {
            return exprConverter.convertInterval(this, intervalQualifier);
        }

        // implement SqlVisitor
        public RexNode visit(SqlLiteral literal)
        {
            return exprConverter.convertLiteral(this, literal);
        }

        // implement SqlVisitor
        public RexNode visit(SqlCall call)
        {
            if (agg != null) {
                final SqlOperator op = call.getOperator();
                if (op.isAggregator()) {
                    Util.permAssert(
                        agg != null,
                        "aggregate fun must occur in aggregation mode");
                    return agg.convertCall(call);
                }
            }
            return exprConverter.convertCall(this, call);
        }

        // implement SqlVisitor
        public RexNode visit(SqlNodeList nodeList)
        {
            throw new UnsupportedOperationException();
        }

        // implement SqlVisitor
        public RexNode visit(SqlIdentifier id)
        {
            return convertIdentifier(this, id);
        }

        // implement SqlVisitor
        public RexNode visit(SqlDataTypeSpec type)
        {
            throw new UnsupportedOperationException();
        }

        // implement SqlVisitor
        public RexNode visit(SqlDynamicParam param)
        {
            return convertDynamicParam(param);
        }

        // implement SqlVisitor
        public RexNode visit(SqlIntervalQualifier intervalQualifier)
        {
            return convertInterval(intervalQualifier);
        }
    }

    private static class DeferredLookupImpl
        implements RelOptQuery.DeferredLookup
    {
        Blackboard bb;
        boolean isParent;
        int offset;

        DeferredLookupImpl(
            Blackboard bb,
            int offset,
            boolean isParent)
        {
            this.bb = bb;
            this.offset = offset;
            this.isParent = isParent;
        }

        /**
         * @deprecated Not currently used
         */
        public RexNode lookup(
            RelNode [] inputs,
            String varName)
        {
            final LookupContext lookupContext = new LookupContext(null, inputs);
            return bb.lookup(offset, lookupContext, isParent, varName);
        }

        public RexFieldAccess getFieldAccess(String name)
        {
            return (RexFieldAccess) bb.mapCorrelateVariableToRexNode.get(name);
        }
    }

    /**
     * An implementation of DefaultValueFactory which always supplies NULL.
     */
    class NullDefaultValueFactory
        implements DefaultValueFactory
    {
        public boolean isGeneratedAlways(
            RelOptTable table,
            int iColumn)
        {
            return false;
        }

        public RexNode newColumnDefaultValue(
            RelOptTable table,
            int iColumn)
        {
            return rexBuilder.constantNull();
        }

        public RexNode newAttributeInitializer(
            RelDataType type,
            SqlFunction constructor,
            int iAttribute,
            RexNode [] constructorArgs)
        {
            return rexBuilder.constantNull();
        }
    }

    /**
     * Converts expressions to aggregates.
     *
     * <p>Consider the expression SELECT deptno, SUM(2 * sal) FROM emp GROUP BY
     * deptno Then
     *
     * <ul>
     * <li>groupExprs = {SqlIdentifier(deptno)}</li>
     * <li>convertedInputExprs = {RexInputRef(deptno), 2 *
     * RefInputRef(sal)}</li>
     * <li>inputRefs = {RefInputRef(#0), RexInputRef(#1)}</li>
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
            new SqlNodeList(SqlParserPos.ZERO);

        /**
         * Input expressions for the group columns and aggregates, in {@link
         * RexNode} format. The first elements of the list correspond to the
         * elements in {@link #groupExprs}; the remaining elements are for
         * aggregates.
         */
        private final List<RexNode> convertedInputExprs =
            new ArrayList<RexNode>();
        private final List<RexInputRef> inputRefs =
            new ArrayList<RexInputRef>();
        private final List<AggregateRelBase.Call> aggCalls =
            new ArrayList<AggregateRelBase.Call>();

        /**
         * Input expressions required by aggregates,
         *
         * @param bb
         */

        public AggConverter(Blackboard bb)
        {
            this.bb = bb;
        }

        public void addGroupExpr(SqlNode expr)
        {
            RexNode convExpr = bb.convertExpression(expr);
            final int index = groupExprs.size();
            groupExprs.add(expr);
            convertedInputExprs.add(convExpr);
            final RelDataType type = convExpr.getType();
            inputRefs.add((RexInputRef) rexBuilder.makeInputRef(type, index));
        }

        public RexNode convertCall(SqlCall call)
        {
            assert call.getOperator().isAggregator();
            assert bb.agg == this;
            int [] args = new int[call.operands.length];
            try {
                // switch out of agg mode
                bb.agg = null;
                for (int i = 0; i < call.operands.length; i++) {
                    SqlNode operand = call.operands[i];
                    RexNode convertedExpr = null;

                    // special case for COUNT(*):  delete the *
                    if (operand instanceof SqlIdentifier) {
                        SqlIdentifier id = (SqlIdentifier) operand;
                        if (id.isStar()) {
                            assert (call.operands.length == 1);
                            args = new int[0];
                            break;
                        }
                    }
                    if (convertedExpr == null) {
                        convertedExpr = bb.convertExpression(operand);
                        assert convertedExpr != null;
                    }
                    args[i] = lookupOrCreateGroupExpr(convertedExpr);
                }
            } finally {
                // switch back into agg mode
                bb.agg = this;
            }
            final Aggregation aggregation = (Aggregation) call.getOperator();
            RelDataType type = validator.deriveType(bb.scope, call);
            boolean distinct = false;
            SqlLiteral quantifier = call.getFunctionQuantifier();
            if ((null != quantifier)
                && (quantifier.getValue() == SqlSelectKeyword.Distinct)) {
                distinct = true;
            }
            final AggregateRel.Call aggCall =
                new AggregateRel.Call(aggregation, distinct, args, type);
            int index = aggCalls.size() + groupExprs.size();
            aggCalls.add(aggCall);
            final RexNode rex = rexBuilder.makeInputRef(type, index);
            return rex;
        }

        private int lookupOrCreateGroupExpr(RexNode expr)
        {
            for (int i = 0; i < convertedInputExprs.size(); i++) {
                RexNode convertedInputExpr = convertedInputExprs.get(i);
                if (expr.toString().equals(convertedInputExpr.toString())) {
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
         * expressions, returns a reference to the expression, otherwise returns
         * null.
         */
        public RexNode lookupGroupExpr(SqlNode expr)
        {
            for (int i = 0; i < groupExprs.size(); i++) {
                SqlNode groupExpr = groupExprs.get(i);
                if (expr.equalsDeep(groupExpr, false)) {
                    return inputRefs.get(i);
                }
            }
            return null;
        }

        public RexNode [] getPreExprs()
        {
            return
                convertedInputExprs.toArray(
                    new RexNode[convertedInputExprs.size()]);
        }

        public AggregateRel.Call [] getAggCalls()
        {
            return aggCalls.toArray(new AggregateRel.Call[aggCalls.size()]);
        }

        public RelDataTypeFactory getTypeFactory()
        {
            return typeFactory;
        }
    }

    private static class LookupContext
    {
        private final List<RelNode> relList = new ArrayList<RelNode>();

        LookupContext(RelNode rel)
        {
            relList.add(rel);
        }

        LookupContext(Blackboard bb, RelNode [] rels)
        {
            bb.flatten(rels, relList);
        }

        RelNode findRel(int offset, int [] fieldOffsets)
        {
            if ((offset < 0) || (offset >= relList.size())) {
                throw Util.newInternal("could not find input " + offset);
            }
            int fieldOffset = 0;
            for (int i = 0; i < offset; i++) {
                final RelNode rel = relList.get(i);
                fieldOffset += rel.getRowType().getFieldCount();
            }
            RelNode rel = relList.get(offset);
            fieldOffsets[0] = fieldOffset;
            return rel;
        }
    }

    /**
     * Shuttle which walks over a tree of {@link RexNode}s and applies 'over'
     * to all agg functions.
     *
     * <p>This is necessary because the returned expression
     * is not necessarily a call to an agg function. For example,
     * <blockquote><code>AVG(x)</code></blockquote>
     * becomes
     * <blockquote><code>SUM(x) / COUNT(x)</code></blockquote>
     *
     * <p>Any aggregate functions are converted to calls to the internal
     * <code>$Histogram</code> aggregation function and accessors such as
     * <code>$HistogramMin</code>; for example, 
     * <blockquote><code>MIN(x), MAX(x)</code></blockquote> are converted to
     * <blockquote><code>$HistogramMin($Histogram(x)),
     *            $HistogramMax($Histogram(x))</code></blockquote>
     * Common sub-expression elmination will ensure that only one histogram
     * is computed.
     */
    private class HistogramShuttle extends RexShuttle
    {
        private final RexNode[] partitionKeys;
        private final RexNode[] orderKeys;
        private final SqlWindow window;

        HistogramShuttle(
            RexNode[] partitionKeys, RexNode[] orderKeys, SqlWindow window)
        {
            this.partitionKeys = partitionKeys;
            this.orderKeys = orderKeys;
            this.window = window;
        }

        public RexNode visitCall(RexCall call)
        {
            final SqlOperator op = call.getOperator();
            if (!(op instanceof SqlAggFunction)) {
                return super.visitCall(call);
            }
            final SqlAggFunction aggOp = (SqlAggFunction) op;
            final RelDataType type = call.getType();
            RexNode [] exprs = call.getOperands();

            // The fennel support for windowed Agg MIN/MAX only
            // supports BIGINT and DOUBLE numeric types. If the
            // expression result is numeric but not correct then pre
            // and post CAST the data.  Example with INTEGER
            // CAST(MIN(CAST(exp to BIGINT)) to INTEGER)
            SqlFunction histogramOp = getHistogramOp(aggOp);

            // If a window contains only the current row, treat it as physical.
            // (It could be logical too, but physical is simpler to implement.)
            boolean physical = window.isRows();
            if (!physical &&
                SqlWindowOperator.isCurrentRow(window.getLowerBound()) &&
                SqlWindowOperator.isCurrentRow(window.getUpperBound())) {
                physical = true;
            }

            if (histogramOp != null) {
                final RelDataType histogramType =
                    computeHistogramType(type);

                // Replace orignal expression with CAST of not one
                // of the supported types
                if (histogramType != type) {
                    exprs[0] =
                        rexBuilder.makeCast(histogramType, exprs[0]);
                }

                RexCallBinding bind =
                    new RexCallBinding(
                        rexBuilder.getTypeFactory(),
                        SqlStdOperatorTable.histogramAggFunction,
                        exprs);

                RexNode over =
                    rexBuilder.makeOver(
                        SqlStdOperatorTable.histogramAggFunction
                            .inferReturnType(bind),
                        SqlStdOperatorTable.histogramAggFunction,
                        exprs,
                        partitionKeys,
                        orderKeys,
                        window.getLowerBound(),
                        window.getUpperBound(),
                        physical);

                RexNode histogramCall =
                    rexBuilder.makeCall(
                        histogramType,
                        histogramOp,
                        over);

                // If needed, post Cast result back to original
                // type.
                if (histogramType != type) {
                    histogramCall =
                        rexBuilder.makeCast(type, histogramCall);
                }

                return histogramCall;
            } else {
                return
                    rexBuilder.makeOver(
                        type,
                        aggOp,
                        exprs,
                        partitionKeys,
                        orderKeys,
                        window.getLowerBound(),
                        window.getUpperBound(),
                        physical);
            }
        }

        /**
         * Returns the histogram operator corresponding to a given aggregate
         * function.
         *
         * <p>For example,
         * <code>getHistogramOp({@link SqlStdOperatorTable#minOperator}}</code>
         * returns {@link SqlStdOperatorTable#histogramMinFunction}.
         *
         * @param aggFunction An aggregate function
         * @return Its histogram function, or null
         */
        SqlFunction getHistogramOp(SqlAggFunction aggFunction)
        {
            if (aggFunction == SqlStdOperatorTable.minOperator) {
                return SqlStdOperatorTable.histogramMinFunction;
            } else if (aggFunction == SqlStdOperatorTable.maxOperator) {
                return SqlStdOperatorTable.histogramMaxFunction;
            } else if (aggFunction == SqlStdOperatorTable.firstValueOperator) {
                return SqlStdOperatorTable.histogramFirstValueFunction;
            } else if (aggFunction == SqlStdOperatorTable.lastValueOperator) {
                return SqlStdOperatorTable.histogramLastValueFunction;
            } else {
                return null;
            }
        }

        /**
         * Returns the type for a histogram function. It is either the actual
         * type or an an approximation to it.
         */
        private RelDataType computeHistogramType(RelDataType type)
        {
            if (SqlTypeUtil.isExactNumeric(type)
                && (type.getSqlTypeName() != SqlTypeName.Bigint)) {
                return new BasicSqlType(SqlTypeName.Bigint);
            } else if (SqlTypeUtil.isApproximateNumeric(type)
                && (type.getSqlTypeName() != SqlTypeName.Double)) {
                return new BasicSqlType(SqlTypeName.Double);
            } else {
                return type;
            }
        }
    }
}

// End SqlToRelConverter.java
