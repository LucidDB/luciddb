/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
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

package net.sf.saffron.oj.xlat;

import java.math.BigDecimal;
import java.util.*;

import openjava.mop.OJClass;
import openjava.mop.QueryEnvironment;
import openjava.ptree.*;

import org.eigenbase.oj.OJTypeFactory;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.Util;

import java.util.List;

/**
 * Converts an {@link ParseTree Openjava expression} into a
 * {@link RexNode row-expression}.
 *
 * <p>
 * For example, the filter expression <code>emp.sal &gt; 50</code> from
 * <code>select &#42; from emp where emp.sal &gt; 50</code> becomes
 * <code>$input0.sal &gt; 50</code>.  And <code>emp.deptno ==
 * dept.deptno</code> becomes <code>$input.$0.deptno ==
 * $input.$1.deptno</code>.
 * </p>
 *
 * <p>Any queries are wrapped as {@link RexQuery} nodes. They will be expanded
 * later.</p>
 *
 * @see AggInternalTranslator
 */
class InternalTranslator
{
    QueryInfo queryInfo;
    RelNode [] inputs;
    protected final JavaRexBuilder rexBuilder;
    protected final QueryEnvironment qenv;
    private static final HashMap mapUnaryOpToSql = createUnaryMap();

    //private static final HashMap mapBinaryOpToRex = createBinaryMap();
    private static HashMap createUnaryMap()
    {
        HashMap map = new HashMap();

        //map.put(new Integer(UnaryExpression.POST_INCREMENT),RexKind.None);
        //map.put(new Integer(UnaryExpression.POST_DECREMENT),RexKind.None);
        //map.put(new Integer(UnaryExpression.PRE_INCREMENT),RexKind.None);
        //map.put(new Integer(UnaryExpression.PRE_DECREMENT),RexKind.None);
        //map.put(new Integer(UnaryExpression.BIT_NOT),RexKind.None);
        map.put(
            new Integer(UnaryExpression.NOT),
            SqlStdOperatorTable.notOperator);

        //map.put(new Integer(UnaryExpression.PLUS),RexKind.None); // no op corresponding to prefix "+"
        map.put(
            new Integer(UnaryExpression.MINUS),
            SqlStdOperatorTable.prefixMinusOperator);

        //map.put(new Integer(UnaryExpression.EXISTS),RexKind.None);
        return map;
    }

    private static HashMap createBinaryMap()
    {
        HashMap map = new HashMap();
        map.put(
            new Integer(BinaryExpression.TIMES),
            RexKind.Times);
        map.put(
            new Integer(BinaryExpression.DIVIDE),
            RexKind.Divide);

        //map.put(new Integer(BinaryExpression.MOD),RexKind.Other);
        map.put(
            new Integer(BinaryExpression.PLUS),
            RexKind.Plus);
        map.put(
            new Integer(BinaryExpression.MINUS),
            RexKind.Minus);

        //map.put(new Integer(BinaryExpression.SHIFT_L),RexKind.Other);
        //map.put(new Integer(BinaryExpression.SHIFT_R),RexKind.Other);
        //map.put(new Integer(BinaryExpression.SHIFT_RR),RexKind.Other);
        map.put(
            new Integer(BinaryExpression.LESS),
            RexKind.LessThan);
        map.put(
            new Integer(BinaryExpression.GREATER),
            RexKind.GreaterThan);
        map.put(
            new Integer(BinaryExpression.LESSEQUAL),
            RexKind.LessThanOrEqual);
        map.put(
            new Integer(BinaryExpression.GREATEREQUAL),
            RexKind.GreaterThanOrEqual);

        //map.put(new Integer(BinaryExpression.INSTANCEOF),RexKind.Other);
        map.put(
            new Integer(BinaryExpression.EQUAL),
            RexKind.Equals);
        map.put(
            new Integer(BinaryExpression.NOTEQUAL),
            RexKind.NotEquals);

        //map.put(new Integer(BinaryExpression.BITAND),RexKind.Other);
        //map.put(new Integer(BinaryExpression.XOR),RexKind.Other);
        //map.put(new Integer(BinaryExpression.BITOR),RexKind.Other);
        map.put(
            new Integer(BinaryExpression.LOGICAL_AND),
            RexKind.And);
        map.put(
            new Integer(BinaryExpression.LOGICAL_OR),
            RexKind.Or);

        //map.put(new Integer(BinaryExpression.IN),RexKind.Other);
        //map.put(new Integer(BinaryExpression.UNION),RexKind.Other);
        //map.put(new Integer(BinaryExpression.EXCEPT),RexKind.Other);
        //map.put(new Integer(BinaryExpression.INTERSECT),RexKind.Other);
        return map;
    }

    InternalTranslator(
        QueryInfo queryInfo,
        RelNode [] inputs,
        JavaRexBuilder rexBuilder)
    {
        this.queryInfo = queryInfo;
        this.qenv = (QueryEnvironment) queryInfo.env;
        this.inputs = inputs;
        this.rexBuilder = rexBuilder;
    }

    public RexNode go(ParseTree p)
    {
        if (p instanceof Leaf) {
            if (p instanceof Variable) {
                return evaluateDown((Variable) p);
            }
            if (p instanceof Literal) {
                return evaluateDown((Literal) p);
            }
        } else if (p instanceof NonLeaf) {
            if (p instanceof AliasedExpression) {
                return go(((AliasedExpression) p).getExpression());
            }
            if (p instanceof QueryExpression) {
                return new RexQuery((QueryExpression) p);
            }
            if (p instanceof FieldAccess) {
                return evaluateDown((FieldAccess) p);
            }
            if (p instanceof BinaryExpression) {
                return evaluateDown((BinaryExpression) p);
            }
            if (p instanceof UnaryExpression) {
                return evaluateDown((UnaryExpression) p);
            }
            if (p instanceof ConditionalExpression) {
                return evaluateDown((ConditionalExpression) p);
            }
            if (p instanceof MethodCall) {
                return evaluateDown((MethodCall) p);
            }
        }
        throw Util.newInternal("unknown expr type " + p);
    }

    private RexNode evaluateDown(Literal literal)
    {
        final Object o = OJUtil.literalValue(literal);
        final OJClass ojClass;
        try {
            ojClass = literal.getType(qenv);
        } catch (Exception e) {
            throw Util.newInternal(e, "Error deriving type of " + literal);
        }
        RexNode rex;
        if (o == null) {
            rex = rexBuilder.constantNull();
        } else if (o instanceof Boolean) {
            rex = rexBuilder.makeLiteral(((Boolean) o).booleanValue());
        } else if (o instanceof String) {
            rex = rexBuilder.makeLiteral(((String) o));
        } else if (o instanceof BigDecimal) {
            rex = rexBuilder.makeExactLiteral((BigDecimal) o);
        } else if (o instanceof Number) {
            rex = rexBuilder.makeExactLiteral(new BigDecimal(o.toString()));
        } else {
            throw Util.needToImplement(this);
        }

        // Cast the expression, so that the row-type is precisely what is
        // expected.
        final RelDataType type =
            ((OJTypeFactory) rexBuilder.getTypeFactory()).toType(ojClass);
        if (type != rex.getType()) {
            rex = rexBuilder.makeCast(type, rex);
        }
        return rex;
    }

    public RexNode evaluateDown(FieldAccess p)
    {
        final RexNode ref = go(p.getReferenceExpr());
        return rexBuilder.makeFieldAccess(
            ref,
            p.getName());
    }

    public RexNode evaluateDown(UnaryExpression p)
    {
        final RexNode rexNode = go(p.getExpression());
        final SqlOperator opCode = unaryOpToSql(p.getOperator());
        return rexBuilder.makeCall(opCode, rexNode);
    }

    /**
     * Translates an operator code from Openjava
     * ({@link UnaryExpression#BIT_NOT} et cetera) to row-expression.
     */
    private SqlOperator unaryOpToSql(int op)
    {
        return (SqlOperator) mapUnaryOpToSql.get(new Integer(op));
    }

    /**
     * Translates an operator code from Openjava
     * ({@link BinaryExpression#DIVIDE} et cetera) to row-expression.
     */
    private SqlOperator binaryOpToSql(int op)
    {
        final String sqlName =
            (String) SqlToOpenjavaConverter.mapBinaryOjToSql.get(
                new Integer(op));
        if (sqlName == null) {
            return null;
        }
        List list = rexBuilder.getOpTab().lookupOperatorOverloads(
            new SqlIdentifier(sqlName.toUpperCase(), SqlParserPos.ZERO),
            null,
            SqlSyntax.Binary);
        if (list.isEmpty()) {
            return null;
        }
        assert(list.size() == 1);
        return (SqlOperator) list.get(0);
    }

    public RexNode evaluateDown(BinaryExpression p)
    {
        final RexNode rexLeft = go(p.getLeft());
        final RexNode rexRight = go(p.getRight());
        final SqlOperator opCode = binaryOpToSql(p.getOperator());
        if (opCode == null) {
            throw Util.newInternal("Cannot translate binary operator "
                + p.getOperator());
        }
        return rexBuilder.makeCall(opCode, rexLeft, rexRight);
    }

    public RexNode evaluateDown(ConditionalExpression p)
    {
        final RexNode rexCond = go(p.getCondition());
        final RexNode rexTrueCase = go(p.getTrueCase());
        final RexNode rexFalseCase = go(p.getFalseCase());
        return rexBuilder.makeCase(rexCond, rexTrueCase, rexFalseCase);
    }

    public RexNode evaluateDown(MethodCall call)
    {
        final ExpressionList arguments = call.getArguments();
        final RexNode [] args;
        if (call.getReferenceExpr() != null) {
            args = new RexNode[arguments.size() + 1];
            args[0] = go(call.getReferenceExpr());
            for (int i = 1; i < args.length; i++) {
                args[i] = go(arguments.get(i - 1));
            }
        } else {
            args = new RexNode[arguments.size()];
            for (int i = 0; i < args.length; i++) {
                args[i] = go(arguments.get(i));
            }
        }
        SqlOperator op = translateFun(call.getName());
        return rexBuilder.makeCall(op, args);
    }

    private SqlOperator translateFun(String name)
    {
        if (name.equals("equals")) {
            return rexBuilder.getOpTab().equalsOperator;
        }
        throw Util.needToImplement(this);
    }

    public RexNode evaluateDown(Variable p)
    {
        String varName = p.toString();

        // The variable might refer to:
        // 1. the current row of a relational input --> convert it to a
        //    reference to the pseudo-variable which holds the current row
        //    input relation (or a field of this input relation, if there
        //    is more than one input)
        // 2. the current row of a query in some enclosing context (it is a
        //    correlating variable)
        QueryInfo.LookupResult exp = lookupExp(varName);
        if (exp != null) {
            if (exp instanceof QueryInfo.CorrelLookupResult) {
                return rexBuilder.makeCorrel(null,
                    ((QueryInfo.CorrelLookupResult) exp).varName);
            } else if (exp instanceof QueryInfo.LocalLookupResult) {
                final QueryInfo.LocalLookupResult localLookupResult =
                    (QueryInfo.LocalLookupResult) exp;
                return rexBuilder.makeRangeReference(localLookupResult.rowType,
                    localLookupResult.offset, false);
            } else {
                throw Util.newInternal("Unknown LookupResult subtype " + exp);
            }
        }

        // 3. a regular java variable --> leave it be
        return rexBuilder.makeJava(qenv, p);
    }

    public Expression evaluateUp(AliasedExpression p)
    {
        // strip away "AliasedExpression(e)" to leave "e".
        return p.getExpression();
    }

    /**
     * Returns an expression with which to reference a from-list item.
     *
     * @param name the alias of the from item
     *
     * @return a {@link openjava.ptree.Variable} or {@link
     *         openjava.ptree.FieldAccess}
     */
    QueryInfo.LookupResult lookupExp(String name)
    {
        int [] offsets = new int [] { -1 };
        Expression expression = qenv.lookupFrom(name, offsets);
        if (expression != null) {
            // Found in current query's from list.  Find which from item.
            // We assume that the order of the from clause items has been
            // preserved.
            return queryInfo.lookup(offsets[0], inputs, false, null);
        }

        // Looks for the name in enclosing queries' from lists.
        for (QueryInfo qi = queryInfo.parent; qi != null; qi = qi.parent) {
            if (!(qi.env instanceof QueryEnvironment)) {
                continue;
            }
            final QueryEnvironment qenv = (QueryEnvironment) qi.env;
            expression = qenv.lookupFrom(name, offsets);
            if (expression == null) {
                continue;
            }
            int offset = offsets[0];
            RelNode input = qi.getRoot();
            if (input == null) {
                // We're referencing a relational expression which has not
                // been translated yet. This occurs when from items are
                // correlated, e.g. "select * from Orders as o join
                // order.Lineitems as li". Create a temporary expression.
                assert (!false);
                assert (null == null) : "when lookup is called to fixup forward references "
                + "(varName!=null), the input must not be null";
                DeferredLookup lookup = new DeferredLookup(qi, offset, false);
                String correlName =
                    qi.cluster.getQuery().createCorrelUnresolved(lookup);
                return new QueryInfo.CorrelLookupResult(correlName);
            } else {
                return qi.lookup(
                    offset,
                    new RelNode [] { input },
                    true,
                    null);
            }
        }

        // Not found. Must be a java variable
        return null;
    }

    /**
     * Temporary holder for a scalar query which has not been translated yet.
     */
    static class RexQuery extends RexNode
    {
        private final QueryExpression queryExpression;

        RexQuery(QueryExpression queryExpression)
        {
            this.queryExpression = queryExpression;
        }

        public RelDataType getType()
        {
            throw new UnsupportedOperationException();
        }

        public Object clone()
        {
            throw new UnsupportedOperationException();
        }

        public void accept(RexVisitor visitor)
        {
            throw new UnsupportedOperationException();
        }

        public RexNode accept(RexShuttle shuttle)
        {
            throw new UnsupportedOperationException();
        }
    }
}
