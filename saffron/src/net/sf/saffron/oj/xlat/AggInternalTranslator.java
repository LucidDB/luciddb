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

package net.sf.saffron.oj.xlat;

import net.sf.saffron.core.AggregationExtender;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.rel.BuiltinAggregation;
import net.sf.saffron.oj.util.JavaRexBuilder;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.Util;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexVariable;
import net.sf.saffron.rex.RexCall;
import net.sf.saffron.rex.RexInputRef;

import openjava.mop.*;

import openjava.ptree.*;

import java.util.Vector;
import java.util.ArrayList;


/**
 * Converts expressions to consist only of constants, references to group by
 * expressions (variables called "$group0", etc.), and calls to aggregate
 * functions (variables called "$agg0", etc.).
 * 
 * <p>
 * These names exist only fleetingly, before {@link AggUnpickler} converts
 * them to field references in the output record. But if we did not use them,
 * we could not be sure whether a field reference such as $input0.$f1 had
 * been converted.
 * </p>
 * 
 * <p>
 * Throws {@link NotAGroupException}.
 * </p>
 *
 * @see AggUnpickler
 */
class AggInternalTranslator extends InternalTranslator
{
    //~ Instance fields -------------------------------------------------------

    ArrayList aggInputList;
    InternalTranslator nonAggTranslator;
    Vector aggCallVector;
    Expression [] groups;

    //~ Constructors ----------------------------------------------------------

    AggInternalTranslator(
        QueryInfo queryInfo,
        SaffronRel [] inputs,
        Expression [] groups,
        ArrayList aggInputList,
        Vector aggCallVector, JavaRexBuilder javaRexBuilder)
    {
        super(queryInfo,inputs,javaRexBuilder);
        this.groups = groups;
        this.aggInputList = aggInputList;
        this.aggCallVector = aggCallVector;
        this.nonAggTranslator = new InternalTranslator(queryInfo,inputs,
                javaRexBuilder);
    }

    //~ Methods ---------------------------------------------------------------

    public RexNode evaluateDown(FieldAccess p)
    {
        return toGroupReference(p,false);
    }

    public RexNode evaluateDown(UnaryExpression p)
    {
        return toGroupReference(p,false);
    }

    public RexNode evaluateDown(BinaryExpression p)
    {
        return toGroupReference(p,false);
    }

    public RexNode evaluateDown(ConditionalExpression p)
    {
        return toGroupReference(p,false);
    }

    public RexNode evaluateDown(Variable p)
    {
        return toGroupReference(p,true);
    }

    public RexNode evaluateDown(MethodCall call)
    {
        // todo: Unify aggregation lookup with method lookup -- perhaps create
        // a special OJClass with a refined getAcceptableMethod() method. Then
        // make Aggregation and BuiltinAggregation package-protected again. See
        // MethodCall.pickupMethod().
        ExpressionList args = call.getArguments();
        int argCount = args.size();
        OJClass [] argTypes = new OJClass[argCount];
        try {
            for (int i = 0; i < argTypes.length; i++) {
                argTypes[i] = args.get(i).getType(qenv);
            }
        } catch (Exception e) {
            throw Util.newInternal(e);
        }
        Aggregation aggregation = lookupBuiltinAggregation(call,argTypes);
        if (aggregation != null) {
            return makeAggExp(aggregation,call.getArguments());
        }
        aggregation = lookupCustomAggregation(call,argTypes);
        if (aggregation != null) {
            return makeAggExp(aggregation,call.getArguments());
        }
        return toGroupReference(call,false);
    }

    /**
     * Creates and returns an aggregation if <code>call</code> is a call to a
     * builtin, otherwise returns null.
     */
    private Aggregation lookupBuiltinAggregation(
        MethodCall call,
        OJClass [] argTypes)
    {
        String methodName = call.getName();
        OJMethod method = BuiltinAggregation.lookup(methodName,argTypes);
        if (method == null) {
            return null;
        }
        return BuiltinAggregation.create(method.getName(),argTypes);
    }

    /**
     * Creates and returns an aggregation if <code>call</code> is a call to a
     * custom aggregation, null otherwise.
     */
    private Aggregation lookupCustomAggregation(
        MethodCall call,
        OJClass [] argTypes)
    {
        if (!call.getName().equals(AggregationExtender.METHOD_AGGREGATE)) {
            return null;
        }
        OJMethod method;
        try {
            method = call.resolve(qenv);
        } catch (Exception e) {
            return null;
        }
        if (method.getModifiers().isStatic()) {
            return null;
        }
        OJClass reftype = method.getDeclaringClass();
        if (!Toolbox.clazzAggregationExtender.isAssignableFrom(reftype)) {
            return null;
        }
        return new ExtenderAggregation(call.getReferenceExpr(), qenv, argTypes);
    }

    private RexNode makeAggExp(Aggregation aggregation,ExpressionList args) {
        // translate the arguments into internal form, then canonize them
        int argCount = args.size();
        RexNode[] rexArgs = new RexNode[argCount];
        for (int i = 0; i < argCount; i++) {
            Expression arg = args.get(i);
            rexArgs[i] = nonAggTranslator.go(arg);
        }
        int [] argIndexes = new int[argCount];
outer: 
        for (int i = 0; i < argCount; i++) {
            Expression arg = args.get(i);
            for (int j = 0,m = aggInputList.size(); j < m; j++) {
                if (aggInputList.get(j).equals(arg)) {
                    // expression already exists; use that
                    argIndexes[i] = j;
                    continue outer;
                }
            }
            argIndexes[i] = aggInputList.size();
            aggInputList.add(arg);
        }
        AggregateRel.Call aggCall =
            new AggregateRel.Call(aggregation,argIndexes);

        // create a new aggregate call, if there isn't already an
        // identical one
        int k = aggCallVector.indexOf(aggCall);
        if (k < 0) {
            k = aggCallVector.size();
            aggCallVector.addElement(aggCall);
        }
        return new RexAggVariable(k);
    }

    private RexNode toGroupReference(Expression expression,boolean fail)
    {
        for (int i = 0; i < groups.length; i++) {
            if (groups[i].equals(expression)) {
                return new RexGroupVariable(i);
            }
        }
        if (fail) {
            throw new NotAGroupException(
                "expression " + expression + " is not a group expression");
        } else {
            return rexBuilder.makeJava(qenv, expression);
        }
    }

    /**
     * Removes {@link RexGroupVariable} and {@link RexAggVariable} objects
     * which we created temporarily, before we knew how many groups there were
     * going to be.
     */
    public RexNode unpickle(RexNode rex) {
        if (rex instanceof RexCall) {
            RexNode[] operands = ((RexCall) rex).operands;
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                operands[i] = unpickle(operand);
            }
            return rex;
        } else if (rex instanceof RexVariable) {
            final SaffronType rowType = inputs[0].getRowType();
            final RexNode ref = rexBuilder.makeRangeReference(rowType);
            if (rex instanceof RexGroupVariable) {
                final RexGroupVariable groupVar = (RexGroupVariable) rex;
                return rexBuilder.makeFieldAccess(ref, groupVar.group);
            } else if (rex instanceof RexAggVariable) {
                final RexAggVariable aggVar = (RexAggVariable) rex;
                return rexBuilder.makeFieldAccess(ref, groups.length + aggVar.agg);
            } else if (rex instanceof RexInputRef) {
                throw Util.newInternal(
                    "Expression " + rex + " is neither a constant nor "
                    + "an aggregate function");
            } else {
                return rex;
            }
        } else {
            return rex;
        }
    }

    /**
     * Reference to a key of the current aggregator.
     * This expression is created only temporarily, and is removed by the
     * {@link #unpickle} method.
     */
    static class RexGroupVariable extends RexVariable {
        private final int group;

        RexGroupVariable(int group) {
            super("$g" + group, null);
            this.group = group;
        }

        public Object clone() {
            return new RexGroupVariable(group);
        }
    }

    /**
     * Reference to an aggregation expression of the current aggregator.
     * This expression is created only temporarily, and is removed by the
     * {@link #unpickle} method.
     */
    static class RexAggVariable extends RexVariable {
        private final int agg;

        RexAggVariable(int agg) {
            super("$a" + agg, null);
            this.agg = agg;
        }

        public Object clone() {
            return new RexAggVariable(agg);
        }
    }
}
