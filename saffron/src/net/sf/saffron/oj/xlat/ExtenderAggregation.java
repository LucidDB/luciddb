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

import net.sf.saffron.core.*;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.OJMethod;
import openjava.mop.Toolbox;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.*;
import org.eigenbase.oj.rex.OJAggImplementor;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.Util;


/**
 * <code>ExtenderAggregation</code> is an aggregation which works by
 * instantiating a user-defined aggregation ({@link
 * net.sf.saffron.core.AggregationExtender}), as opposed to generating custom
 * code.
 *
 * <p>
 * Two bugs with the current implementation of {@link #implementStart} etc.
 * First, the aggregation expression should be evaluated only once -- when
 * the query starts -- and stored in a variable. I guess it should be added
 * to the environment holding the query.
 * </p>
 *
 * <p>
 * Second, we pass real expressions as the values of the dummy arguments to
 * {@link #implementStart} and {@link OJAggImplementor#implementResult}. This
 * is inefficient, but moreover, it is conceivable that the expressions will
 * not be valid in the scope where start and result are executed.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 3 February, 2002
 */
class ExtenderAggregation implements Aggregation, OJAggImplementor
{
    Expression aggExp;
    OJClass aggClazz;
    OJMethod aggregateMethod;
    OJMethod mergeMethod;
    OJMethod nextMethod;
    OJMethod resultMethod;
    OJMethod startMethod;
    OJClass [] argTypes;

    ExtenderAggregation(
        Expression aggExp,
        Environment env,
        OJClass [] argTypes)
    {
        this.aggExp = aggExp;
        this.aggClazz = Toolbox.getType(env, aggExp);
        assert (Toolbox.clazzAggregationExtender.isAssignableFrom(aggClazz));
        this.argTypes = argTypes;
        this.aggregateMethod =
            findMethod(aggClazz, AggregationExtender.METHOD_AGGREGATE,
                argTypes, 0, null, true);
        if (this.aggregateMethod.getReturnType() == Toolbox.clazzVoid) {
            throw Util.newInternal("Method 'aggregate' must not return 'void'");
        }
        this.startMethod =
            findMethod(aggClazz, AggregationExtender.METHOD_START, argTypes,
                0, Toolbox.clazzObject, true);
        this.nextMethod =
            findMethod(aggClazz, AggregationExtender.METHOD_NEXT, argTypes, 1,
                Toolbox.clazzObject, true);
        this.mergeMethod =
            findMethod(aggClazz, AggregationExtender.METHOD_MERGE, argTypes,
                2, Toolbox.clazzObject, false);
        this.resultMethod =
            findMethod(
                aggClazz,
                AggregationExtender.METHOD_RESULT,
                argTypes,
                1,
                this.aggregateMethod.getReturnType(),
                true);
    }

    public String getName()
    {
        return aggClazz.getName();
    }

    // implement Aggregation
    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        OJClass [] classes = argTypes;
        RelDataType [] types = new RelDataType[classes.length];
        for (int i = 0; i < classes.length; ++i) {
            types[i] = OJUtil.ojToType(typeFactory, classes[i]);
        }
        return types;
    }

    // TODO:  share common type adapter with BuiltinAggregation, and
    // move this to oj.rel
    // implement Aggregation
    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return OJUtil.ojToType(
            typeFactory,
            resultMethod.getReturnType());
    }

    /**
     * Returns whether this aggregation has an overloading which matches the
     * given name and argument types.
     */
    public static boolean matches(
        OJClass aggClazz,
        OJClass [] argTypes)
    {
        return findMethod(aggClazz, "result", argTypes, 1, null, false) != null;
    }

    // implement Aggregation
    public boolean canMerge()
    {
        return mergeMethod != null;
    }

    // implement Aggregation
    public void implementMerge(
        JavaRelImplementor implementor,
        RelNode rel,
        Expression accumulator,
        Expression otherAccumulator)
    {
        assert (canMerge());

        // saffron.runtime.AggAndAcc a = (saffron.runtime.AggAndAcc) acc;
        // "acc.total = acc.agg.merge(
        //    (T0) 0, (T1) null..., acc.total, ((AggAndAcc) otherAcc).total)"
        StatementList stmtList = implementor.getStatementList();
        Variable var = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzAggAndAcc),
                var.toString(),
                accumulator));
        ExpressionList exprList = new ExpressionList();
        for (int i = 0; i < argTypes.length; i++) {
            exprList.add(
                new CastExpression(
                    TypeName.forOJClass(argTypes[i]),
                    argTypes[i].isPrimitive() ? Literal.constantZero()
                    : Literal.constantNull()));
        }
        exprList.add(new FieldAccess(var, "total"));
        exprList.add(
            new FieldAccess(
                new CastExpression(Toolbox.clazzAggAndAcc, otherAccumulator),
                "total"));
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(var, "total"),
                    AssignmentExpression.EQUALS,
                    new MethodCall(
                        new FieldAccess(var, "agg"),
                        AggregationExtender.METHOD_MERGE,
                        exprList))));
    }

    // implement OJAggImplementor
    public void implementNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression accumulator,
        AggregateRel.Call call)
    {
        // saffron.runtime.AggAndAcc a = (saffron.runtime.AggAndAcc) acc;
        // "a.total = a.agg.next(arg..., a.total);"
        StatementList stmtList = implementor.getStatementList();
        Variable var = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzAggAndAcc),
                var.toString(),
                new CastExpression(
                    TypeName.forOJClass(Toolbox.clazzAggAndAcc),
                    accumulator)));
        ExpressionList exprList = new ExpressionList();
        final int [] args = call.args;
        for (int i = 0; i < args.length; i++) {
            exprList.add(implementor.translateInputField(rel, 0, args[i]));
        }
        exprList.add(new FieldAccess(var, "total"));
        stmtList.add(
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(var, "total"),
                    AssignmentExpression.EQUALS,
                    new MethodCall(
                        new CastExpression(
                            TypeName.forOJClass(aggClazz),
                            new FieldAccess(var, "agg")),
                        AggregationExtender.METHOD_NEXT,
                        exprList))));
    }

    // implement Aggregation
    public Expression implementResult(
        JavaRelImplementor implementor,
        Expression accumulator,
        AggregateRel.Call call)
    {
        // "((T) ((AggAndAcc) acc).agg).result(
        //     (T0) 0, (T1) null..., ((AggAndAcc) acc).total)"
        ExpressionList exprList = new ExpressionList();
        for (int i = 0; i < argTypes.length; i++) {
            exprList.add(
                new CastExpression(
                    TypeName.forOJClass(argTypes[i]),
                    argTypes[i].isPrimitive() ? Literal.constantZero()
                    : Literal.constantNull()));
        }
        exprList.add(
            new FieldAccess(
                new CastExpression(Toolbox.clazzAggAndAcc, accumulator),
                "total"));
        return new MethodCall(
            new CastExpression(
                aggClazz,
                new FieldAccess(
                    new CastExpression(Toolbox.clazzAggAndAcc, accumulator),
                    "agg")),
            AggregationExtender.METHOD_RESULT,
            exprList);
    }

    // implement Aggregation
    public Expression implementStart(
        JavaRelImplementor implementor,
        JavaRel rel,
        AggregateRel.Call call)
    {
        Variable var = implementor.newVariable();
        StatementList stmtList = implementor.getStatementList();

        // Nth agg = new Nth(5);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(aggClazz),
                var.toString(),
                aggExp));

        // new saffron.runtime.AggAndAcc(
        //   agg,
        //   agg.start((T0) 0, (T1) null))
        ExpressionList exprList = new ExpressionList();
        for (int i = 0; i < argTypes.length; i++) {
            exprList.add(
                new CastExpression(
                    TypeName.forOJClass(argTypes[i]),
                    argTypes[i].isPrimitive() ? Literal.constantZero()
                    : Literal.constantNull()));
        }
        return new AllocationExpression(
            OJClass.forClass(net.sf.saffron.runtime.AggAndAcc.class),
            new ExpressionList(
                var,
                new MethodCall(var, AggregationExtender.METHOD_START, exprList)));
    }

    // implement Aggregation
    public Expression implementStartAndNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        AggregateRel.Call call)
    {
        Variable var_agg = implementor.newVariable();
        StatementList stmtList = implementor.getStatementList();

        // Nth agg = new Nth(5);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(aggClazz),
                var_agg.toString(),
                aggExp));

        // AggAndAcc acc = new saffron.runtime.AggAndAcc(
        //   agg,
        //   agg.next(x, y, agg.start((T0) 0, (T1) null)))
        ExpressionList startList = new ExpressionList();

        // AggAndAcc acc = new saffron.runtime.AggAndAcc(
        //   agg,
        //   agg.next(x, y, agg.start((T0) 0, (T1) null)))
        ExpressionList nextList = new ExpressionList();
        for (int i = 0; i < argTypes.length; i++) {
            startList.add(
                new CastExpression(
                    TypeName.forOJClass(argTypes[i]),
                    argTypes[i].isPrimitive() ? Literal.constantZero()
                    : Literal.constantNull()));
            nextList.add(implementor.translateInputField(rel, 0, call.args[i]));
        }
        nextList.add(
            new MethodCall(var_agg, AggregationExtender.METHOD_START, startList));
        Variable var_acc = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzAggAndAcc),
                var_acc.toString(),
                new AllocationExpression(
                    Toolbox.clazzAggAndAcc,
                    new ExpressionList(
                        var_agg,
                        new MethodCall(var_agg,
                            AggregationExtender.METHOD_NEXT, nextList)))));
        return var_acc;
    }

    private static OJMethod findMethod(
        OJClass aggClazz,
        String name,
        OJClass [] argTypes,
        int extra,
        OJClass desiredType,
        boolean fail)
    {
        if (extra > 0) {
            OJClass [] newArgTypes = new OJClass[argTypes.length + extra];
            System.arraycopy(argTypes, 0, newArgTypes, 0, argTypes.length);
            for (int i = 0; i < extra; i++) {
                newArgTypes[argTypes.length + i] = Toolbox.clazzObject;
            }
            argTypes = newArgTypes;
        }
        OJMethod [] allMethods = aggClazz.getMethods();
loop: 
        for (int i = 0, n = allMethods.length; i < n; i++) {
            OJMethod method = allMethods[i];
            if (method.getName().equals(name)
                    && (method.getParameterTypes().length == argTypes.length)) {
                OJClass [] parameterTypes = method.getParameterTypes();
                for (int j = 0; j < argTypes.length; j++) {
                    if (!parameterTypes[j].isAssignableFrom(argTypes[j])) {
                        continue loop;
                    }
                }
                if ((desiredType != null)
                        && (method.getReturnType() != desiredType)) {
                    throw Util.newInternal("Method '" + name
                        + "' should return '" + desiredType + "'");
                }
                return method;
            }
        }
        if (fail) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < argTypes.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(argTypes[i]);
            }
            throw Util.newInternal("could not find method " + name + "(" + sb
                + ") required to implement AggregationExtender");
        } else {
            return null;
        }
    }
}


// End ExtenderAggregation.java
