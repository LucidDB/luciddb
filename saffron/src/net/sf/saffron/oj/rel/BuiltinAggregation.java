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

package net.sf.saffron.oj.rel;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Util;

/**
 * <code>BuiltinAggregation</code> is a basic aggregator for which special
 * code is generated.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 3 February, 2002
 */
public abstract class BuiltinAggregation implements Aggregation
{
    private static final String holderClassName = "saffron.runtime.Holder";

    // The following methods are placeholders.
    public static int count()
    {
        throw new UnsupportedOperationException();
    }

    public static int count(Object v)
    {
        throw new UnsupportedOperationException();
    }

    public static int count(int v)
    {
        throw new UnsupportedOperationException();
    }

    public static int count(double v)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a <code>BuiltinAggregation</code> with a given name and
     * argument types.
     */
    public static BuiltinAggregation create(
        String name,
        OJClass [] argTypes)
    {
        if (name.equals("sum") && (argTypes.length == 1)) {
            return new Sum(argTypes[0]);
        }
        if (name.equals("count")) {
            return new Count();
        }
        if ((name.equals("min") || name.equals("max"))
                && (MinMax.getKind(argTypes) != MinMax.MINMAX_INVALID)) {
            return new MinMax(
                argTypes,
                name.equals("min"));
        }
        return null;
    }

    // implement Aggregation
    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        OJClass [] classes = getParameterTypes();
        RelDataType [] types = new RelDataType[classes.length];
        for (int i = 0; i < classes.length; ++i) {
            types[i] = OJUtil.ojToType(typeFactory, classes[i]);
        }
        return types;
    }

    // implement Aggregation
    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return OJUtil.ojToType(
            typeFactory,
            getReturnType());
    }

    // implement Aggregation
    public boolean canMerge()
    {
        return false;
    }

    // implement Aggregation
    public void implementMerge(
        JavaRelImplementor implementor,
        RelNode rel,
        Expression accumulator,
        Expression otherAccumulator)
    {
        throw Util.newInternal(
            "This method shouldn't have been called, because canMerge "
            + "returned " + canMerge());
    }

    /**
     * This is a default implementation of {@link
     * Aggregation#implementStartAndNext}; particular derived classes may do
     * better.
     */
    public Expression implementStartAndNext(
        JavaRelImplementor implementor,
        JavaRel rel,
        int [] args)
    {
        StatementList stmtList = implementor.getStatementList();
        Variable var = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzObject),
                var.toString(),
                implementStart(implementor, rel, args)));
        implementNext(implementor, rel, var, args);
        return var;
    }

    /**
     * Returns the builtin aggregator with a given name, if there is one. Note
     * that there is only one builtin aggregator with a particular name
     * ("sum", say), but a particular instance may have different parameter
     * types.
     */
    public static OJMethod lookup(
        String name,
        OJClass [] argTypes)
    {
        OJClass clazz = OJClass.forClass(BuiltinAggregation.class);
        OJMethod method;
        try {
            method = clazz.getMethod(name, argTypes);
        } catch (NoSuchMemberException e) {
            return null;
        }
        if ((method != null) && method.getModifiers().isStatic()) {
            return method;
        }
        return null;
    }

    public static int max(Object v)
    {
        throw new UnsupportedOperationException();
    }

    public static double max(int v)
    {
        throw new UnsupportedOperationException();
    }

    public static double max(double v)
    {
        throw new UnsupportedOperationException();
    }

    public static int min(Object v)
    {
        throw new UnsupportedOperationException();
    }

    public static double min(int v)
    {
        throw new UnsupportedOperationException();
    }

    public static double min(double v)
    {
        throw new UnsupportedOperationException();
    }

    public static int sum(int v)
    {
        throw new UnsupportedOperationException();
    }

    public static double sum(double v)
    {
        throw new UnsupportedOperationException();
    }

    protected abstract OJClass [] getParameterTypes();

    protected abstract OJClass getReturnType();

    abstract String getName();

    /**
     * <code>Count</code> is an aggregator which returns the number of rows
     * which have gone into it. With one argument (or more), it returns the
     * number of rows for which that argument (or all) is not
     * <code>null</code>.
     */
    static class Count extends BuiltinAggregation
    {
        // REVIEW jvs 26-Sept-2003:  Shouldn't this be OJSystem.LONG?
        static final OJClass type = OJSystem.INT;

        Count()
        {
        }

        public OJClass [] getParameterTypes()
        {
            return new OJClass[0];
        }

        public OJClass getReturnType()
        {
            return type;
        }

        public OJClass [] getStartParameterTypes()
        {
            return new OJClass[0];
        }

        public boolean canMerge()
        {
            return true;
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            int [] args)
        {
            StatementList stmtList = implementor.getStatementList();
            ExpressionStatement stmt =
                new ExpressionStatement(new UnaryExpression(
                        UnaryExpression.POST_INCREMENT,
                        new FieldAccess(
                            new CastExpression(
                                new TypeName(holderClassName + "." + type
                                    + "_Holder"),
                                accumulator),
                            "value")));
            if (args.length == 0) {
                // e.g. "((Holder.int_Holder) acc).value++;"
                stmtList.add(stmt);
            } else {
                // if (arg1 != null && arg2 != null) {
                //  ((Holder.int_Holder) acc).value++;
                // }
                Expression condition = null;
                for (int i = 0; i < args.length; i++) {
                    Expression term =
                        new BinaryExpression(
                            implementor.translateInputField(rel, 0, args[i]),
                            BinaryExpression.NOTEQUAL,
                            Literal.constantNull());
                    if (condition == null) {
                        condition = term;
                    } else {
                        condition =
                            new BinaryExpression(condition,
                                BinaryExpression.LOGICAL_AND, term);
                    }
                }
                stmtList.add(
                    new IfStatement(
                        condition,
                        new StatementList(stmt)));
            }
        }

        public Expression implementResult(Expression accumulator)
        {
            // e.g. "o" becomes "((Holder.int_Holder) o).value"
            return new FieldAccess(
                new CastExpression(
                    new TypeName(holderClassName +
                    "." + type + "_Holder"),
                    accumulator),
                "value");
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            int [] args)
        {
            // e.g. "new Holder.int_Holder(0)"
            return new AllocationExpression(
                new TypeName(holderClassName +
                    "." + type + "_Holder"),
                new ExpressionList(Literal.constantZero()));
        }

        String getName()
        {
            return "count";
        }
    }

    /**
     * <code>MinMax</code> implements the "min" and "max" aggregator
     * functions, returning the returns the smallest/largest of the values
     * which go into it. There are 3 forms:
     *
     * <dl>
     * <dt>
     * sum(<em>primitive type</em>)
     * </dt>
     * <dd>
     * values are compared using &lt;
     * </dd>
     * <dt>
     * sum({@link Comparable})
     * </dt>
     * <dd>
     * values are compared using {@link Comparable#compareTo}
     * </dd>
     * <dt>
     * sum({@link java.util.Comparator}, {@link Object})
     * </dt>
     * <dd>
     * the {@link java.util.Comparator#compare} method of the comparator is
     * used to compare pairs of objects. The comparator is a startup
     * argument, and must therefore be constant for the duration of the
     * aggregation.
     * </dd>
     * </dl>
     */
    static class MinMax extends BuiltinAggregation
    {
        static final int MINMAX_INVALID = -1;
        static final int MINMAX_PRIMITIVE = 0;
        static final int MINMAX_COMPARABLE = 1;
        static final int MINMAX_COMPARATOR = 2;
        private OJClass [] argTypes;
        private boolean isMin;
        private int kind;

        MinMax(
            OJClass [] argTypes,
            boolean isMin)
        {
            this.argTypes = argTypes;
            this.isMin = isMin;
            this.kind = getKind(argTypes);
        }

        public OJClass [] getParameterTypes()
        {
            switch (kind) {
            case MINMAX_PRIMITIVE:
            case MINMAX_COMPARABLE:
                return argTypes;
            case MINMAX_COMPARATOR:
                return new OJClass [] { argTypes[1] };
            default:
                throw Toolbox.newInternal("bad kind: " + kind);
            }
        }

        public OJClass getReturnType()
        {
            switch (kind) {
            case MINMAX_PRIMITIVE:
            case MINMAX_COMPARABLE:
                return argTypes[0];
            case MINMAX_COMPARATOR:
                return argTypes[1];
            default:
                throw Toolbox.newInternal("bad kind: " + kind);
            }
        }

        public OJClass [] getStartParameterTypes()
        {
            switch (kind) {
            case MINMAX_PRIMITIVE:
            case MINMAX_COMPARABLE:
                return new OJClass[0];
            case MINMAX_COMPARATOR:
                return new OJClass [] { Toolbox.clazzComparator };
            default:
                throw Toolbox.newInternal("bad kind: " + kind);
            }
        }

        public boolean canMerge()
        {
            return true;
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            int [] args)
        {
            StatementList stmtList = implementor.getStatementList();
            switch (kind) {
            case MINMAX_PRIMITIVE:

                // "((Holder.int_Holder) acc).setLesser(arg)"
                Expression arg =
                    implementor.translateInputField(rel, 0, args[0]);
                stmtList.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new CastExpression(
                                new TypeName(holderClassName + "."
                                    + argTypes[0] + "_Holder"),
                                accumulator),
                            isMin ? "setLesser" : "setGreater",
                            new ExpressionList(arg))));
                return;
            case MINMAX_COMPARABLE:

                // T t = arg;
                // if (acc == null || (t != null && t.compareTo(acc) < 0)) {
                //   acc = t;
                // }
                arg = implementor.translateInputField(rel, 0, args[0]);
                Variable var_t = implementor.newVariable();
                stmtList.add(
                    new VariableDeclaration(
                        TypeName.forOJClass(argTypes[0]),
                        var_t.toString(),
                        arg));
                stmtList.add(
                    new IfStatement(
                        new BinaryExpression(
                            new BinaryExpression(
                                accumulator,
                                BinaryExpression.EQUAL,
                                Literal.constantNull()),
                            BinaryExpression.LOGICAL_OR,
                            new BinaryExpression(
                                new BinaryExpression(
                                    var_t,
                                    BinaryExpression.NOTEQUAL,
                                    Literal.constantNull()),
                                BinaryExpression.LOGICAL_AND,
                                new BinaryExpression(
                                    new MethodCall(
                                        var_t,
                                        "compareTo",
                                        new ExpressionList(accumulator)),
                                    BinaryExpression.LESS,
                                    Literal.constantZero()))),
                        new StatementList(
                            new ExpressionStatement(
                                new AssignmentExpression(accumulator,
                                    AssignmentExpression.EQUALS, var_t)))));
                return;
            case MINMAX_COMPARATOR:

                // "((Holder.ComparatorHolder)
                // acc).setLesser(arg)"
                arg = implementor.translateInputField(rel, 0, args[1]);
                stmtList.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new CastExpression(
                                new TypeName(holderClassName + "."
                                    + argTypes[1] + "_Holder"),
                                accumulator),
                            isMin ? "setLesser" : "setGreater",
                            new ExpressionList(arg))));
                return;
            default:
                throw Toolbox.newInternal("bad kind: " + kind);
            }
        }

        public Expression implementResult(Expression accumulator)
        {
            switch (kind) {
            case MINMAX_PRIMITIVE:

                // ((Holder.int_Holder) acc).value
                return new FieldAccess(
                    new CastExpression(
                        new TypeName(holderClassName + "." + argTypes[1]
                            + "_Holder"),
                        accumulator),
                    "value");
            case MINMAX_COMPARABLE:

                // (T) acc
                return new CastExpression(
                    TypeName.forOJClass(argTypes[0]),
                    accumulator);
            case MINMAX_COMPARATOR:

                // (T) ((Holder.int_Holder) acc).value
                return new CastExpression(
                    TypeName.forOJClass(argTypes[1]),
                    new FieldAccess(
                        new CastExpression(
                            new TypeName(
                                holderClassName + ".ComparatorHolder"),
                            accumulator),
                        "value"));
            default:
                throw Toolbox.newInternal("bad kind: " + kind);
            }
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            int [] args)
        {
            switch (kind) {
            case MINMAX_PRIMITIVE:

                // "new Holder.int_Holder(Integer.MAX_VALUE)" if
                // the type is "int" and the function is "min"
                return new AllocationExpression(
                    new TypeName(holderClassName + "." + argTypes[0]
                        + "_Holder"),
                    new ExpressionList(
                        new FieldAccess(
                            TypeName.forOJClass(
                                argTypes[0].primitiveWrapper()),
                            isMin ? "MAX_VALUE" : "MIN_VALUE")));
            case MINMAX_COMPARABLE:

                // "null"
                return Literal.constantNull();
            case MINMAX_COMPARATOR:

                // "new saffron.runtime.ComparatorAndObject(comparator, null)"
                Expression arg =
                    implementor.translateInputField(rel, 0, args[0]);
                return new AllocationExpression(
                    new TypeName("saffron.runtime.ComparatorAndObject"),
                    new ExpressionList(
                        arg,
                        Literal.constantNull()));
            default:
                throw Toolbox.newInternal("bad kind: " + kind);
            }
        }

        static int getKind(OJClass [] argTypes)
        {
            if ((argTypes.length == 1) && argTypes[0].isPrimitive()) {
                return MINMAX_PRIMITIVE;
            } else if ((argTypes.length == 1)
                    && Toolbox.clazzComparable.isAssignableFrom(argTypes[0])) {
                return MINMAX_COMPARABLE;
            } else if ((argTypes.length == 2)
                    && Toolbox.clazzComparator.isAssignableFrom(argTypes[0])
                    && Toolbox.clazzObject.isAssignableFrom(argTypes[1])) {
                return MINMAX_COMPARATOR;
            } else {
                return MINMAX_INVALID;
            }
        }

        String getName()
        {
            return isMin ? "min" : "max";
        }
    }

    /**
     * <code>Sum</code> is an aggregator which returns the sum of the values
     * which go into it. It has precisely one argument of numeric type
     * (<code>int</code>, <code>long</code>, <code>float</code>,
     * <code>double</code>), and the result is the same type.
     */
    static class Sum extends BuiltinAggregation
    {
        private OJClass type;

        Sum(OJClass type)
        {
            this.type = type;
        }

        public OJClass [] getParameterTypes()
        {
            return new OJClass [] { type };
        }

        public OJClass getReturnType()
        {
            return type;
        }

        public OJClass [] getStartParameterTypes()
        {
            return new OJClass[0];
        }

        public boolean canMerge()
        {
            return true;
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            int [] args)
        {
            assert (args.length == 1);
            StatementList stmtList = implementor.getStatementList();
            Expression arg = implementor.translateInputField(rel, 0, args[0]);

            // e.g. "((Holder.int_Holder) acc).value += arg"
            stmtList.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new FieldAccess(
                            new CastExpression(
                                new TypeName(holderClassName + "." + type
                                    + "_Holder"),
                                accumulator),
                            "value"),
                        AssignmentExpression.ADD,
                        arg)));
        }

        public Expression implementResult(Expression accumulator)
        {
            // e.g. "o" becomes "((Holder.int_Holder) o).value"
            return new FieldAccess(
                new CastExpression(
                    new TypeName(holderClassName + "." + type + "_Holder"),
                    accumulator),
                "value");
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            int [] args)
        {
            // e.g. "new Holder.int_Holder(0)"
            return new AllocationExpression(
                new TypeName(holderClassName + "." + type + "_Holder"),
                new ExpressionList(Literal.constantZero()));
        }

        String getName()
        {
            return "sum";
        }
    }
}


// End BuiltinAggregation.java
