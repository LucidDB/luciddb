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

import openjava.mop.NoSuchMemberException;
import openjava.mop.OJClass;
import openjava.mop.OJMethod;
import openjava.mop.Toolbox;
import openjava.ptree.Expression;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rex.OJAggImplementor;
import org.eigenbase.oj.rex.OJRexImplementorTableImpl;
import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.fun.SqlCountAggFunction;
import org.eigenbase.sql.fun.SqlMinMaxAggFunction;
import org.eigenbase.sql.fun.SqlSumAggFunction;
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
public abstract class BuiltinAggregation
    implements Aggregation, OJAggImplementor
{
    // The following methods are placeholders.
    public static int count()
    {
        throw new UnsupportedOperationException();
    }

    public static int count(Object v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static int count(int v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static int count(double v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a <code>BuiltinAggregation</code> with a given name and
     * argument types.
     */
    public static BuiltinAggregation create(
        String name,
        RelDataType [] argTypes)
    {
        if (name.equals("sum") && (argTypes.length == 1)) {
            return new DelegatingAggregation(
                new OJRexImplementorTableImpl.OJSumAggImplementor(),
                new SqlSumAggFunction(argTypes[0]));
        }
        if (name.equals("count")) {
            return new DelegatingAggregation(
                new OJRexImplementorTableImpl.OJCountAggImplementor(),
                new SqlCountAggFunction());
        }
        if ((name.equals("min") || name.equals("max"))) {
            OJClass [] ojArgTypes = new OJClass[argTypes.length];
            final int kind = getKind(ojArgTypes);
            if (kind == SqlMinMaxAggFunction.MINMAX_INVALID) {
                return null;
            }
            return new DelegatingAggregation(
                new OJRexImplementorTableImpl.OJMinMaxAggImplementor(),
                new SqlMinMaxAggFunction(
                    argTypes,
                    name.equals("min"), kind));
        }
        return null;
    }

    public static int getKind(OJClass [] argTypes)
    {
        if ((argTypes.length == 1) && argTypes[0].isPrimitive()) {
            return SqlMinMaxAggFunction.MINMAX_PRIMITIVE;
        } else if ((argTypes.length == 1)
                && Toolbox.clazzComparable.isAssignableFrom(argTypes[0])) {
            return SqlMinMaxAggFunction.MINMAX_COMPARABLE;
        } else if ((argTypes.length == 2)
                && Toolbox.clazzComparator.isAssignableFrom(argTypes[0])
                && Toolbox.clazzObject.isAssignableFrom(argTypes[1])) {
            return SqlMinMaxAggFunction.MINMAX_COMPARATOR;
        } else {
            return SqlMinMaxAggFunction.MINMAX_INVALID;
        }
    }


    // implement Aggregation
    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        return getParameterTypes();
    }

    // implement Aggregation
    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        return getReturnType();
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
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static double max(int v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static double max(double v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static int min(Object v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static double min(int v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static double min(double v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static int sum(int v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    public static double sum(double v)
    {
        Util.discard(v);
        throw new UnsupportedOperationException();
    }

    protected abstract RelDataType [] getParameterTypes();

    protected abstract RelDataType getReturnType();

    public abstract String getName();

    /**
     * Partial implementation for an {@link Aggregation} which delegates the
     * code-generation to an {@link OJAggImplementor}.
     */
    public static class DelegatingAggregation
        extends BuiltinAggregation
    {
        private final OJAggImplementor aggImplementor;
        private final SqlAggFunction aggFunction;

        protected DelegatingAggregation(
            OJAggImplementor implementor,
            SqlAggFunction aggFunction)
        {
            this.aggImplementor = implementor;
            this.aggFunction = aggFunction;
        }

        public RelDataType [] getParameterTypes()
        {
            final RelDataTypeFactory typeFactory = null; // TODO:
            return aggFunction.getParameterTypes(typeFactory);
        }

        public RelDataType getReturnType()
        {
            final RelDataTypeFactory typeFactory = null; // TODO:
            return aggFunction.getReturnType(typeFactory);
        }

        public OJClass [] getStartParameterTypes()
        {
            return aggFunction.getStartParameterTypes();
        }

        public String getName()
        {
            return aggFunction.getName();
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator,
            AggregateRel.Call call)
        {
            aggImplementor.implementNext(
                implementor,
                rel,
                accumulator,
                call);
        }

        public Expression implementResult(
            Expression accumulator,
            AggregateRel.Call call)
        {
            return aggImplementor.implementResult(accumulator, call);
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel,
            AggregateRel.Call call)
        {
            return aggImplementor.implementStart(
                implementor,
                rel,
                call);
        }

        public boolean canMerge()
        {
            return aggImplementor.canMerge();
        }

        public Expression implementStartAndNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            AggregateRel.Call call)
        {
            return aggImplementor.implementStartAndNext(
                implementor,
                rel,
                call);
        }

        public void implementMerge(
            JavaRelImplementor implementor,
            RelNode rel,
            Expression accumulator,
            Expression otherAccumulator)
        {
            aggImplementor.implementMerge(
                implementor,
                rel,
                accumulator,
                otherAccumulator);
        }
    }

}


// End BuiltinAggregation.java
