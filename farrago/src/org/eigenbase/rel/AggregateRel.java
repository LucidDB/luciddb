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

package org.eigenbase.rel;

import java.util.Arrays;

import openjava.ptree.Expression;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;


/**
 * <code>AggregateRel</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 3 February, 2002
 */
public class AggregateRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected Call [] aggCalls;
    protected int groupCount;

    //~ Constructors ----------------------------------------------------------

    public AggregateRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(cluster, child);
        this.groupCount = groupCount;
        this.aggCalls = aggCalls;
    }

    //~ Methods ---------------------------------------------------------------

    public Call [] getAggCalls()
    {
        return aggCalls;
    }

    public int getGroupCount()
    {
        return groupCount;
    }

    public Object clone()
    {
        return new AggregateRel(
            cluster,
            RelOptUtil.clone(child),
            groupCount,
            aggCalls);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    protected RelDataType deriveRowType()
    {
        final RelDataType childType = child.getRowType();
        RelDataType [] types = new RelDataType[groupCount + aggCalls.length];
        for (int i = 0; i < groupCount; i++) {
            types[i] = childType.getFields()[i].getType();
        }
        for (int i = 0; i < aggCalls.length; i++) {
            final RelDataType returnType =
                aggCalls[i].aggregation.getReturnType(cluster.typeFactory);
            types[groupCount + i] = returnType;
        }
        return cluster.typeFactory.createProjectType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return groupCount + aggCalls.length;
                }

                public String getFieldName(int index)
                {
                    if (index < groupCount) {
                        return childType.getFields()[index].getName();
                    } else {
                        return "$f" + index;
                    }
                }

                public RelDataType getFieldType(int index)
                {
                    if (index < groupCount) {
                        return childType.getFields()[index].getType();
                    } else {
                        final Call aggCall = aggCalls[index - groupCount];
                        return aggCall.aggregation.getReturnType(cluster.typeFactory);
                    }
                }
            });
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class Call
    {
        Aggregation aggregation;
        int [] args;

        public Call(
            Aggregation aggregation,
            int [] args)
        {
            this.aggregation = aggregation;
            this.args = args;
        }

        public Aggregation getAggregation()
        {
            return aggregation;
        }

        public int [] getArgs()
        {
            return args;
        }

        // override Object
        public boolean equals(Object o)
        {
            if (!(o instanceof Call)) {
                return false;
            }
            Call other = (Call) o;
            return aggregation.equals(other.aggregation)
                && Arrays.equals(args, other.args);
        }

        public void implementNext(
            JavaRelImplementor implementor,
            JavaRel rel,
            Expression accumulator)
        {
            aggregation.implementNext(implementor, rel, accumulator, args);
        }

        /**
         * Generates the expression to retrieve the result of this
         * aggregation.
         */
        public Expression implementResult(Expression accumulator)
        {
            return aggregation.implementResult(accumulator);
        }

        public Expression implementStart(
            JavaRelImplementor implementor,
            JavaRel rel)
        {
            return aggregation.implementStart(implementor, rel, args);
        }

        public Expression implementStartAndNext(
            JavaRelImplementor implementor,
            JavaRel rel)
        {
            return aggregation.implementStartAndNext(implementor, rel, args);
        }
    }
}


// End AggregateRel.java
