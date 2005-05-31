/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * <code>AggregateRelBase</code> is an abstract base class for implementations
 * of {@link AggregateRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class AggregateRelBase extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected Call [] aggCalls;
    protected int groupCount;

    protected AggregateRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(cluster, traits, child);
        Util.pre(aggCalls != null, "aggCalls != null");
        this.groupCount = groupCount;
        this.aggCalls = aggCalls;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public boolean isDistinct()
    {
        return (aggCalls.length == 0)
            && (groupCount == child.getRowType().getFieldList().size());
    }

    public Call [] getAggCalls()
    {
        return aggCalls;
    }

    public int getGroupCount()
    {
        return groupCount;
    }

    public void explain(RelOptPlanWriter pw)
    {
        ArrayList names = new ArrayList(),
            values = new ArrayList();
        names.add("child");
        names.add("groupCount");
        values.add(new Integer(groupCount));
        for (int i = 0; i < aggCalls.length; i++) {
            names.add("agg#" + i);
            values.add(aggCalls[i]);
        }
        pw.explain(
            this,
            (String []) names.toArray(new String[names.size()]),
            values.toArray(new Object[values.size()]));
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
        return cluster.typeFactory.createStructType(
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
        public final Aggregation aggregation;
        public final int [] args;
        public final RelDataType type;

        public Call(
            Aggregation aggregation,
            int [] args,
            RelDataType type)
        {
            this.aggregation = aggregation;
            this.args = args;
            this.type = type;
        }

        public Aggregation getAggregation()
        {
            return aggregation;
        }

        public int [] getArgs()
        {
            return args;
        }

        public String toString()
        {
            StringBuffer buf = new StringBuffer(aggregation.getName());
            buf.append("(");
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                buf.append(args[i]);
            }
            buf.append(")");
            return buf.toString();
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

        public RelDataType getType()
        {
            return type;
        }
    }
}

// End AggregateRelBase.java
