/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.type.SqlTypeUtil;


/**
 * <code>ProjectRelBase</code> is an abstract base class for RelNodes which
 * perform projection.  RelNodes which combine projection with other
 * operations should subclass ProjectRelBase rather than ProjectRel (which
 * represents pure projection).
 */
public abstract class ProjectRelBase extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected RexNode [] exps;
    protected String [] fieldNames;

    /** Values defined in {@link Flags}. */
    protected int flags;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Project.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param traits traits of this rel
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param fieldNames aliases of the expressions
     * @param flags values as in {@link Flags}
     */
    protected ProjectRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexNode [] exps,
        String [] fieldNames,
        int flags)
    {
        super(cluster, traits, child);
        this.exps = exps;
        if (fieldNames == null) {
            fieldNames = new String[exps.length];
        }
        assert (exps.length == fieldNames.length);
        this.fieldNames = fieldNames;
        this.flags = flags;

        //assert isBoxed();
        if (!isBoxed()) {
            assert (exps.length == 1);
        }
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isBoxed()
    {
        return (flags & Flags.Boxed) == Flags.Boxed;
    }

    public RexNode [] getChildExps()
    {
        return exps;
    }

    public String [] getFieldNames()
    {
        return fieldNames;
    }

    public int getFlags()
    {
        return flags;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = child.getRows();
        double dCpu = child.getRows() * exps.length;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    protected void defineTerms(String [] terms)
    {
        int i = 0;
        terms[i++] = "child";
        for (int j = 0; j < fieldNames.length; j++) {
            String fieldName = fieldNames[j];
            if (fieldName == null) {
                fieldName = "field#" + j;
            }
            terms[i++] = fieldName;
        }
    }

    public void explain(RelOptPlanWriter pw)
    {
        String [] terms = new String[1 + exps.length];
        defineTerms(terms);
        pw.explain(this, terms);
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation
     * which provides the field called <code>fieldName</code>.
     */
    public JavaRel implementFieldAccess(
        JavaRelImplementor implementor,
        String fieldName)
    {
        if (!isBoxed()) {
            return implementor.implementFieldAccess((JavaRel) child, fieldName);
        }
        RelDataType type = getRowType();
        int field = type.getFieldOrdinal(fieldName);
        return implementor.findRel((JavaRel) this, exps[field]);
    }

    protected RelDataType deriveRowType()
    {
        if (!isBoxed()) {
            return exps[0].getType();
        }
        final RelDataType [] types = SqlTypeUtil.collectTypes(exps);
        if (((flags & Flags.AnonFields) == Flags.AnonFields) && false) {
            return cluster.typeFactory.createJoinType(types);
        } else {
            return cluster.typeFactory.createStructType(
                new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return exps.length;
                    }

                    public String getFieldName(int index)
                    {
                        final String fieldName = fieldNames[index];
                        if (fieldName == null) {
                            return "$f" + index;
                        } else {
                            return fieldName;
                        }
                    }

                    public RelDataType getFieldType(int index)
                    {
                        return types[index];
                    }
                });
        }
    }

    //~ Inner Interfaces ------------------------------------------------------

    public interface Flags
    {
        int AnonFields = 2;

        /**
         * Whether the resulting row is to be a synthetic class whose fields
         * are the aliases of the fields. <code>boxed</code> must be true
         * unless there is only one field: <code>select {dept.deptno} from
         * dept</code> is boxed, <code>select dept.deptno from dept</code> is
         * not.
         */
        int Boxed = 1;
        int None = 0;
    }
}


// End ProjectRelBase.java
